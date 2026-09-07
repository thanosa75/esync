package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/receiver"
	"esync/internal/sender"
)

// TestE2ETransfer runs the real sender and receiver state machines against each
// other over real loopback TCP sockets and asserts the destination tree is a
// byte-for-byte copy of the source (ARCHITECTURE §19.3, E2E-01).
func TestE2ETransfer(t *testing.T) {
	if _, err := paircode.Discover(""); err != nil {
		t.Skipf("no usable LAN endpoint in this environment: %v", err)
	}

	src := t.TempDir()
	dst := t.TempDir()

	want := map[string][]byte{
		"readme.txt":            []byte("hello esync\n"),
		"empty":                 {},
		"nested/a/small.bin":    randBytes(t, 1024),
		"nested/a/b/big.bin":    randBytes(t, 5<<20+777), // spans several chunks + hash buffers
		"nested/c/another.data": randBytes(t, 300<<10),
	}
	for rel, content := range want {
		p := filepath.Join(src, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Capture stdout (the pairing code) and drain stderr (guidance + logs).
	origOut, origErr := os.Stdout, os.Stderr
	outR, outW, _ := os.Pipe()
	errR, errW, _ := os.Pipe()
	os.Stdout, os.Stderr = outW, errW
	t.Cleanup(func() { os.Stdout, os.Stderr = origOut, origErr })
	go io.Copy(io.Discard, errR) //nolint (test-only drain)

	// Separate obs instances so their live counters (and thus each Summary) stay
	// independent — a shared Ctx would sum sender + receiver tallies.
	sctx, sflush := obs.Init(obs.Config{Level: obs.LevelError, Role: "sender"})
	defer sflush()
	rctx, rflush := obs.Init(obs.Config{Level: obs.LevelError, Role: "receiver"})
	defer rflush()

	codeCh := make(chan string, 1)
	go func() {
		br := bufio.NewReader(outR)
		line, _ := br.ReadString('\n')
		codeCh <- strings.TrimSpace(line)
		io.Copy(io.Discard, br)
	}()

	type result struct {
		sum  any
		code int
	}
	senderDone := make(chan result, 1)
	go func() {
		sum, code := sender.Run(sctx, sender.Config{
			SourcePath:      src,
			Version:         "e2e",
			MaxChannels:     4,
			PairTimeout:     30 * time.Second,
			MaxPairAttempts: 5,
			DrainTimeout:    30 * time.Second,
		})
		senderDone <- result{sum, code}
	}()

	var code string
	select {
	case code = <-codeCh:
	case <-time.After(20 * time.Second):
		t.Fatal("sender did not emit a pairing code")
	}
	if code == "" {
		t.Fatal("empty pairing code")
	}

	recvSum, recvCode := receiver.Run(rctx, receiver.Config{
		Link:          code,
		Dest:          dst,
		Version:       "e2e",
		Channels:      3,
		GroupCredit:   4,
		PipelineDepth: 2,
		DrainTimeout:  30 * time.Second,
	})
	if recvCode != 0 {
		t.Fatalf("receiver exit %d (outcome %q, resume %q)", recvCode, recvSum.Outcome, recvSum.ResumeCommand)
	}

	var sres result
	select {
	case sres = <-senderDone:
	case <-time.After(20 * time.Second):
		t.Fatal("sender did not finish")
	}
	if sres.code != 0 {
		t.Fatalf("sender exit %d", sres.code)
	}

	// Restore std streams before using t.Errorf so failures are visible.
	os.Stdout, os.Stderr = origOut, origErr

	if recvSum.FilesTransferred != uint64(len(want)) {
		t.Errorf("transferred %d files, want %d", recvSum.FilesTransferred, len(want))
	}
	assertTreeEqual(t, src, dst, want)
}

func assertTreeEqual(t *testing.T, src, dst string, want map[string][]byte) {
	t.Helper()

	for rel, content := range want {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("dest missing %s: %v", rel, err)
			continue
		}
		if !bytes.Equal(got, content) {
			t.Errorf("%s: content differs (got %d bytes, want %d)", rel, len(got), len(content))
		}
	}

	var extra []string
	filepath.WalkDir(dst, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dst, p)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, ".esync/") {
			t.Errorf("journal residue left behind: %s", rel)
			return nil
		}
		if _, ok := want[rel]; !ok {
			extra = append(extra, rel)
		}
		return nil
	})
	if len(extra) > 0 {
		sort.Strings(extra)
		t.Errorf("unexpected files at destination: %v", extra)
	}
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}
