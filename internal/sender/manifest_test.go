package sender

import (
	"context"
	"crypto/md5"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/obs"
	"esync/internal/wire"
)

func mustRecv(t *testing.T, c *channel.Conn) wire.Message {
	t.Helper()
	m, err := c.RecvMsg()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	return m
}

// T-HASH-01 / T-HASH-03: the manifest publisher streams MD5 over each file and
// publishes the digest in GROUP_MANIFEST; large (multi-buffer) files hash the
// same as a one-shot md5.
func TestManifestHashesAndPublishes(t *testing.T) {
	root := t.TempDir()
	small := []byte("small")
	big := make([]byte, 900*1024) // > one 256 KiB streaming buffer
	for i := range big {
		big[i] = byte(i * 7)
	}
	writeFile(t, filepath.Join(root, "a-small"), small)
	writeFile(t, filepath.Join(root, "b-big"), big)
	pl := buildPlan(t, root)

	snd, rcv := connPair(t, 0)
	ds := newDigestStore()
	mp := &manifestPublisher{
		ctx: obs.Ctx{}, ctrl: snd, pl: pl, root: root,
		algo: digest.MD5, workers: 2, digests: ds, credit: newCreditGate(8),
	}
	go func() { _ = mp.run(context.Background()) }()

	sc, ok := mustRecv(t, rcv).(*wire.ScanComplete)
	if !ok {
		t.Fatal("want SCAN_COMPLETE first")
	}
	if sc.TotalFiles != 2 {
		t.Fatalf("total_files = %d, want 2", sc.TotalFiles)
	}

	gm, ok := mustRecv(t, rcv).(*wire.GroupManifest)
	if !ok {
		t.Fatal("want GROUP_MANIFEST")
	}
	wantSmall := md5.Sum(small)
	wantBig := md5.Sum(big)
	seen := map[string][]byte{}
	for _, e := range gm.Entries {
		seen[string(e.Path)] = e.Digest
	}
	if string(seen["a-small"]) != string(wantSmall[:]) {
		t.Fatalf("a-small digest wrong")
	}
	if string(seen["b-big"]) != string(wantBig[:]) {
		t.Fatalf("b-big digest wrong (streaming hash mismatch)")
	}

	fidSmall, _ := pl.FileID("a-small")
	if d, _ := ds.get(fidSmall); string(d) != string(wantSmall[:]) {
		t.Fatalf("digest store not populated for file_id %d", fidSmall)
	}
}

// T-PAR-06: hashing is lazy and gated by credit — a group's files are not
// hashed until credit lets that group's manifest go out.
func TestManifestLazyHashingGatedByCredit(t *testing.T) {
	root := t.TempDir()
	const n = 1030 // 2 groups: 1024 + 6
	for i := 0; i < n; i++ {
		writeFile(t, filepath.Join(root, fmt.Sprintf("f%05d", i)), []byte{byte(i), byte(i >> 8)})
	}
	pl := buildPlan(t, root)
	if pl.NumGroups() != 2 {
		t.Fatalf("NumGroups = %d, want 2", pl.NumGroups())
	}

	snd, rcv := connPair(t, 0)
	ds := newDigestStore()
	cg := newCreditGate(1) // room for exactly one group
	mp := &manifestPublisher{
		ctx: obs.Ctx{}, ctrl: snd, pl: pl, root: root,
		algo: digest.MD5, workers: 4, digests: ds, credit: cg,
	}
	go func() { _ = mp.run(context.Background()) }()

	if _, ok := mustRecv(t, rcv).(*wire.ScanComplete); !ok {
		t.Fatal("want SCAN_COMPLETE")
	}
	g0, ok := mustRecv(t, rcv).(*wire.GroupManifest)
	if !ok || g0.GroupID != 0 {
		t.Fatalf("want GROUP_MANIFEST 0, got %#v", g0)
	}

	// Group 1 (file_id 1024+) must not be hashed yet: no credit was granted.
	time.Sleep(100 * time.Millisecond)
	if d, ok := ds.get(1024); ok {
		t.Fatalf("group 1 file hashed before credit granted: %x", d)
	}

	cg.add(1)
	g1, ok := mustRecv(t, rcv).(*wire.GroupManifest)
	if !ok || g1.GroupID != 1 {
		t.Fatalf("want GROUP_MANIFEST 1, got %#v", g1)
	}
	if d, ok := ds.get(1024); !ok || len(d) != 16 {
		t.Fatalf("group 1 file not hashed after credit: %v", d)
	}
}
