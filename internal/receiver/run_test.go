package receiver

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/wire"
)

// fakeSenderMaxChannels is the ceiling the fake sender advertises in
// SESSION_PARAMS.MaxChannels; late CHANNEL_JOINs (§13.4 ramp-up) are validated
// against it, same as the real sender validates against cfg.MaxChannels.
const fakeSenderMaxChannels = 8

type treeFile struct {
	rel   string
	data  []byte
	etype uint8
	link  string
}

type fakeSender struct {
	t     *testing.T
	ln    net.Listener
	link  string
	files []treeFile
	errc  chan error

	// chunkDelay/chunkSize pace file content, when set, so a test can force a
	// transfer to span multiple adaptive-tuner windows (§13.4) without moving
	// much actual data. Zero means "send it all in one burst".
	chunkDelay time.Duration
	chunkSize  int

	// abortBeforeSummary, when set, makes the fake sender abandon the session
	// once every needed file has been served but before SESSION_SUMMARY:
	// "error" sends a fatal wire.Error then closes the control channel, "close"
	// just closes it. Models a sender that crashed or tripped its own watchdog.
	abortBeforeSummary string

	mu       sync.Mutex
	needed   map[uint64]bool
	served   map[uint64]bool
	decided  int
	summSent bool
	ctrl     *channel.Conn
	joined   int // total CHANNEL_JOINs accepted, initial batch + any later ramp-ups
}

func newFakeSender(t *testing.T, files []treeFile, opts ...func(*fakeSender)) *fakeSender {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ap := netip.MustParseAddrPort(ln.Addr().String())
	secret := obs.NewSecret([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	code := paircode.Encode(&paircode.Payload{
		Endpoints: []paircode.Endpoint{{Addr: ap.Addr(), Port: ap.Port()}},
		Secret:    secret,
	})
	fs := &fakeSender{
		t: t, ln: ln, link: code, files: files, errc: make(chan error, 4),
		needed: map[uint64]bool{}, served: map[uint64]bool{},
	}
	for _, o := range opts {
		o(fs)
	}
	t.Cleanup(func() { ln.Close() })
	go fs.serve(secret)
	return fs
}

func (fs *fakeSender) fail(err error) {
	select {
	case fs.errc <- err:
	default:
	}
}

func (fs *fakeSender) serve(secret obs.Secret) {
	kPair := paircode.KPair(secret)
	sid := paircode.SessionID(secret)

	ctrlNC, err := fs.ln.Accept()
	if err != nil {
		fs.fail(err)
		return
	}
	sess, err := handshake.ServerHandshake(obs.Ctx{}, ctrlNC, kPair, sid, 5*time.Second)
	if err != nil {
		fs.fail(err)
		return
	}
	ctrl := channel.Control(ctrlNC, sess, channel.Sender)
	fs.ctrl = ctrl

	digAll := sha256.Sum256([]byte("manifest"))
	if err := ctrl.SendMsg(&wire.SessionParams{
		RootName: "payload", HashAlg: 0, GroupBytes: 512 << 20, SenderVersion: "esync/test",
		MaxChannels: fakeSenderMaxChannels,
	}); err != nil {
		fs.fail(err)
		return
	}
	rm, err := ctrl.RecvMsg()
	if err != nil {
		fs.fail(err)
		return
	}
	ready, ok := rm.(*wire.SessionReady)
	if !ok {
		fs.fail(errf("want SESSION_READY, got %T", rm))
		return
	}
	n := int(ready.Channels)

	dataConns := make([]*channel.Conn, 0, n)
	for i := 0; i < n; i++ {
		nc, err := fs.ln.Accept()
		if err != nil {
			fs.fail(err)
			return
		}
		id, jerr := handshake.AcceptChannel(obs.Ctx{}, nc, sess, uint8(n))
		if jerr != nil {
			fs.fail(jerr)
			return
		}
		dataConns = append(dataConns, channel.Data(nc, id, sess, channel.Sender, 0))
	}
	fs.mu.Lock()
	fs.joined = n
	fs.mu.Unlock()

	// Keep accepting: the adaptive tuner (§13.4) may CHANNEL_JOIN more data
	// channels than the initial SESSION_READY count. Each is served exactly
	// like the initial batch. Accept returns an error once the test closes the
	// listener, ending this goroutine.
	go func() {
		for {
			nc, err := fs.ln.Accept()
			if err != nil {
				return
			}
			id, jerr := handshake.AcceptChannel(obs.Ctx{}, nc, sess, fakeSenderMaxChannels)
			if jerr != nil {
				fs.fail(jerr)
				return
			}
			fs.mu.Lock()
			fs.joined++
			fs.mu.Unlock()
			fs.dataLoop(channel.Data(nc, id, sess, channel.Sender, 0))
		}
	}()

	// SCAN_COMPLETE + one GROUP_MANIFEST covering the whole tree.
	entries := make([]wire.ManifestEntry, len(fs.files))
	var totalBytes uint64
	for i, f := range fs.files {
		e := wire.ManifestEntry{
			EntryType: f.etype, Path: []byte(f.rel), Mode: 0o644,
			MtimeSec: 1700000000,
		}
		switch f.etype {
		case entryTypeFile:
			e.Size = uint64(len(f.data))
			e.Digest = md5sum(fs.t, f.data)
			totalBytes += uint64(len(f.data))
		case entryTypeDir:
			e.Mode = 0o755
		case entryTypeSymlink:
			e.LinkTarget = []byte(f.link)
		}
		entries[i] = e
	}
	if err := ctrl.SendMsg(&wire.ScanComplete{
		TotalFiles: uint64(len(fs.files)), TotalBytes: totalBytes, TotalGroups: 1,
		ManifestDigest: digAll[:],
	}); err != nil {
		fs.fail(err)
		return
	}
	if err := ctrl.SendMsg(&wire.GroupManifest{GroupID: 0, FirstFileID: 0, Entries: entries}); err != nil {
		fs.fail(err)
		return
	}

	var wg sync.WaitGroup
	for _, dc := range dataConns {
		wg.Add(1)
		go func(dc *channel.Conn) {
			defer wg.Done()
			fs.dataLoop(dc)
		}(dc)
	}

	// Control reader.
	ackc := make(chan *wire.SessionSummaryAck, 1)
	go func() {
		for {
			m, err := ctrl.RecvMsg()
			if err != nil {
				return
			}
			switch v := m.(type) {
			case *wire.GroupDecision:
				fs.mu.Lock()
				for _, idx := range v.Needed {
					fs.needed[uint64(idx)] = true
				}
				fs.decided++
				fs.mu.Unlock()
				fs.maybeSummary()
			case *wire.Credit:
			case *wire.SessionSummaryAck:
				ackc <- v
			}
		}
	}()

	select {
	case ack := <-ackc:
		want := fs.completionDigest()
		if string(ack.CompletionDigest) != string(want[:]) {
			fs.fail(errf("ack completion digest mismatch: ack=%x want=%x", ack.CompletionDigest, want[:]))
		}
	case <-time.After(15 * time.Second):
		fs.fail(errf("timed out waiting for SESSION_SUMMARY_ACK"))
	}
	wg.Wait()
}

func (fs *fakeSender) dataLoop(dc *channel.Conn) {
	for {
		m, err := dc.RecvMsg()
		if err != nil {
			return
		}
		req, ok := m.(*wire.FileRequest)
		if !ok {
			continue
		}
		f := fs.files[req.FileID]
		dig := md5sum(fs.t, f.data)
		_ = dc.SendMsg(&wire.FileHeader{
			RequestID: req.RequestID, FileID: req.FileID, Size: uint64(len(f.data)),
			Mode: 0o644, MtimeSec: 1700000000, Digest: dig, ChunkSize: 1 << 16,
		})
		cs := 1 << 16
		if fs.chunkSize > 0 {
			cs = fs.chunkSize
		}
		for off := 0; off < len(f.data); off += cs {
			end := off + cs
			if end > len(f.data) {
				end = len(f.data)
			}
			if fs.chunkDelay > 0 {
				time.Sleep(fs.chunkDelay)
			}
			_ = dc.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: uint64(off), Data: f.data[off:end]})
		}
		_ = dc.SendMsg(&wire.FileComplete{RequestID: req.RequestID, BytesSent: uint64(len(f.data)), DigestFull: dig})
		fs.mu.Lock()
		fs.served[req.FileID] = true
		fs.mu.Unlock()
		fs.maybeSummary()
	}
}

func (fs *fakeSender) maybeSummary() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.summSent || fs.decided < 1 {
		return
	}
	for id := range fs.needed {
		if !fs.served[id] {
			return
		}
	}
	fs.summSent = true
	if fs.abortBeforeSummary != "" {
		if fs.abortBeforeSummary == "error" {
			_ = fs.ctrl.SendMsg(&wire.Error{
				Code: 9002, Fatal: 1, Message: "drain watchdog", Detail: "the transfer stopped making progress",
			})
		}
		_ = fs.ctrl.Close()
		return
	}
	comp := fs.completionDigestLocked()
	var bytesT uint64
	for id := range fs.served {
		bytesT += uint64(len(fs.files[id].data))
	}
	_ = fs.ctrl.SendMsg(&wire.SessionSummary{
		FilesTotal: uint64(len(fs.files)), FilesTransferred: uint64(len(fs.served)),
		BytesTransferred: bytesT, GroupsTotal: 1, CompletionDigest: comp[:],
	})
}

func (fs *fakeSender) completionDigest() [32]byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.completionDigestLocked()
}

func (fs *fakeSender) completionDigestLocked() [32]byte {
	ids := make([]uint64, 0, len(fs.served))
	for id := range fs.served {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	var b [8]byte
	for _, id := range ids {
		binary.BigEndian.PutUint64(b[:], id)
		h.Write(b[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func errf(format string, a ...any) error { return fmt.Errorf(format, a...) }

// joinedCount reports how many data channels (initial batch plus any
// tuner-driven ramp-ups) the fake sender has accepted so far.
func (fs *fakeSender) joinedCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.joined
}

// T-PROTO-06 / end-to-end: Run pairs, transfers a small tree, reconciles the
// completion digest, and exits 0 with every file materialised.
func TestRunEndToEnd(t *testing.T) {
	files := []treeFile{
		{rel: "readme.md", data: []byte("# hello\n")},
		{rel: "dir", etype: entryTypeDir},
		{rel: "dir/big.bin", data: mkbytes(300000, 0x5a)},
		{rel: "dir/small.txt", data: []byte("tiny")},
	}
	fs := newFakeSender(t, files)
	dest := t.TempDir()

	octx, flush := obs.Init(obs.Config{Sync: true, Role: "receiver"})
	defer flush()
	sum, exit := Run(octx, Config{
		Link: fs.link, Dest: dest, Channels: 2, ChannelsPinned: true, HandshakeTimeout: 5 * time.Second,
		ConnectTimeout: 5 * time.Second,
	})

	select {
	case err := <-fs.errc:
		t.Fatalf("fake sender error: %v", err)
	default:
	}
	if exit != 0 {
		t.Fatalf("exit = %d (outcome %s), want 0", exit, sum.Outcome)
	}
	if sum.FilesTransferred != 3 {
		t.Fatalf("FilesTransferred = %d, want 3", sum.FilesTransferred)
	}
	for _, f := range files {
		if f.etype != entryTypeFile {
			continue
		}
		got, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(f.rel)))
		if err != nil || string(got) != string(f.data) {
			t.Fatalf("%s: content mismatch (err %v)", f.rel, err)
		}
	}
	// Journal cleared on success.
	if _, err := os.Stat(filepath.Join(dest, ".esync")); !os.IsNotExist(err) {
		t.Fatalf(".esync should be gone after success: %v", err)
	}
}

// T-RES-02/03: a second run against the same destination, with files already in
// place, transfers nothing and still exits 0.
func TestRunResumeNothingToDo(t *testing.T) {
	files := []treeFile{
		{rel: "a.txt", data: []byte("alpha")},
		{rel: "b.txt", data: []byte("bravo")},
	}
	dest := t.TempDir()
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dest, f.rel), f.data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fs := newFakeSender(t, files)

	sum, exit := Run(obs.Ctx{}, Config{
		Link: fs.link, Dest: dest, Channels: 1, ChannelsPinned: true,
		HandshakeTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second,
	})
	select {
	case err := <-fs.errc:
		t.Fatalf("fake sender error: %v", err)
	default:
	}
	if exit != 0 {
		t.Fatalf("exit = %d (%s), want 0", exit, sum.Outcome)
	}
	if sum.FilesTransferred != 0 {
		t.Fatalf("FilesTransferred = %d, want 0", sum.FilesTransferred)
	}
	if sum.FilesSkipped != 2 {
		t.Fatalf("FilesSkipped = %d, want 2", sum.FilesSkipped)
	}
}

// A sender that abandons the control channel after serving every file but
// before SESSION_SUMMARY must not be reported as a clean success: the transfer
// never reconciled, so Run exits non-zero and keeps the journal for resume.
func TestRunSenderAbortsBeforeSummary(t *testing.T) {
	for _, mode := range []string{"close", "error"} {
		t.Run(mode, func(t *testing.T) {
			files := []treeFile{{rel: "a.txt", data: []byte("alpha")}}
			fs := newFakeSender(t, files, func(fs *fakeSender) { fs.abortBeforeSummary = mode })
			dest := t.TempDir()

			sum, exit := Run(obs.Ctx{}, Config{
				Link: fs.link, Dest: dest, Channels: 1, ChannelsPinned: true,
				HandshakeTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second,
				DrainTimeout: 2 * time.Second,
			})

			if exit == 0 {
				t.Fatalf("exit = 0 (outcome %s), want non-zero: the sender never sent SESSION_SUMMARY", sum.Outcome)
			}
			if _, err := os.Stat(filepath.Join(dest, ".esync")); os.IsNotExist(err) {
				t.Fatalf(".esync journal must be retained for resume after an incomplete transfer")
			}
			if sum.ResumeCommand == "" {
				t.Fatalf("want a resume command in the summary, got none")
			}
		})
	}
}

func mkbytes(n int, b byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b ^ byte(i)
	}
	return out
}
