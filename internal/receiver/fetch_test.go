package receiver

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/wire"
)

type stubFile struct {
	data     []byte
	dig      []byte
	corruptN int // initial responses whose DigestFull is wrong (retryable E8001)
}

// serveRequests answers FILE_REQUESTs on conn until it errors (peer closed).
func serveRequests(t *testing.T, conn *channel.Conn, files map[uint64]*stubFile) {
	t.Helper()
	for {
		m, err := conn.RecvMsg()
		if err != nil {
			return
		}
		req, ok := m.(*wire.FileRequest)
		if !ok {
			continue
		}
		sf := files[req.FileID]
		if sf == nil {
			_ = conn.SendMsg(&wire.FileError{RequestID: req.RequestID, Code: 5007, Message: "unknown file"})
			continue
		}
		_ = conn.SendMsg(&wire.FileHeader{
			RequestID: req.RequestID, FileID: req.FileID,
			Size: uint64(len(sf.data)), Mode: 0o644,
			Digest: sf.dig, ChunkSize: 1 << 16,
		})
		const cs = 1 << 16
		for off := 0; off < len(sf.data); off += cs {
			end := off + cs
			if end > len(sf.data) {
				end = len(sf.data)
			}
			_ = conn.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: uint64(off), Data: sf.data[off:end]})
		}
		full := sf.dig
		if sf.corruptN > 0 {
			sf.corruptN--
			bad := append([]byte(nil), sf.dig...)
			bad[0] ^= 0xff
			full = bad
		}
		_ = conn.SendMsg(&wire.FileComplete{RequestID: req.RequestID, BytesSent: uint64(len(sf.data)), DigestFull: full})
	}
}

type fetchHarness struct {
	f       *fetcher
	q       *needQueue
	cnt     *obs.Counters
	dir     string
	dest    *fsx.Dest
	journal *fsx.Journal
	done    []uint64
	doneMu  sync.Mutex
	failErr atomic.Value
}

func newFetchHarness(t *testing.T, rConn *channel.Conn) *fetchHarness {
	t.Helper()
	dir := t.TempDir()
	dest, err := fsx.OpenDest(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dest.Close() })
	j, _, err := fsx.Open(dir, "0000000000000000", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })

	h := &fetchHarness{q: newNeedQueue(needQueueDepth), cnt: &obs.Counters{}, dir: dir, dest: dest, journal: j}
	h.f = &fetcher{
		octx:     obs.Ctx{},
		cfg:      Config{}.withDefaults(),
		conn:     rConn,
		dest:     dest,
		journal:  j,
		algo:     digest.MD5,
		q:        h.q,
		meta:     newMetaStore(),
		hl:       newHardlinkMap(),
		counters: h.cnt,
		reqID:    &atomic.Uint64{},
		rnd:      nil,
		onComplete: func(id uint64) {
			h.doneMu.Lock()
			h.done = append(h.done, id)
			h.doneMu.Unlock()
		},
		fail: func(err error) { h.failErr.Store(err) },
	}
	return h
}

func (h *fetchHarness) enqueue(t *testing.T, id uint64, rel string, data, dig []byte) {
	t.Helper()
	h.f.meta.set(id, fileMeta{rel: rel, size: int64(len(data)), mode: 0o644, mtime: time.Unix(1000, 0), digest: dig})
	if err := h.q.pushGroup(context.Background(), []needItem{{fileID: id, size: int64(len(data)), rel: rel}}); err != nil {
		t.Fatal(err)
	}
}

// T-XFER-03: a completed file is published atomically — the final path holds the
// verified content and the .part scratch file is gone.
func TestFetchAtomicPublish(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	data := []byte("the quick brown fox jumps over the lazy dog")
	dig := md5sum(t, data)

	go serveRequests(t, sConn, map[uint64]*stubFile{0: {data: data, dig: dig}})

	h.enqueue(t, 0, "docs/fox.txt", data, dig)
	h.q.close()
	h.f.loop(context.Background())

	if e := h.failErr.Load(); e != nil {
		t.Fatalf("fetch failed: %v", e)
	}
	got, err := os.ReadFile(filepath.Join(h.dir, "docs", "fox.txt"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("published content = %q, err %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, ".esync", "parts", "0.part")); !os.IsNotExist(err) {
		t.Fatalf("scratch .part still present: %v", err)
	}
	if h.cnt.FilesTransferred.Load() != 1 {
		t.Fatalf("FilesTransferred = %d, want 1", h.cnt.FilesTransferred.Load())
	}
	h.doneMu.Lock()
	defer h.doneMu.Unlock()
	if len(h.done) != 1 || h.done[0] != 0 {
		t.Fatalf("completed ids = %v, want [0]", h.done)
	}
}

// T-XFER-02: a streamed-digest mismatch is retried and the retry succeeds.
func TestFetchRetriesOnDigestMismatch(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	data := []byte("payload bytes that will be verified")
	dig := md5sum(t, data)

	go serveRequests(t, sConn, map[uint64]*stubFile{0: {data: data, dig: dig, corruptN: 1}})

	h.enqueue(t, 0, "p.bin", data, dig)
	h.q.close()

	doneCh := make(chan struct{})
	go func() { h.f.loop(context.Background()); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop did not finish")
	}

	if e := h.failErr.Load(); e != nil {
		t.Fatalf("fetch failed: %v", e)
	}
	if h.cnt.Retries.Load() != 1 || h.cnt.RetriesOK.Load() != 1 {
		t.Fatalf("retries=%d retriesOK=%d, want 1/1", h.cnt.Retries.Load(), h.cnt.RetriesOK.Load())
	}
	got, _ := os.ReadFile(filepath.Join(h.dir, "p.bin"))
	if string(got) != string(data) {
		t.Fatalf("content = %q", got)
	}
}

// A FILE_ERROR the catalogue marks non-retryable ends the file permanently and
// tallies it as failed without stopping the session.
func TestFetchPermanentFileError(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)

	go func() {
		for {
			m, err := sConn.RecvMsg()
			if err != nil {
				return
			}
			if req, ok := m.(*wire.FileRequest); ok {
				// E6002 (source file vanished) is Item-class, non-retryable.
				_ = sConn.SendMsg(&wire.FileError{RequestID: req.RequestID, Code: 6002, Message: "gone"})
			}
		}
	}()

	h.enqueue(t, 0, "missing.txt", []byte("x"), md5sum(t, []byte("x")))
	h.q.close()
	h.f.loop(context.Background())

	if e := h.failErr.Load(); e != nil {
		t.Fatalf("session should survive a permanent item failure, got %v", e)
	}
	if h.cnt.FilesFailed.Load() != 1 {
		t.Fatalf("FilesFailed = %d, want 1", h.cnt.FilesFailed.Load())
	}
}

// A file whose content matches its own FILE_HEADER digest and streamed
// FILE_COMPLETE digest, but does not match the digest the file's group
// manifest originally declared for it (as if the source changed between
// manifest and transfer), is a permanent, non-retryable E8003 — the receiver
// must never publish content that doesn't match the md5 the group promised.
func TestFetchManifestDigestMismatch(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	data := []byte("payload that matches its own header and streamed digest")
	dig := md5sum(t, data)
	staleManifestDigest := md5sum(t, []byte("a completely different, stale manifest entry"))

	go serveRequests(t, sConn, map[uint64]*stubFile{0: {data: data, dig: dig}})

	// The manifest digest recorded at decide time (fileMeta.digest, copied
	// verbatim from the group's wire.ManifestEntry.Digest) does not match what
	// the sender actually streams for this request.
	h.enqueue(t, 0, "changed.bin", data, staleManifestDigest)
	h.q.close()
	h.f.loop(context.Background())

	if e := h.failErr.Load(); e != nil {
		t.Fatalf("a manifest-digest mismatch is item-class, not fatal: got %v", e)
	}
	if h.cnt.FilesTransferred.Load() != 0 {
		t.Fatalf("FilesTransferred = %d, want 0", h.cnt.FilesTransferred.Load())
	}
	if h.cnt.FilesFailed.Load() != 1 {
		t.Fatalf("FilesFailed = %d, want 1", h.cnt.FilesFailed.Load())
	}
	if _, err := os.Stat(filepath.Join(h.dir, "changed.bin")); !os.IsNotExist(err) {
		t.Fatalf("changed.bin must not be published on a manifest-digest mismatch: %v", err)
	}
}

// T-XFER: a data channel that accepts a FILE_REQUEST and then goes silent must
// not hang the fetcher. The per-receive stall timeout trips E3005 (fatal,
// resumable) so the session can end and be re-run from the journal.
func TestFetchStallTimeout(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	h.f.stall = 150 * time.Millisecond

	data := make([]byte, 4096)
	dig := md5sum(t, data)

	// The fake sender takes the request and never answers.
	go func() { _, _ = sConn.RecvMsg() }()

	h.enqueue(t, 0, "stall.bin", data, dig)
	h.q.close()

	done := make(chan struct{})
	go func() { h.f.loop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop hung past the stall timeout")
	}

	e, _ := h.failErr.Load().(error)
	if fault.GetCode(e) != fault.E3005 {
		t.Fatalf("fail error = %v, want E3005", e)
	}
}

// The stall timeout also fires mid-file: the header arrives, then the chunk
// stream stops.
func TestFetchStallTimeoutMidStream(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	h.f.stall = 150 * time.Millisecond

	data := make([]byte, 200000)
	dig := md5sum(t, data)

	go func() {
		m, err := sConn.RecvMsg()
		if err != nil {
			return
		}
		req := m.(*wire.FileRequest)
		_ = sConn.SendMsg(&wire.FileHeader{RequestID: req.RequestID, FileID: req.FileID, Size: uint64(len(data)), Digest: dig, ChunkSize: 1 << 16})
		_ = sConn.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: 0, Data: data[:1000]})
		// then silence
	}()

	h.enqueue(t, 0, "stall-mid.bin", data, dig)
	h.q.close()

	done := make(chan struct{})
	go func() { h.f.loop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop hung past the stall timeout")
	}

	e, _ := h.failErr.Load().(error)
	if fault.GetCode(e) != fault.E3005 {
		t.Fatalf("fail error = %v, want E3005", e)
	}
}

// A chunk that arrives at the wrong offset is a fatal protocol fault (E5006).
func TestFetchFatalOnOffsetGap(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	data := make([]byte, 200000)
	dig := md5sum(t, data)

	go func() {
		m, err := sConn.RecvMsg()
		if err != nil {
			return
		}
		req := m.(*wire.FileRequest)
		_ = sConn.SendMsg(&wire.FileHeader{RequestID: req.RequestID, FileID: req.FileID, Size: uint64(len(data)), Digest: dig, ChunkSize: 1 << 16})
		_ = sConn.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: 0, Data: data[:1000]})
		_ = sConn.SendMsg(&wire.FileChunk{RequestID: req.RequestID, Offset: 5000, Data: data[5000:6000]}) // gap
	}()

	h.enqueue(t, 0, "gap.bin", data, dig)
	h.q.close()
	h.f.loop(context.Background())

	e, _ := h.failErr.Load().(error)
	if fault.GetCode(e) != fault.E5006 {
		t.Fatalf("fail error = %v, want E5006", e)
	}
}
