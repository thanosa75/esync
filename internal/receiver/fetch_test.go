package receiver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
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

	neededBytes atomic.Int64
	neededFiles atomic.Int64
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
		neededBytes: &h.neededBytes,
		neededFiles: &h.neededFiles,
		octx:        obs.Ctx{},
		cfg:         Config{}.withDefaults(),
		conn:        rConn,
		dest:        dest,
		journal:     j,
		algo:        digest.MD5,
		q:           h.q,
		meta:        newMetaStore(),
		hl:          newHardlinkMap(),
		counters:    h.cnt,
		reqID:       &atomic.Uint64{},
		rnd:         nil,
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

// R-16: if a hardlink secondary cannot be linked once its primary publishes,
// finish() must fall back to fetching its content as an ordinary file instead
// of leaving it silently missing (§12.4 content fallback) or letting the run
// exit 0 with a hole in the destination.
//
// MakeHardlink is forced to fail deterministically and portably (no root or
// second filesystem needed to get a genuine EXDEV/ENOTSUP) by pointing it at a
// primary path that was never published — the same trigger
// internal/fsx.TestHardlink uses for MakeHardlink's E7009 ("does-not-exist")
// case — while still exercising the real linkOrFallback/fallbackFetch code
// finish() calls after a successful publish.
func TestFetchHardlinkFallsBackToContentOnLinkFailure(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)

	secData := []byte("secondary content fetched instead of linked")
	secDig := md5sum(t, secData)
	h.f.meta.set(1, fileMeta{rel: "second.dat", size: int64(len(secData)), mode: 0o644, mtime: time.Unix(1000, 0), digest: secDig})

	go serveRequests(t, sConn, map[uint64]*stubFile{1: {data: secData, dig: secDig}})

	// The queue is closed FIRST, which is the production ordering: run.go
	// closes it as soon as decide finishes, and decide runs far ahead of fetch
	// (MFR-0004). A fallback enqueued after that point must still be served —
	// asserting it here is what catches a pushFallback that refuses a closed
	// queue and quietly turns the secondary into a FilesFailed count with no
	// file on disk.
	h.q.close()

	// Simulate what finish() does once a hardlink primary publishes: link its
	// parked secondaries, or fall back. "does-not-exist-primary.dat" was never
	// published, so MakeHardlink fails.
	h.f.linkOrFallback(0, "does-not-exist-primary.dat",
		[]pendingLink{{fileID: 1, rel: "second.dat"}})

	h.f.loop(context.Background())

	if e := h.failErr.Load(); e != nil {
		t.Fatalf("fetch failed: %v", e)
	}
	if h.cnt.FilesFailed.Load() != 0 {
		t.Fatalf("FilesFailed = %d, want 0 (content fallback should succeed)", h.cnt.FilesFailed.Load())
	}
	got, err := os.ReadFile(filepath.Join(h.dir, "second.dat"))
	if err != nil || string(got) != string(secData) {
		t.Fatalf("second.dat must be fetched as ordinary content when the link fails: got %q, err %v", got, err)
	}
	// The fallback enqueues work no GROUP_DECISION ever announced, so it must
	// also grow the progress denominator — otherwise its bytes land in the
	// numerator alone and the progress bar runs past 100%. The decide-side
	// mirror of this fallback always did; the fetch side did not until both
	// were routed through hlFallbackDeps.fetchInstead.
	if got := h.neededFiles.Load(); got != 1 {
		t.Errorf("neededFiles = %d, want 1: the fallback must count toward the progress denominator", got)
	}
	if got := h.neededBytes.Load(); got != int64(len(secData)) {
		t.Errorf("neededBytes = %d, want %d", got, len(secData))
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

// MFR-0011: a worker cancelled while a fetch is in flight — the adaptive tuner
// retiring a channel (§13.4) leaves the session running — must hand the item
// back. Resolving it would leave a hole in the destination on an exit-0 run.
func TestFetchCancelledMidFetchRequeues(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		m, err := sConn.RecvMsg()
		if err != nil {
			return
		}
		req := m.(*wire.FileRequest)
		// Cancel before answering: the fetcher is parked in the receive until
		// this error lands, so it is guaranteed to observe a cancelled ctx.
		cancel()
		_ = sConn.SendMsg(&wire.FileError{RequestID: req.RequestID, Code: 6004, Retryable: 1, Message: "transient"})
	}()

	data := []byte("never arrives")
	h.enqueue(t, 0, "hole.bin", data, md5sum(t, data))
	h.q.close()

	loopDone := make(chan struct{})
	go func() { h.f.loop(ctx); close(loopDone) }()
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop did not return")
	}

	if h.q.drained() {
		t.Fatal("queue drained: a file nobody fetched was marked resolved")
	}
	if len(h.q.items) != 1 || h.q.items[0].fileID != 0 {
		t.Fatalf("queued items = %+v, want the unfetched file back", h.q.items)
	}
	if h.q.items[0].attempt != 0 {
		t.Fatalf("attempt = %d, want 0: cancellation is not a failed attempt", h.q.items[0].attempt)
	}
}

// MFR-0011: cancellation during the retry backoff must requeue too. The retry was
// already decided and counted, so the attempt is consumed — but the file is not
// dropped.
func TestFetchCancelledDuringBackoffRequeues(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		m, err := sConn.RecvMsg()
		if err != nil {
			return
		}
		req := m.(*wire.FileRequest)
		_ = sConn.SendMsg(&wire.FileError{RequestID: req.RequestID, Code: 6004, Retryable: 1, Message: "transient"})
		// The harness fetcher has rnd == nil, so the backoff for attempt 1 is
		// an un-jittered 400ms. Cancel well inside it.
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	data := []byte("retry me")
	h.f.meta.set(0, fileMeta{rel: "retry.bin", size: int64(len(data)), mode: 0o644, mtime: time.Unix(1000, 0), digest: md5sum(t, data)})
	if err := h.q.pushGroup(context.Background(), []needItem{{fileID: 0, size: int64(len(data)), rel: "retry.bin", attempt: 1}}); err != nil {
		t.Fatal(err)
	}
	h.q.close()

	loopDone := make(chan struct{})
	go func() { h.f.loop(ctx); close(loopDone) }()
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch loop did not return")
	}

	if h.q.drained() {
		t.Fatal("queue drained: a file awaiting retry was marked resolved")
	}
	if len(h.q.items) != 1 || h.q.items[0].fileID != 0 {
		t.Fatalf("queued items = %+v, want the pending retry back", h.q.items)
	}
	if h.q.items[0].attempt != 2 {
		t.Fatalf("attempt = %d, want 2: the decided retry is consumed", h.q.items[0].attempt)
	}
	if h.cnt.Retries.Load() != 1 {
		t.Fatalf("Retries = %d, want 1", h.cnt.Retries.Load())
	}
}

// blockPartFile makes OpenPart(fileID) fail with EISDIR (→ E7001, item-class and
// retryable) by occupying the part path with a directory. The failure lands
// after the FILE_HEADER is consumed, while the sender is still streaming — the
// mid-stream abort that desyncs the channel.
func blockPartFile(t *testing.T, dir string, fileID uint64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, ".esync", "parts", strconv.FormatUint(fileID, 10)+".part"), 0o700); err != nil {
		t.Fatal(err)
	}
}

// MFR-0014: an item-class error raised mid-stream leaves the dead request's chunks
// unread on the wire. The item keeps its normal retry budget, but the worker
// must end so the channel is rejoined (§7.3 row 1) — reusing it would read that
// tail as the next request's FILE_HEADER and kill the session with a bogus
// E5001.
func TestFetchMidStreamErrorDropsChannel(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	data := make([]byte, 300000) // several chunks still in flight when we fail
	dig := md5sum(t, data)

	go serveRequests(t, sConn, map[uint64]*stubFile{0: {data: data, dig: dig}})

	blockPartFile(t, h.dir, 0)
	h.enqueue(t, 0, "blocked.bin", data, dig)
	h.q.close()

	lostCh := make(chan bool, 1)
	go func() { lostCh <- h.f.loop(context.Background()) }()
	var lost bool
	select {
	case lost = <-lostCh:
	case <-time.After(10 * time.Second):
		t.Fatal("fetch loop did not return")
	}

	if !lost {
		t.Fatal("loop kept a desynced channel: the next request would read the dead request's tail")
	}
	if e := h.failErr.Load(); e != nil {
		t.Fatalf("session failed on a recoverable item error: %v", e)
	}
	if len(h.q.items) != 1 || h.q.items[0].fileID != 0 {
		t.Fatalf("queued items = %+v, want the file requeued for another channel", h.q.items)
	}
	if h.q.items[0].attempt != 1 {
		t.Fatalf("attempt = %d, want 1: a mid-stream abort still spends a retry", h.q.items[0].attempt)
	}
}

// The companion to the above: a desynced channel is dropped even when the item
// has no retries left. The item is failed and resolved (no rejoin-forever loop
// on failing media), and the channel still ends.
func TestFetchMidStreamErrorDropsChannelWhenRetriesExhausted(t *testing.T) {
	sConn, rConn := connPair(t, 1)
	h := newFetchHarness(t, rConn)
	h.f.cfg.MaxRetries = 0
	data := make([]byte, 100000)
	dig := md5sum(t, data)

	go serveRequests(t, sConn, map[uint64]*stubFile{0: {data: data, dig: dig}})

	blockPartFile(t, h.dir, 0)
	h.enqueue(t, 0, "blocked.bin", data, dig)
	h.q.close()

	lostCh := make(chan bool, 1)
	go func() { lostCh <- h.f.loop(context.Background()) }()
	var lost bool
	select {
	case lost = <-lostCh:
	case <-time.After(10 * time.Second):
		t.Fatal("fetch loop did not return")
	}

	if !lost {
		t.Fatal("loop kept a desynced channel after failing the item")
	}
	if h.cnt.FilesFailed.Load() != 1 {
		t.Fatalf("FilesFailed = %d, want 1", h.cnt.FilesFailed.Load())
	}
	if !h.q.drained() {
		t.Fatalf("queue not drained: a permanently failed file must be resolved, items=%+v", h.q.items)
	}
}
