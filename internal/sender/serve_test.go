package sender

import (
	"context"
	"crypto/md5"
	"os"
	"path/filepath"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/plan"
	"esync/internal/wire"
)

func buildPlan(t *testing.T, root string) *plan.Plan {
	t.Helper()
	es, _, err := collectWalk(t, walkConfig{root: root})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	pl, err := plan.Build(es, plan.Options{})
	if err != nil {
		t.Fatalf("plan.Build: %v", err)
	}
	return pl
}

func newServicer(t *testing.T, root string, pl *plan.Plan, ds *digestStore, tr *tracker) (*servicer, *chanReceiver) {
	t.Helper()
	snd, rcv := connPair(t, 1)
	s := &servicer{
		ctx:       obs.Ctx{},
		conn:      snd,
		entries:   pl.Entries(),
		root:      root,
		algo:      digest.MD5,
		chunkSize: 8,
		readSem:   make(chan struct{}, 4),
		digests:   ds,
		tr:        tr,
	}
	return s, &chanReceiver{t: t, conn: rcv}
}

type chanReceiver struct {
	t    *testing.T
	conn *channel.Conn
}

func (c *chanReceiver) send(m wire.Message) {
	if err := c.conn.SendMsg(m); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

func (c *chanReceiver) recv() wire.Message {
	m, err := c.conn.RecvMsg()
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return m
}

// Component test: a FILE_REQUEST is answered with a header, contiguous ascending
// chunks, and a FILE_COMPLETE whose digest matches the content.
func TestServeHappyPath(t *testing.T) {
	root := t.TempDir()
	content := []byte("the quick brown fox jumps over thirteen lazy dogs!!")
	writeFile(t, filepath.Join(root, "d", "f.bin"), content)
	pl := buildPlan(t, root)

	fid, ok := pl.FileID("d/f.bin")
	if !ok {
		t.Fatal("file_id not found")
	}
	sum := md5.Sum(content)
	ds := newDigestStore()
	ds.set(fid, sum[:])

	tr := newTracker([]uint64{0})
	s, rx := newServicer(t, root, pl, ds, tr)
	go func() { _ = s.run(context.Background()) }()

	rx.send(&wire.FileRequest{RequestID: 7, FileID: fid, MaxChunk: 8})

	h := rx.recv()
	hdr, ok := h.(*wire.FileHeader)
	if !ok {
		t.Fatalf("first message = %T, want FILE_HEADER", h)
	}
	if hdr.Size != uint64(len(content)) || hdr.RequestID != 7 {
		t.Fatalf("bad header: %+v", hdr)
	}

	var got []byte
	var nextOff uint64
	for {
		m := rx.recv()
		switch v := m.(type) {
		case *wire.FileChunk:
			if v.Offset != nextOff {
				t.Fatalf("chunk offset %d, want %d", v.Offset, nextOff)
			}
			got = append(got, v.Data...)
			nextOff += uint64(len(v.Data))
		case *wire.FileComplete:
			if string(got) != string(content) {
				t.Fatalf("reassembled %q, want %q", got, content)
			}
			if string(v.DigestFull) != string(sum[:]) {
				t.Fatalf("digest_full mismatch")
			}
			if v.BytesSent != uint64(len(content)) {
				t.Fatalf("bytes_sent = %d", v.BytesSent)
			}
			return
		default:
			t.Fatalf("unexpected %T", m)
		}
	}
}

// Resume: a request with a non-zero offset streams only the tail.
func TestServeResumeOffset(t *testing.T) {
	root := t.TempDir()
	content := []byte("0123456789abcdefghij")
	writeFile(t, filepath.Join(root, "f"), content)
	pl := buildPlan(t, root)
	fid, _ := pl.FileID("f")

	ds := newDigestStore()
	s, rx := newServicer(t, root, pl, ds, newTracker([]uint64{0}))
	go func() { _ = s.run(context.Background()) }()

	rx.send(&wire.FileRequest{RequestID: 1, FileID: fid, Offset: 10, MaxChunk: 8})
	if _, ok := rx.recv().(*wire.FileHeader); !ok {
		t.Fatal("want header")
	}
	var got []byte
	for {
		switch v := rx.recv().(type) {
		case *wire.FileChunk:
			if v.Offset < 10 {
				t.Fatalf("chunk offset %d below resume point", v.Offset)
			}
			got = append(got, v.Data...)
		case *wire.FileComplete:
			if string(got) != "abcdefghij" {
				t.Fatalf("tail = %q", got)
			}
			return
		}
	}
}

// An unknown file_id is answered with FILE_ERROR E5007.
func TestServeUnknownFileID(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "f"), []byte("x"))
	pl := buildPlan(t, root)

	s, rx := newServicer(t, root, pl, newDigestStore(), newTracker([]uint64{0}))
	go func() { _ = s.run(context.Background()) }()

	rx.send(&wire.FileRequest{RequestID: 1, FileID: 9999})
	fe, ok := rx.recv().(*wire.FileError)
	if !ok {
		t.Fatalf("want FILE_ERROR, got %T", fe)
	}
	if fe.Code != codeNum(fault.E5007) {
		t.Fatalf("code = %d, want %d", fe.Code, codeNum(fault.E5007))
	}
}

// A vanished source file is answered with a non-retryable FILE_ERROR E6002.
func TestServeVanishedFile(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "gone")
	writeFile(t, p, []byte("here for now"))
	pl := buildPlan(t, root)
	fid, _ := pl.FileID("gone")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}

	tr := newTracker([]uint64{0})
	s, rx := newServicer(t, root, pl, newDigestStore(), tr)
	go func() { _ = s.run(context.Background()) }()

	rx.send(&wire.FileRequest{RequestID: 1, FileID: fid})
	fe, ok := rx.recv().(*wire.FileError)
	if !ok {
		t.Fatalf("want FILE_ERROR, got %T", fe)
	}
	if fe.Code != codeNum(fault.E6002) || fe.Retryable != 0 {
		t.Fatalf("bad error: code=%d retryable=%d", fe.Code, fe.Retryable)
	}
}

// T-PROTO-04: the sender's termination condition — every group decided and every
// needed file resolved — releases the drain wait.
func TestTrackerTerminationCondition(t *testing.T) {
	tr := newTracker([]uint64{0, 1024})

	// group 0 needs index 3 (file_id 3); group 1 needs nothing.
	if err := tr.decision(&wire.GroupDecision{GroupID: 0, Needed: []uint16{3}}); err != nil {
		t.Fatal(err)
	}
	if err := tr.decision(&wire.GroupDecision{GroupID: 1}); err != nil {
		t.Fatal(err)
	}

	// not done yet: file_id 3 is unresolved.
	if err := tr.wait(context.Background(), 100*time.Millisecond); fault.GetCode(err) != fault.E9002 {
		t.Fatalf("wait should have hit the drain watchdog, got %v", err)
	}

	tr.reqStarted(50, 3)
	tr.reqCompleted(50, 3, 128)

	if err := tr.wait(context.Background(), time.Second); err != nil {
		t.Fatalf("wait after resolution: %v", err)
	}
}

// The receiver sends GROUP_DECISION at the head of its decision pass, before it
// enqueues that group's needed files and long before it requests them, so
// "every group decided" can precede the last FILE_REQUEST by far more than
// --drain-timeout. A session that is still moving must not trip the watchdog,
// however long the tail runs.
func TestTrackerDrainWatchdogIgnoresLiveTransfer(t *testing.T) {
	tr := newTracker([]uint64{0})
	if err := tr.decision(&wire.GroupDecision{GroupID: 0, Needed: []uint16{0, 1}}); err != nil {
		t.Fatal(err)
	}

	const drain = 50 * time.Millisecond
	go func() {
		// Work spanning many drain windows, each with steady chunk activity.
		tr.reqStarted(1, 0)
		for i := 0; i < 40; i++ {
			time.Sleep(drain / 5)
			tr.bump() // one streamed chunk
		}
		tr.reqCompleted(1, 0, 1024)
		tr.reqStarted(2, 1)
		tr.reqCompleted(2, 1, 1024)
	}()

	if err := tr.wait(context.Background(), drain); err != nil {
		t.Fatalf("watchdog tripped on a live transfer: %v", err)
	}
}

// ...but a session that stops moving with work outstanding still trips it: that
// is the stall E9002 exists to catch.
func TestTrackerDrainWatchdogTripsOnStall(t *testing.T) {
	tr := newTracker([]uint64{0})
	if err := tr.decision(&wire.GroupDecision{GroupID: 0, Needed: []uint16{0}}); err != nil {
		t.Fatal(err)
	}
	tr.reqStarted(1, 0) // in flight, and then nothing ever happens again

	if err := tr.wait(context.Background(), 30*time.Millisecond); fault.GetCode(err) != fault.E9002 {
		t.Fatalf("a stalled session should trip the watchdog, got %v", err)
	}
}

// inProgressGroups reports only decided groups with an unresolved needed
// file, and drops a group once every needed file is resolved.
func TestTrackerInProgressGroups(t *testing.T) {
	tr := newTracker([]uint64{0, 1024, 2048})
	if got := tr.inProgressGroups(); len(got) != 0 {
		t.Fatalf("in progress before any decision = %v, want none", got)
	}

	// group 0 needs index 3 (file_id 3); group 1 needs index 0 of its range
	// (file_id 1024); group 2 needs nothing.
	if err := tr.decision(&wire.GroupDecision{GroupID: 0, Needed: []uint16{3}}); err != nil {
		t.Fatal(err)
	}
	if err := tr.decision(&wire.GroupDecision{GroupID: 1, Needed: []uint16{0}}); err != nil {
		t.Fatal(err)
	}
	if err := tr.decision(&wire.GroupDecision{GroupID: 2}); err != nil {
		t.Fatal(err)
	}

	got := tr.inProgressGroups()
	want := []uint32{0, 1}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("in progress = %v, want %v", got, want)
	}

	tr.reqStarted(50, 3)
	tr.reqCompleted(50, 3, 128)

	got = tr.inProgressGroups()
	want = []uint32{1}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("in progress after group 0 resolves = %v, want %v", got, want)
	}
}

// A duplicate GROUP_DECISION for one group is E5009.
func TestTrackerDuplicateDecision(t *testing.T) {
	tr := newTracker([]uint64{0})
	if err := tr.decision(&wire.GroupDecision{GroupID: 0}); err != nil {
		t.Fatal(err)
	}
	if err := tr.decision(&wire.GroupDecision{GroupID: 0}); fault.GetCode(err) != fault.E5009 {
		t.Fatalf("code = %v, want E5009", fault.GetCode(err))
	}
}

// The credit gate blocks acquisition beyond the granted amount and releases on
// add.
func TestCreditGateBlocksAndReleases(t *testing.T) {
	g := newCreditGate(1)
	if err := g.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan error, 1)
	go func() { blocked <- g.acquire(context.Background()) }()
	select {
	case <-blocked:
		t.Fatal("second acquire should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	g.add(1)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("acquire after add: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire never unblocked after add")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.acquire(ctx); err == nil {
		t.Fatal("acquire on cancelled context should fail")
	}
}
