package sender

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"sync"
	"time"

	"esync/internal/fault"
	"esync/internal/wire"
)

// groupEntries is the fixed number of plan entries per group (§10.4).
const groupEntries = 1024

// digestStore holds the per-file content digests computed by the manifest
// publisher, so the servicers can re-assert them in FILE_HEADER and verify the
// streamed bytes.
type digestStore struct {
	mu sync.RWMutex
	m  map[uint64][]byte
}

func newDigestStore() *digestStore { return &digestStore{m: map[uint64][]byte{}} }

func (d *digestStore) set(id uint64, dg []byte) {
	d.mu.Lock()
	d.m[id] = dg
	d.mu.Unlock()
}

func (d *digestStore) get(id uint64) ([]byte, bool) {
	d.mu.RLock()
	dg, ok := d.m[id]
	d.mu.RUnlock()
	return dg, ok
}

// creditGate is the group-manifest credit semaphore (INV-2, ARCHITECTURE §11.1):
// the publisher may never have more un-decided GROUP_MANIFESTs outstanding than
// the receiver has granted.
type creditGate struct {
	mu    sync.Mutex
	cond  *sync.Cond
	avail int
}

func newCreditGate(initial int) *creditGate {
	g := &creditGate{avail: initial}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *creditGate) add(n int) {
	g.mu.Lock()
	g.avail += n
	g.cond.Broadcast()
	g.mu.Unlock()
}

// acquire blocks until one unit of credit is available, then consumes it.
func (g *creditGate) acquire(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})
	defer stop()

	g.mu.Lock()
	defer g.mu.Unlock()
	for g.avail <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.cond.Wait()
	}
	g.avail--
	return nil
}

// tracker is the sender's session-completion accounting. Termination (§13.7) is
// reached when every group has been decided, every needed file_id has been
// resolved (completed, permanently failed, or cancelled), and nothing is
// in flight.
type tracker struct {
	mu            sync.Mutex
	totalGroups   int
	groupsDecided int
	decidedSet    map[uint32]bool
	needed        map[uint64]bool
	resolved      map[uint64]bool
	reqToFile     map[uint64]uint64
	inflight      int
	completed     []uint64
	completedSet  map[uint64]bool
	failedSet     map[uint64]bool
	bytesTotal    uint64
	skippedCount  uint64
	rejectedCount uint64

	allDecidedCh chan struct{}
	doneCh       chan struct{}
	decidedDone  bool
	doneClosed   bool
}

func newTracker(totalGroups int) *tracker {
	t := &tracker{
		totalGroups:  totalGroups,
		decidedSet:   map[uint32]bool{},
		needed:       map[uint64]bool{},
		resolved:     map[uint64]bool{},
		reqToFile:    map[uint64]uint64{},
		completedSet: map[uint64]bool{},
		failedSet:    map[uint64]bool{},
		allDecidedCh: make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	t.mu.Lock()
	t.reevalLocked()
	t.mu.Unlock()
	return t
}

func (t *tracker) reevalLocked() {
	if !t.decidedDone && t.groupsDecided >= t.totalGroups {
		t.decidedDone = true
		close(t.allDecidedCh)
	}
	if t.doneClosed {
		return
	}
	if t.groupsDecided < t.totalGroups || t.inflight > 0 {
		return
	}
	for fid := range t.needed {
		if !t.resolved[fid] {
			return
		}
	}
	t.doneClosed = true
	close(t.doneCh)
}

// decision records a GROUP_DECISION. A duplicate for one group is E5009.
func (t *tracker) decision(m *wire.GroupDecision) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.decidedSet[m.GroupID] {
		return fault.Newf(fault.E5009, "group decision", groupSubject(m.GroupID), nil, "duplicate decision")
	}
	t.decidedSet[m.GroupID] = true
	t.groupsDecided++
	first := uint64(m.GroupID) * groupEntries
	for _, idx := range m.Needed {
		t.needed[first+uint64(idx)] = true
	}
	t.skippedCount += uint64(m.SkippedCount)
	t.rejectedCount += uint64(len(m.Rejected))
	t.reevalLocked()
	return nil
}

func (t *tracker) reqStarted(reqID, fileID uint64) {
	t.mu.Lock()
	t.reqToFile[reqID] = fileID
	t.inflight++
	t.mu.Unlock()
}

func (t *tracker) reqCompleted(reqID, fileID uint64, bytesSent uint64) {
	t.mu.Lock()
	t.finishLocked(reqID, true)
	if !t.completedSet[fileID] {
		t.completedSet[fileID] = true
		t.completed = append(t.completed, fileID)
		t.bytesTotal += bytesSent
	}
	t.reevalLocked()
	t.mu.Unlock()
}

// reqFailed ends a request. resolve is true for a permanent failure (the file
// will not be retried); false for a retryable failure (the receiver re-requests).
// permanent additionally tallies the file as failed for the session summary.
func (t *tracker) reqFailed(reqID uint64, resolve, permanent bool) {
	t.mu.Lock()
	fid, ok := t.reqToFile[reqID]
	t.finishLocked(reqID, resolve)
	if ok && permanent {
		t.failedSet[fid] = true
	}
	t.reevalLocked()
	t.mu.Unlock()
}

func (t *tracker) reqCancelled(reqID uint64) {
	t.mu.Lock()
	t.finishLocked(reqID, true)
	t.reevalLocked()
	t.mu.Unlock()
}

func (t *tracker) finishLocked(reqID uint64, resolve bool) {
	fid, ok := t.reqToFile[reqID]
	if !ok {
		return
	}
	delete(t.reqToFile, reqID)
	t.inflight--
	if resolve {
		t.resolved[fid] = true
	}
}

// wait blocks until termination, ctx cancellation, or — once every group is
// decided — the drain watchdog (E9002).
func (t *tracker) wait(ctx context.Context, drainTimeout time.Duration) error {
	select {
	case <-t.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.allDecidedCh:
	}
	select {
	case <-t.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(drainTimeout):
		return fault.New(fault.E9002, "drain watchdog", "", nil)
	}
}

func (t *tracker) completionDigest() [32]byte {
	t.mu.Lock()
	ids := append([]uint64(nil), t.completed...)
	t.mu.Unlock()
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

func (t *tracker) counts() (transferred, skipped, failed, bytesSent uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return uint64(len(t.completedSet)), t.skippedCount, uint64(len(t.failedSet)), t.bytesTotal
}

func groupSubject(g uint32) string {
	return "group " + strconv.Itoa(int(g))
}
