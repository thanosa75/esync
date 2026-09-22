package receiver

import (
	"context"
	"sync"
	"testing"
	"time"
)

// drainAll closes the queue and pops every remaining item, calling done() for
// each, and returns the fileIDs in yield order.
func drainAll(t *testing.T, q *needQueue) []uint64 {
	t.Helper()
	q.close()
	var out []uint64
	for {
		it, ok := q.pop(context.Background())
		if !ok {
			return out
		}
		out = append(out, it.fileID)
		q.done(it.groupID)
	}
}

// P-QUEUE-01: within a group the queue yields needed files largest-first.
func TestQueueLargestFirstWithinGroup(t *testing.T) {
	q := newNeedQueue(64)
	err := q.pushGroup(context.Background(), []needItem{
		{fileID: 10, size: 5},
		{fileID: 11, size: 500},
		{fileID: 12, size: 50},
		{fileID: 13, size: 500}, // tie keeps input order (stable)
	})
	if err != nil {
		t.Fatalf("pushGroup: %v", err)
	}
	got := drainAll(t, q)
	want := []uint64{11, 13, 12, 10}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// P-QUEUE-01: groups are yielded in ascending push order, before any reordering
// within a later group can jump ahead.
func TestQueueAscendingGroupOrder(t *testing.T) {
	q := newNeedQueue(64)
	_ = q.pushGroup(context.Background(), []needItem{{fileID: 1, size: 1}, {fileID: 2, size: 9}})
	_ = q.pushGroup(context.Background(), []needItem{{fileID: 3, size: 1}, {fileID: 4, size: 9}})
	got := drainAll(t, q)
	want := []uint64{2, 1, 4, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

// P-QUEUE-01: the queue never exceeds its capacity and pushGroup blocks until
// space is freed; nothing is dropped and nothing is yielded twice.
func TestQueueBoundedNeverDropsNeverDoubleYields(t *testing.T) {
	const cap = 4
	q := newNeedQueue(cap)
	if err := q.pushGroup(context.Background(), []needItem{
		{fileID: 1, size: 4}, {fileID: 2, size: 3}, {fileID: 3, size: 2}, {fileID: 4, size: 1},
	}); err != nil {
		t.Fatalf("pushGroup 1: %v", err)
	}
	if d := q.depth(); d != cap {
		t.Fatalf("depth = %d, want %d", d, cap)
	}

	pushed := make(chan struct{})
	go func() {
		_ = q.pushGroup(context.Background(), []needItem{{fileID: 5, size: 2}, {fileID: 6, size: 1}})
		close(pushed)
	}()

	select {
	case <-pushed:
		t.Fatal("pushGroup returned while the queue was full")
	case <-time.After(50 * time.Millisecond):
	}
	if d := q.depth(); d > cap {
		t.Fatalf("depth %d exceeded capacity %d", d, cap)
	}

	seen := map[uint64]int{}
	// free two slots
	for i := 0; i < 2; i++ {
		it, ok := q.pop(context.Background())
		if !ok {
			t.Fatal("pop returned !ok early")
		}
		seen[it.fileID]++
		q.done(it.groupID)
	}
	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("pushGroup did not unblock after space was freed")
	}

	q.close()
	for {
		it, ok := q.pop(context.Background())
		if !ok {
			break
		}
		if d := q.depth(); d > cap {
			t.Fatalf("depth %d exceeded capacity %d", d, cap)
		}
		seen[it.fileID]++
		q.done(it.groupID)
	}
	for id := uint64(1); id <= 6; id++ {
		if seen[id] != 1 {
			t.Fatalf("file %d yielded %d times, want 1 (seen=%v)", id, seen[id], seen)
		}
	}
}

// requeue returns a popped item for another attempt without losing it and lets
// the queue still terminate cleanly.
func TestQueueRequeue(t *testing.T) {
	q := newNeedQueue(8)
	_ = q.pushGroup(context.Background(), []needItem{{fileID: 7, size: 1}})
	q.close()

	it, ok := q.pop(context.Background())
	if !ok || it.fileID != 7 {
		t.Fatalf("first pop = %v, %v", it, ok)
	}
	it.attempt++
	q.requeue(it)

	it2, ok := q.pop(context.Background())
	if !ok || it2.fileID != 7 || it2.attempt != 1 {
		t.Fatalf("second pop = %+v, %v", it2, ok)
	}
	q.done(it2.groupID)

	if _, ok := q.pop(context.Background()); ok {
		t.Fatal("queue should be drained")
	}
}

// inProgressGroups reports only groups with an item not yet done(), and drops
// a group once every one of its items is done() — regardless of retries in
// between (requeue must not clear a group's pending count).
func TestQueueInProgressGroups(t *testing.T) {
	q := newNeedQueue(64)
	if got := q.inProgressGroups(); len(got) != 0 {
		t.Fatalf("in progress before any push = %v, want none", got)
	}

	_ = q.pushGroup(context.Background(), []needItem{{fileID: 1, groupID: 5, size: 1}, {fileID: 2, groupID: 5, size: 2}})
	_ = q.pushGroup(context.Background(), []needItem{{fileID: 3, groupID: 9, size: 1}})

	got := q.inProgressGroups()
	want := []uint32{5, 9}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("in progress = %v, want %v", got, want)
	}

	// pop and requeue one item of group 5: still in progress, not dropped.
	it, ok := q.pop(context.Background())
	if !ok || it.groupID != 5 {
		t.Fatalf("first pop = %+v, %v", it, ok)
	}
	q.requeue(it)
	if got := q.inProgressGroups(); len(got) != 2 {
		t.Fatalf("in progress after requeue = %v, want 2 groups still pending", got)
	}

	// drain everything: every group should disappear as its last item is done().
	q.close()
	for {
		it, ok := q.pop(context.Background())
		if !ok {
			break
		}
		q.done(it.groupID)
	}
	if got := q.inProgressGroups(); len(got) != 0 {
		t.Fatalf("in progress after full drain = %v, want none", got)
	}
}

// pop unblocks and returns !ok when its context is cancelled.
func TestQueuePopContextCancel(t *testing.T) {
	q := newNeedQueue(8)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var ok bool
	go func() {
		defer wg.Done()
		_, ok = q.pop(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()
	if ok {
		t.Fatal("pop returned ok after context cancel")
	}
}

// TestQueuePushFallbackNeverBlocksWhenFull pins the property that makes the
// R-16 content fallback safe: pushFallback is called from a fetcher worker —
// the same goroutine that pops — so it must never wait for a free slot. If it
// blocked like pushGroup, a full queue with every worker parked in the
// fallback would leave nobody to pop, and the session would hang until the
// drain watchdog fired a misleading E9002.
func TestQueuePushFallbackNeverBlocksWhenFull(t *testing.T) {
	q := newNeedQueue(1)
	if err := q.pushGroup(context.Background(), []needItem{{fileID: 1, groupID: 7, size: 10, rel: "a"}}); err != nil {
		t.Fatalf("pushGroup: %v", err)
	}
	if got := q.depth(); got != 1 {
		t.Fatalf("depth = %d, want 1 (queue must be at capacity for this test to mean anything)", got)
	}

	done := make(chan struct{})
	go func() {
		q.pushFallback(needItem{fileID: 2, groupID: 7, size: 10, rel: "b"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pushFallback blocked on a full queue; it is called from the popping goroutine, so this deadlocks the drain")
	}

	// The item must really be queued and must be accounted to its group, so
	// the run cannot report drained while the fallback is still outstanding.
	if got := q.depth(); got != 2 {
		t.Errorf("depth = %d, want 2 (fallback item queued despite capacity)", got)
	}
	if got := q.inProgressGroups(); len(got) != 1 || got[0] != 7 {
		t.Errorf("inProgressGroups = %v, want [7]", got)
	}
	q.close()
	if q.drained() {
		t.Error("drained() is true with a fallback item still queued")
	}
}

// TestQueuePushFallbackAfterCloseIsStillServed pins the other half of
// pushFallback's contract. run.go closes the queue the moment decide finishes,
// and decide runs far ahead of fetch (MFR-0004 observed ~20 minutes), so a
// hardlink secondary whose link fails during the fetch phase is almost always
// enqueued *after* close. Refusing it there leaves the file absent from the
// destination — the exact R-16 hole — so close, which only means "no new
// groups", must not reject it. The caller always holds an item in flight, so
// pop cannot have given up yet and the item is guaranteed to be served.
func TestQueuePushFallbackAfterCloseIsStillServed(t *testing.T) {
	q := newNeedQueue(4)
	if err := q.pushGroup(context.Background(), []needItem{{fileID: 1, groupID: 3, size: 10, rel: "a"}}); err != nil {
		t.Fatalf("pushGroup: %v", err)
	}
	ctx := context.Background()
	primary, ok := q.pop(ctx) // in flight, as a fetcher worker would be
	if !ok {
		t.Fatal("pop returned no item")
	}
	q.close()

	q.pushFallback(needItem{fileID: 2, groupID: 3, size: 10, rel: "b"})
	q.done(primary.groupID)

	got, ok := q.pop(ctx)
	if !ok {
		t.Fatal("pop returned nothing: the fallback was dropped on a closed queue, so the secondary would be missing from the destination")
	}
	if got.fileID != 2 {
		t.Fatalf("popped file %d, want the fallback item 2", got.fileID)
	}
	q.done(got.groupID)
	if !q.drained() {
		t.Error("queue should be drained once the fallback is resolved")
	}
}
