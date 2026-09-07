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
		q.done()
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
		q.done()
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
		q.done()
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
	q.done()

	if _, ok := q.pop(context.Background()); ok {
		t.Fatal("queue should be drained")
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
