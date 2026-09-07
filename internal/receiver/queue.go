package receiver

import (
	"context"
	"sort"
	"sync"
)

// needItem is one file the receiver must fetch.
type needItem struct {
	fileID  uint64
	groupID uint32
	size    int64
	rel     string
	attempt int
}

// needQueue is the ordered, bounded, resumable need queue (ARCHITECTURE §13.2,
// §13.6). Groups are appended in ascending order; within a group items are
// ordered largest-first. It never exceeds its capacity, never drops an item,
// and never yields the same item twice [P-QUEUE-01]. pushGroup blocks while the
// queue is full, which is the backpressure that stops the decide pool emitting
// CREDIT (§13.5).
type needQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	items    []needItem
	capacity int
	inflight int  // popped but not yet terminally resolved (or requeued)
	closed   bool // no more brand-new groups will be pushed

	pushed  uint64
	yielded uint64
}

func newNeedQueue(capacity int) *needQueue {
	q := &needQueue{capacity: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// pushGroup enqueues a whole group's needed items, sorted largest-first. It
// blocks until every item fits under the capacity ceiling, or ctx is done (in
// which case it returns ctx.Err() and enqueues nothing).
func (q *needQueue) pushGroup(ctx context.Context, items []needItem) error {
	if len(items) == 0 {
		return nil
	}
	sorted := append([]needItem(nil), items...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].size > sorted[j].size })

	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.notFull.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items)+len(sorted) > q.capacity && !q.closed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		q.notFull.Wait()
	}
	if q.closed {
		return errQueueClosed
	}
	q.items = append(q.items, sorted...)
	q.pushed += uint64(len(sorted))
	q.notEmpty.Broadcast()
	return nil
}

// requeue puts a previously popped item back for another attempt. It does not
// block on capacity (the slot was already accounted) and never drops the item.
func (q *needQueue) requeue(it needItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inflight > 0 {
		q.inflight--
	}
	q.items = append(q.items, it)
	q.notEmpty.Broadcast()
}

// pop returns the next item. ok is false once the queue is drained: no items,
// no in-flight items, and closed — or ctx is done.
func (q *needQueue) pop(ctx context.Context) (needItem, bool) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.notEmpty.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return needItem{}, false
		}
		if len(q.items) > 0 {
			it := q.items[0]
			q.items = q.items[1:]
			q.inflight++
			q.yielded++
			q.notFull.Broadcast()
			return it, true
		}
		if q.closed && q.inflight == 0 {
			return needItem{}, false
		}
		q.notEmpty.Wait()
	}
}

// done marks one popped item as terminally resolved (published or permanently
// failed).
func (q *needQueue) done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inflight > 0 {
		q.inflight--
	}
	if len(q.items) == 0 && q.closed && q.inflight == 0 {
		q.notEmpty.Broadcast()
	}
}

// close signals that no further groups will be pushed. pop drains what remains.
func (q *needQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

// waitDrained blocks until the queue reports the same "nothing left" state pop
// signals via ok=false (closed, no queued items, none in flight), or ctx is
// done. Unlike close, which only stops new groups from being pushed, this is
// the point past which no fetcher will ever pop another item — the correct
// moment for the adaptive tuner (§13.4) to stop opening new channels, rather
// than the earlier and much weaker signal that decide has merely finished
// enqueueing work while a large backlog may still be draining.
func (q *needQueue) waitDrained(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.notEmpty.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return
		}
		if q.closed && len(q.items) == 0 && q.inflight == 0 {
			return
		}
		q.notEmpty.Wait()
	}
}

func (q *needQueue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
