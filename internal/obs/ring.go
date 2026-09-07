package obs

import "sync"

// ring is the trace ring buffer (ARCHITECTURE §15.6): the last n records at TRACE
// fidelity, retained regardless of the configured output level, dumped to the log
// at ERROR on any Fatal fault.
type ring struct {
	mu   sync.Mutex
	buf  []record
	next int
	full bool
}

func newRing(n int) *ring {
	if n <= 0 {
		n = 2048
	}
	return &ring{buf: make([]record, n)}
}

func (r *ring) add(rec record) {
	r.mu.Lock()
	r.buf[r.next] = rec
	r.next++
	if r.next == len(r.buf) {
		r.next = 0
		r.full = true
	}
	r.mu.Unlock()
}

// snapshot returns the retained records in chronological order (oldest first).
func (r *ring) snapshot() []record {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		out := make([]record, r.next)
		copy(out, r.buf[:r.next])
		return out
	}
	out := make([]record, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}
