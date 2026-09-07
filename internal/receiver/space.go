package receiver

import (
	"errors"
	"sync"

	"esync/internal/fault"
)

var errQueueClosed = errors.New("need queue closed")

// spaceGuard implements the free-space guard (ARCHITECTURE §12.7). After each
// GROUP_DECISION the receiver adds the group's needed byte count and compares
// the running requirement against the free space measured at startup minus a
// 64 MiB margin: a WARN at the first crossing, then a fatal E7003 once the
// deficit exceeds the margin — before writing rather than after filling the disk.
type spaceGuard struct {
	mu       sync.Mutex
	free     int64
	margin   int64
	required int64
	warned   bool
}

func newSpaceGuard(freeBytes uint64, margin int64) *spaceGuard {
	f := int64(freeBytes)
	if f < 0 {
		f = 0
	}
	return &spaceGuard{free: f, margin: margin}
}

// reserve adds bytes to the running requirement and reports whether a WARN
// should be logged now and whether the session must stop (E7003).
func (g *spaceGuard) reserve(bytes int64) (warn bool, fatal error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.required += bytes

	over := g.required - (g.free - g.margin) // > 0 means the budget is short
	if over > 0 && !g.warned {
		g.warned = true
		warn = true
	}
	if over > g.margin {
		fatal = fault.Newf(fault.E7003, "free-space guard", "",
			nil, "need %d bytes, %d available (margin %d)", g.required, g.free, g.margin)
	}
	return warn, fatal
}
