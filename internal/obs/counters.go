package obs

import "sync/atomic"

// Counters are the live session counters rendered in the final summary
// (ARCHITECTURE §15.7). All fields are safe for concurrent use.
type Counters struct {
	FilesTransferred atomic.Int64
	FilesSkipped     atomic.Int64
	FilesExcluded    atomic.Int64
	FilesRejected    atomic.Int64
	FilesFailed      atomic.Int64
	Bytes            atomic.Int64
	Dirs             atomic.Int64
	Symlinks         atomic.Int64
	SymlinksRejected atomic.Int64
	Retries          atomic.Int64
	RetriesOK        atomic.Int64
	CacheHit         atomic.Int64
	CacheMiss        atomic.Int64
	LogDrops         atomic.Int64
}
