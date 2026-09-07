package obs

import (
	"fmt"
	"strings"
	"time"
)

// Summary renders the end-of-session counter block (ARCHITECTURE §15.7) at INFO.
// It is emitted on every exit path (REQ-ERR-031), so it bypasses the level
// filter. failedItems, if given, are listed one per line.
func Summary(ctx Ctx, outcome string, exit int, elapsed time.Duration, failedItems ...string) {
	l := ctx.lg
	if l == nil {
		return
	}
	c := &l.counters
	c.LogDrops.Store(l.drops.Load())

	var b strings.Builder
	fmt.Fprintf(&b, "\nesync summary  session=%s  role=%s  outcome=%s  exit=%d\n\n",
		ctx.Session(), l.role, outcome, exit)
	fmt.Fprintf(&b, "  transferred   %8d files   %14d bytes\n", c.FilesTransferred.Load(), c.Bytes.Load())
	fmt.Fprintf(&b, "  skipped       %8d files\n", c.FilesSkipped.Load())
	fmt.Fprintf(&b, "  excluded      %8d entries\n", c.FilesExcluded.Load())
	fmt.Fprintf(&b, "  rejected      %8d entries\n", c.FilesRejected.Load())
	fmt.Fprintf(&b, "  failed        %8d files\n", c.FilesFailed.Load())
	fmt.Fprintf(&b, "  directories   %8d created\n", c.Dirs.Load())
	fmt.Fprintf(&b, "  symlinks      %8d created, %d rejected\n", c.Symlinks.Load(), c.SymlinksRejected.Load())
	fmt.Fprintf(&b, "  retries       %8d (%d succeeded)\n", c.Retries.Load(), c.RetriesOK.Load())
	fmt.Fprintf(&b, "  hash cache    hit %d / miss %d\n", c.CacheHit.Load(), c.CacheMiss.Load())
	fmt.Fprintf(&b, "  elapsed       %s\n", elapsed.Round(time.Second))
	fmt.Fprintf(&b, "  log           %8d records dropped\n", c.LogDrops.Load())
	if len(failedItems) > 0 {
		b.WriteString("\n  failed items:\n")
		for _, it := range failedItems {
			fmt.Fprintf(&b, "    %s\n", it)
		}
	}

	rec := record{ts: time.Now().UTC(), level: LevelInfo, role: l.role, msg: b.String()}
	l.emit(l.formatRecord(rec))
}
