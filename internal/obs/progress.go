package obs

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Progress is the stderr progress renderer (ARCHITECTURE §15.10), a sink separate
// from the log. On a TTY it repaints a single status line; on a non-TTY it emits
// one plain line every interval so piped and CI output stays clean.
type Progress struct {
	interval time.Duration
	tty      bool

	doneBytes  atomic.Int64
	totalBytes atomic.Int64
	doneFiles  atomic.Int64
	totalFiles atomic.Int64

	stop chan struct{}
	once sync.Once
	w    *os.File
}

// NewProgress starts the renderer. interval <= 0 uses 5s.
func NewProgress(ctx Ctx, interval time.Duration) *Progress {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	p := &Progress{
		interval: interval,
		tty:      isTTY(os.Stderr),
		stop:     make(chan struct{}),
		w:        os.Stderr,
	}
	Go(ctx, "progress", func() error { p.loop(); return nil })
	return p
}

// Update sets the current byte and file counts.
func (p *Progress) Update(doneBytes, totalBytes, doneFiles, totalFiles int64) {
	p.doneBytes.Store(doneBytes)
	p.totalBytes.Store(totalBytes)
	p.doneFiles.Store(doneFiles)
	p.totalFiles.Store(totalFiles)
}

// Stop halts the renderer and clears the status line on a TTY.
func (p *Progress) Stop() {
	p.once.Do(func() { close(p.stop) })
}

func (p *Progress) loop() {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			if p.tty {
				fmt.Fprint(p.w, "\r\x1b[K")
			}
			return
		case <-t.C:
			p.render()
		}
	}
}

func (p *Progress) render() {
	db, tb := p.doneBytes.Load(), p.totalBytes.Load()
	df, tf := p.doneFiles.Load(), p.totalFiles.Load()
	if p.tty {
		fmt.Fprintf(p.w, "\r\x1b[K  %s / %s   %d / %d files",
			humanBytes(db), humanBytes(tb), df, tf)
	} else {
		fmt.Fprintf(p.w, "%s progress  %s / %s   %d / %d files\n",
			time.Now().UTC().Format(tsLayout), humanBytes(db), humanBytes(tb), df, tf)
	}
}

func isTTY(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// humanRate renders a bytes/sec rate in decimal (SI) units — B/s, KB/s,
// MB/s, ... — the convention bandwidth is normally reported in, unlike
// humanBytes above which uses binary units for on-disk sizes.
func humanRate(bytesPerSec float64) string {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	const unit = 1000.0
	if bytesPerSec < unit {
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	}
	div, exp := unit, 0
	for n := bytesPerSec / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB/s", bytesPerSec/div, "KMGTPE"[exp])
}
