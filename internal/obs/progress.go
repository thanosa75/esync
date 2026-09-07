package obs

import (
	"fmt"
	"io"
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
	w    io.Writer

	// Rate/ETA estimate state, touched only by the render goroutine.
	lastAt    time.Time
	lastBytes int64
	rate      float64 // bytes/sec, exponentially smoothed
}

// NewProgress starts the renderer. interval <= 0 uses 5s.
func NewProgress(ctx Ctx, interval time.Duration) *Progress {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	p := newProgress(os.Stderr, isTTY(os.Stderr), interval)
	Go(ctx, "progress", func() error { p.loop(); return nil })
	return p
}

func newProgress(w io.Writer, tty bool, interval time.Duration) *Progress {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Progress{
		interval: interval,
		tty:      tty,
		stop:     make(chan struct{}),
		w:        w,
	}
}

// Update sets the current byte and file counts. A totalBytes or totalFiles of
// zero is rendered as "not known yet" — only the done figure is shown.
func (p *Progress) Update(doneBytes, totalBytes, doneFiles, totalFiles int64) {
	p.doneBytes.Store(doneBytes)
	p.totalBytes.Store(totalBytes)
	p.doneFiles.Store(doneFiles)
	p.totalFiles.Store(totalFiles)
}

// Feed drives Update from sample on the render interval until Stop. It is the
// convenience for a caller that already holds live counters; do not also call
// Update on the same Progress.
func (p *Progress) Feed(ctx Ctx, sample func() (doneBytes, totalBytes, doneFiles, totalFiles int64)) {
	Go(ctx, "progress.feed", func() error {
		t := time.NewTicker(p.interval)
		defer t.Stop()
		p.Update(sample())
		for {
			select {
			case <-p.stop:
				return nil
			case <-t.C:
				p.Update(sample())
			}
		}
	})
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

	now := time.Now()
	if !p.lastAt.IsZero() {
		if dt := now.Sub(p.lastAt).Seconds(); dt > 0 {
			inst := float64(db-p.lastBytes) / dt
			if inst < 0 {
				inst = 0
			}
			if p.rate == 0 {
				p.rate = inst
			} else {
				p.rate = 0.6*p.rate + 0.4*inst
			}
		}
	}
	p.lastAt, p.lastBytes = now, db

	bytesStr := humanBytes(db)
	if tb > 0 {
		bytesStr += " / " + humanBytes(tb)
	}
	filesStr := fmt.Sprintf("%d", df)
	if tf > 0 {
		filesStr += fmt.Sprintf(" / %d", tf)
	}
	line := fmt.Sprintf("%s   %s files   %s", bytesStr, filesStr, humanRate(p.rate))
	if tb > db && p.rate > 0 {
		line += "   eta " + formatETA(time.Duration(float64(tb-db)/p.rate*float64(time.Second)))
	}
	if p.tty {
		fmt.Fprintf(p.w, "\r\x1b[K  %s", line)
	} else {
		fmt.Fprintf(p.w, "%s progress  %s\n", time.Now().UTC().Format(tsLayout), line)
	}
}

// formatETA renders a remaining-time estimate as H:MM:SS (hours omitted below 1h).
func formatETA(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int(d.Seconds())
	h, m, sec := s/3600, (s%3600)/60, s%60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, sec)
	}
	return fmt.Sprintf("%02d:%02d", m, sec)
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
