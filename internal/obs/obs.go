// Package obs is esync's observability layer (ARCHITECTURE §15, §14.5):
// a leveled structured logger with trace context, spans, a trace ring buffer, a
// non-blocking sink, live counters, the final summary, secret redaction, the
// progress renderer, and obs.Go — the only sanctioned goroutine launcher.
//
// A Ctx carries the trace fields (session, group, file, req, chan, span) so any
// log call in any package emits them without the call site restating them.
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sinkDepth = 8192
	tsLayout  = "2006-01-02T15:04:05.000000Z07:00"
)

// traceOrder is the order trace fields are emitted in (ARCHITECTURE §15.1).
var traceOrder = []string{"session", "group", "file", "req", "chan", "span"}

// fieldOrder is the leading render order for known fields (ARCHITECTURE §15.3).
var fieldOrder = []string{
	"session", "group", "file", "path", "req", "chan", "span",
	"dur_ms", "code", "bytes", "files", "count", "rate_mbps", "attempt", "peer", "err",
}

// Field is one structured key/value pair on a log record.
type Field struct {
	Key string
	Val any
}

// F builds a Field.
func F(key string, val any) Field { return Field{Key: key, Val: val} }

type record struct {
	ts     time.Time
	level  Level
	role   string
	msg    string
	fields []Field
}

// Config configures the logger (ARCHITECTURE §15, §16.1).
type Config struct {
	Level            Level
	Format           string // "text" (default) or "json"
	File             string // optional --log-file; full fidelity, independent level
	RingSize         int    // --log-ring, default 2048
	RedactPaths      bool   // --redact-paths
	Sync             bool   // --log-sync: format and write on the caller goroutine
	ProgressInterval time.Duration
	Role             string // "sender" | "receiver"
}

func (c Config) withDefaults() Config {
	if c.Level == 0 {
		c.Level = LevelInfo
	}
	if c.Format == "" {
		c.Format = "text"
	}
	if c.RingSize <= 0 {
		c.RingSize = 2048
	}
	if c.ProgressInterval <= 0 {
		c.ProgressInterval = 5 * time.Second
	}
	return c
}

type logger struct {
	level       Level
	format      string
	role        string
	redactPaths bool
	sync        bool

	w   io.Writer
	wmu sync.Mutex

	ch      chan []byte
	drained chan struct{}
	drops   atomic.Int64

	ring     *ring
	counters Counters
}

func newLogger(cfg Config, w io.Writer) *logger {
	return &logger{
		level:       cfg.Level,
		format:      cfg.Format,
		role:        cfg.Role,
		redactPaths: cfg.RedactPaths,
		sync:        cfg.Sync,
		w:           w,
		ch:          make(chan []byte, sinkDepth),
		drained:     make(chan struct{}),
		ring:        newRing(cfg.RingSize),
	}
}

// Init builds the logger and returns the root Ctx and a flush/close function to
// be deferred by main. The session id on the root Ctx is a placeholder until the
// handshake sets the real one via obs.With(ctx, obs.F("session", id)).
func Init(cfg Config) (Ctx, func()) {
	cfg = cfg.withDefaults()

	var w io.Writer = os.Stderr
	var closers []io.Closer
	if cfg.File != "" {
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "esync: cannot open --log-file %s: %v\n", cfg.File, err)
		} else {
			w = io.MultiWriter(os.Stderr, f)
			closers = append(closers, f)
		}
	}

	l := newLogger(cfg, w)
	root := Ctx{Context: context.Background(), lg: l, tf: map[string]any{"session": newSessionID()}}
	setPkgRoot(root)

	if !l.sync {
		Go(root, "log-writer", func() error { l.drain(); return nil })
	}

	flush := func() {
		l.close()
		for _, c := range closers {
			_ = c.Close()
		}
	}
	return root, flush
}

func newSessionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}

func (l *logger) enabled(lvl Level) bool { return lvl <= l.level }

func (l *logger) log(ctx Ctx, lvl Level, msg string, user []Field) {
	rec := record{ts: time.Now().UTC(), level: lvl, role: l.role, msg: msg}
	tf := ctx.canonFields()
	rec.fields = make([]Field, 0, len(tf)+len(user))
	rec.fields = append(rec.fields, tf...)
	rec.fields = append(rec.fields, user...)

	l.ring.add(rec) // the ring always retains, regardless of level (§15.6)

	if l.enabled(lvl) {
		l.emit(l.formatRecord(rec))
	}
}

// emit hands a formatted record to the sink. Non-blocking: a full channel drops
// the record and bumps the drop counter (ARCHITECTURE §15.8).
func (l *logger) emit(b []byte) {
	if l.sync {
		l.wmu.Lock()
		_, _ = l.w.Write(b)
		l.wmu.Unlock()
		return
	}
	select {
	case l.ch <- b:
	default:
		l.drops.Add(1)
	}
}

func (l *logger) drain() {
	for b := range l.ch {
		l.wmu.Lock()
		_, _ = l.w.Write(b)
		l.wmu.Unlock()
	}
	close(l.drained)
}

func (l *logger) close() {
	if l.sync {
		return
	}
	close(l.ch)
	<-l.drained
}

func (l *logger) formatRecord(rec record) []byte {
	idx := make(map[string]int, len(rec.fields))
	keys := make([]string, 0, len(rec.fields))
	vals := make([]any, 0, len(rec.fields))
	for _, f := range rec.fields {
		if i, ok := idx[f.Key]; ok {
			vals[i] = f.Val
			continue
		}
		idx[f.Key] = len(keys)
		keys = append(keys, f.Key)
		vals = append(vals, f.Val)
	}
	if l.redactPaths {
		if i, ok := idx["path"]; ok {
			vals[i] = redacted
		}
	}
	ts := rec.ts.Format(tsLayout)

	if l.format == "json" {
		m := make(map[string]any, len(keys)+4)
		m["ts"] = ts
		m["level"] = rec.level.String()
		m["msg"] = rec.msg
		m["role"] = rec.role
		for i, k := range keys {
			m[k] = vals[i]
		}
		b, err := json.Marshal(m)
		if err != nil {
			b = []byte(fmt.Sprintf(`{"ts":%q,"level":"ERROR","msg":"log marshal failed"}`, ts))
		}
		return append(b, '\n')
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %-5s %-9s ", ts, rec.level.String(), rec.role)
	written := make(map[string]bool, len(keys))
	for _, k := range fieldOrder {
		if i, ok := idx[k]; ok {
			fmt.Fprintf(&sb, "%s=%v ", k, vals[i])
			written[k] = true
		}
	}
	sb.WriteString(rec.msg)
	for i, k := range keys {
		if written[k] {
			continue
		}
		fmt.Fprintf(&sb, " %s=%v", k, vals[i])
	}
	sb.WriteByte('\n')
	return []byte(sb.String())
}
