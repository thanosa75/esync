package obs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"esync/internal/fault"
)

// newTestCtx builds a logger writing to buf and a root Ctx, without touching the
// package-global fatal state.
func newTestCtx(cfg Config, buf *bytes.Buffer) (Ctx, *logger) {
	cfg = cfg.withDefaults()
	l := newLogger(cfg, buf)
	ctx := Ctx{lg: l, tf: map[string]any{"session": "8f3a2c1d3b0e7a45"}}
	return ctx, l
}

// T-OBS-06: a JSON record is valid JSON with the fixed field names.
func TestJSONLineValid(t *testing.T) {
	var buf bytes.Buffer
	ctx, _ := newTestCtx(Config{Format: "json", Sync: true, Role: "receiver"}, &buf)
	ctx = With(ctx, F("group", uint32(0)), F("file", uint64(317)))
	Warn(ctx, "path rejected", F("code", "E7010"), F("bytes", uint64(42)), F("rule", "parent-traversal"))

	line := strings.TrimSpace(buf.String())
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, line)
	}
	for _, k := range []string{"ts", "level", "msg", "role", "session", "group", "file", "code", "bytes"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing field %q in %s", k, line)
		}
	}
	if m["level"] != "WARN" || m["role"] != "receiver" || m["msg"] != "path rejected" {
		t.Errorf("bad fixed fields: %v", m)
	}
	if _, err := time.Parse(tsLayout, m["ts"].(string)); err != nil {
		t.Errorf("ts not RFC3339/µs: %v", err)
	}
}

func TestTextFormat(t *testing.T) {
	var buf bytes.Buffer
	ctx, _ := newTestCtx(Config{Format: "text", Sync: true, Role: "sender"}, &buf)
	Info(ctx, "peer linked", F("peer", "192.168.1.31:52118"))
	out := buf.String()
	if !strings.Contains(out, "INFO  sender") ||
		!strings.Contains(out, "session=8f3a2c1d3b0e7a45") ||
		!strings.Contains(out, "peer linked") ||
		!strings.Contains(out, "peer=192.168.1.31:52118") {
		t.Errorf("text line malformed: %q", out)
	}
}

// T-OBS-04 / I-SEC-01: secrets never reach any log path, in any format, at any level.
func TestRedaction(t *testing.T) {
	const raw = "PLAINTEXT_SECRET_9c3f_do_not_leak"
	sec := NewSecret([]byte(raw))
	code := RedactedString("ABCD-EF12-" + raw)

	// Direct formatting with every verb.
	for _, verb := range []string{"%v", "%s", "%+v", "%#v", "%d", "%q"} {
		for _, val := range []any{sec, code} {
			got := fmt.Sprintf(verb, val)
			if strings.Contains(got, raw) {
				t.Errorf("verb %s leaked raw secret: %q", verb, got)
			}
			if !strings.Contains(got, redacted) {
				t.Errorf("verb %s did not render [redacted]: %q", verb, got)
			}
		}
	}
	if string(sec.Bytes()) != raw || code.Reveal() != "ABCD-EF12-"+raw {
		t.Fatal("accessor did not return the plaintext")
	}
	// json.Marshal must not leak either.
	for _, val := range []any{sec, code, map[string]any{"secret": sec, "code": code}} {
		b, _ := json.Marshal(val)
		if bytes.Contains(b, []byte(raw)) {
			t.Errorf("json.Marshal leaked: %s", b)
		}
	}

	// Through the logger, both formats, every level.
	for _, format := range []string{"text", "json"} {
		var buf bytes.Buffer
		ctx, _ := newTestCtx(Config{Format: format, Level: LevelTrace, Sync: true, Role: "sender"}, &buf)
		ctx = With(ctx, F("session", code)) // even a trace field
		emitters := []func(Ctx, string, ...Field){Error, Warn, Info, Debug, Trace}
		for _, e := range emitters {
			e(ctx, "handshake", F("secret", sec), F("code2", code))
		}
		DumpRing(ctx)
		if strings.Contains(buf.String(), raw) {
			t.Errorf("format %s: logger leaked the raw secret\n%s", format, buf.String())
		}
		if !strings.Contains(buf.String(), redacted) {
			t.Errorf("format %s: expected [redacted] in output", format)
		}
	}
}

// Ring buffer keeps N and DumpRing emits them, even when the output level would
// otherwise suppress them.
func TestRingBuffer(t *testing.T) {
	var buf bytes.Buffer
	ctx, l := newTestCtx(Config{Format: "text", Level: LevelError, RingSize: 4, Sync: true, Role: "receiver"}, &buf)

	for i := 0; i < 10; i++ {
		Trace(ctx, fmt.Sprintf("msg%d", i))
	}
	if buf.Len() != 0 {
		t.Fatalf("TRACE records were emitted at ERROR level: %q", buf.String())
	}
	if snap := l.ring.snapshot(); len(snap) != 4 {
		t.Fatalf("ring kept %d records, want 4", len(snap))
	}
	DumpRing(ctx)
	out := buf.String()
	if !strings.Contains(out, "trace ring dump records=4") {
		t.Errorf("no dump header: %q", out)
	}
	for _, want := range []string{"msg6", "msg7", "msg8", "msg9"} {
		if !strings.Contains(out, want) {
			t.Errorf("dump missing %s", want)
		}
	}
	for _, gone := range []string{"msg0", "msg5"} {
		if strings.Contains(out, gone) {
			t.Errorf("dump kept evicted record %s", gone)
		}
	}
}

// T-OBS-08: under flood the sink drops records, counts them, and never blocks.
func TestSinkFloodNeverBlocks(t *testing.T) {
	var buf bytes.Buffer
	release := make(chan struct{})
	bw := &blockingWriter{buf: &buf, release: release}
	ctx, l := newTestCtx(Config{Format: "text", Level: LevelTrace, Role: "sender"}, &buf)
	l.w = bw

	var drainDone sync.WaitGroup
	drainDone.Add(1)
	go func() { defer drainDone.Done(); l.drain() }()

	const n = 20000
	start := time.Now()
	for i := 0; i < n; i++ {
		Trace(ctx, "flood", F("count", uint64(i)))
	}
	elapsed := time.Since(start)
	// The writer is blocked for the whole loop; a blocking sink would never
	// return. Formatting cost is on the caller (§15.8), so allow generous slack.
	if elapsed > 8*time.Second {
		t.Fatalf("logging blocked on a stalled sink: %v for %d records", elapsed, n)
	}
	if l.drops.Load() == 0 {
		t.Fatalf("expected dropped records under flood, got 0")
	}
	if l.drops.Load() > n {
		t.Fatalf("drop count %d exceeds records emitted %d", l.drops.Load(), n)
	}
	close(release)
	l.close()
	drainDone.Wait()
}

type blockingWriter struct {
	buf     *bytes.Buffer
	mu      sync.Mutex
	release chan struct{}
	once    sync.Once
	blocked atomic.Bool
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		w.blocked.Store(true)
		<-w.release
		w.blocked.Store(false)
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// T-ERR-04: obs.Go recovers a panic into E9001 and calls OnFatal.
func TestGoPanicToFatal(t *testing.T) {
	root, flush := Init(Config{Level: LevelInfo, Format: "text", Sync: true, Role: "test"})
	defer flush()

	got := make(chan error, 1)
	OnFatal(func(e error) { got <- e })
	defer OnFatal(nil)

	Go(root, "boom", func() error { panic("kaboom") })

	select {
	case e := <-got:
		var f *fault.Fault
		if !errors.As(e, &f) {
			t.Fatalf("fatal handler got a non-Fault error: %v", e)
		}
		if f.Code != fault.E9001 {
			t.Fatalf("panic mapped to %s, want E9001", f.Code)
		}
		if f.Ctx["stack"] == nil {
			t.Error("E9001 fault carries no stack trace")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal not called after a panic")
	}
}

func TestGoFatalErrorRouted(t *testing.T) {
	root, flush := Init(Config{Level: LevelInfo, Format: "text", Sync: true, Role: "test"})
	defer flush()

	got := make(chan error, 1)
	OnFatal(func(e error) { got <- e })
	defer OnFatal(nil)

	Go(root, "fatal-return", func() error { return fault.New(fault.E4003, "verify mac", "chan=3", nil) })

	select {
	case e := <-got:
		if fault.GetCode(e) != fault.E4003 {
			t.Fatalf("routed %v, want E4003", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Fatal-class error from obs.Go was not routed to OnFatal")
	}
}

func TestSpan(t *testing.T) {
	var buf bytes.Buffer
	ctx, _ := newTestCtx(Config{Format: "json", Level: LevelInfo, Sync: true, Role: "sender"}, &buf)
	end := Start(ctx, "scan")
	time.Sleep(2 * time.Millisecond)
	end("ok", F("files", uint64(48219)))

	var m map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &m); err != nil {
		t.Fatalf("span record not JSON: %v", err)
	}
	if m["span"] != "scan" || m["outcome"] != "ok" {
		t.Errorf("span fields wrong: %v", m)
	}
	if d, ok := m["dur_ms"].(float64); !ok || d <= 0 {
		t.Errorf("dur_ms missing or non-positive: %v", m["dur_ms"])
	}
}

func TestCounters(t *testing.T) {
	ctx, _ := newTestCtx(Config{Sync: true}, &bytes.Buffer{})
	c := ctx.Counters()
	c.FilesTransferred.Add(3)
	c.Bytes.Add(1024)
	if ctx.Counters().FilesTransferred.Load() != 3 || ctx.Counters().Bytes.Load() != 1024 {
		t.Error("counters not shared through Ctx")
	}
}

func TestLevelParse(t *testing.T) {
	for name, want := range map[string]Level{
		"error": LevelError, "WARN": LevelWarn, "info": LevelInfo,
		"Debug": LevelDebug, "trace": LevelTrace,
	} {
		got, err := ParseLevel(name)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v", name, got, err)
		}
	}
	if _, err := ParseLevel("chatty"); !fault.HasCode(err, fault.E1007) {
		t.Errorf("bad level should be E1007, got %v", err)
	}
}

func TestSummaryAlwaysEmitted(t *testing.T) {
	var buf bytes.Buffer
	ctx, l := newTestCtx(Config{Format: "text", Level: LevelError, Sync: true, Role: "receiver"}, &buf)
	l.counters.FilesTransferred.Store(31004)
	l.counters.FilesFailed.Store(4)
	Summary(ctx, "partial", 1, 14*time.Minute+22*time.Second, "file=9921 digest mismatch")
	out := buf.String()
	// Emitted despite the ERROR output level.
	if !strings.Contains(out, "esync summary") || !strings.Contains(out, "outcome=partial") ||
		!strings.Contains(out, "31004 files") || !strings.Contains(out, "file=9921 digest mismatch") {
		t.Errorf("summary block malformed:\n%s", out)
	}
}

func TestProgressNoDeadlock(t *testing.T) {
	root, flush := Init(Config{Level: LevelInfo, Sync: true, Role: "receiver"})
	defer flush()
	p := NewProgress(root, 5*time.Millisecond)
	for i := 0; i < 5; i++ {
		p.Update(int64(i)*1024, 5120, int64(i), 5)
		time.Sleep(3 * time.Millisecond)
	}
	p.Stop()
	p.Stop() // idempotent
}
