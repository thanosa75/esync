package obs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"esync/internal/fault"
)

// Ctx carries a context.Context plus the trace fields (session, group, file, req,
// chan, span) that every log call auto-emits (ARCHITECTURE §15.1). Derive a child
// with With; swap the underlying context with WithContext. The zero Ctx is a
// no-op sink.
type Ctx struct {
	context.Context
	lg *logger
	tf map[string]any
}

// With derives a child Ctx with the given trace fields set or overridden.
func With(ctx Ctx, fields ...Field) Ctx {
	tf := make(map[string]any, len(ctx.tf)+len(fields))
	maps.Copy(tf, ctx.tf)
	for _, f := range fields {
		tf[f.Key] = f.Val
	}
	ctx.tf = tf
	return ctx
}

// WithContext returns a Ctx with the same trace fields but a new underlying
// context.Context (for cancellation trees).
func WithContext(ctx Ctx, std context.Context) Ctx {
	ctx.Context = std
	return ctx
}

// Counters returns the session counters (ARCHITECTURE §15.7).
func (c Ctx) Counters() *Counters {
	if c.lg == nil {
		return &Counters{}
	}
	return &c.lg.counters
}

// Session returns the trace session id, or "" if unset.
func (c Ctx) Session() string {
	s, _ := c.tf["session"].(string)
	return s
}

func (c Ctx) canonFields() []Field {
	if len(c.tf) == 0 {
		return nil
	}
	out := make([]Field, 0, len(c.tf))
	for _, k := range traceOrder {
		if v, ok := c.tf[k]; ok {
			out = append(out, F(k, v))
		}
	}
	return out
}

func logAt(ctx Ctx, lvl Level, msg string, fields []Field) {
	if ctx.lg == nil {
		return
	}
	ctx.lg.log(ctx, lvl, msg, fields)
}

// Error logs at ERROR. Every ERROR record must carry a code and state the action.
func Error(ctx Ctx, msg string, fields ...Field) { logAt(ctx, LevelError, msg, fields) }

// Warn logs at WARN: correct but degraded.
func Warn(ctx Ctx, msg string, fields ...Field) { logAt(ctx, LevelWarn, msg, fields) }

// Info logs at INFO: the story of the session at a glance.
func Info(ctx Ctx, msg string, fields ...Field) { logAt(ctx, LevelInfo, msg, fields) }

// Debug logs at DEBUG: decisions and their inputs.
func Debug(ctx Ctx, msg string, fields ...Field) { logAt(ctx, LevelDebug, msg, fields) }

// Trace logs at TRACE: every protocol message, transition, and queue movement.
func Trace(ctx Ctx, msg string, fields ...Field) { logAt(ctx, LevelTrace, msg, fields) }

// LogFault logs err at the level implied by its Fault class, emitting the code,
// op, subject, cause, and the fault's own trace context (fault.Fields).
func LogFault(ctx Ctx, err error) {
	if ctx.lg == nil || err == nil {
		return
	}
	var f *fault.Fault
	if !errors.As(err, &f) {
		Error(ctx, "error", F("err", err.Error()))
		return
	}
	fields := []Field{F("code", string(f.Code))}
	if f.Subject != "" {
		fields = append(fields, F("subject", f.Subject))
	}
	if f.Detail() != "" {
		fields = append(fields, F("detail", f.Detail()))
	}
	if f.Cause != nil {
		fields = append(fields, F("err", f.Cause.Error()))
	}
	fields = append(fields, F("action", f.Code.Action()))
	for k, v := range f.Ctx {
		fields = append(fields, F(k, v))
	}
	msg := f.Op
	if msg == "" {
		msg = f.Code.Condition()
	}
	if f.Class == fault.Warn {
		Warn(ctx, msg, fields...)
	} else {
		Error(ctx, msg, fields...)
	}
}

// SpanEnd closes a span, logging its duration and outcome.
type SpanEnd func(outcome string, fields ...Field)

var spanLevel = map[string]Level{
	"session":       LevelInfo,
	"pair":          LevelInfo,
	"handshake":     LevelDebug,
	"scan":          LevelInfo,
	"sort":          LevelDebug,
	"group.hash":    LevelDebug,
	"group.publish": LevelTrace,
	"group.decide":  LevelDebug,
	"file.transfer": LevelTrace,
	"file.verify":   LevelTrace,
	"file.publish":  LevelTrace,
	"drain":         LevelDebug,
}

// Start opens a span (ARCHITECTURE §15.5). The returned SpanEnd logs at the
// span's configured level with dur_ms and the outcome.
func Start(ctx Ctx, name string) SpanEnd {
	start := time.Now()
	lvl, ok := spanLevel[name]
	if !ok {
		lvl = LevelDebug
	}
	return func(outcome string, fields ...Field) {
		dur := time.Since(start)
		all := make([]Field, 0, len(fields)+3)
		all = append(all,
			F("span", name),
			F("outcome", outcome),
			F("dur_ms", float64(dur.Microseconds())/1000.0),
		)
		all = append(all, fields...)
		logAt(ctx, lvl, name+" span", all)
	}
}

// DumpRing writes the trace ring buffer to the sink at ERROR (ARCHITECTURE §15.6).
// Wired to run automatically on a Fatal fault via the fatal handler.
func DumpRing(ctx Ctx) {
	l := ctx.lg
	if l == nil {
		return
	}
	recs := l.ring.snapshot()
	header := fmt.Sprintf("%s %-5s %-9s trace ring dump records=%d\n",
		time.Now().UTC().Format(tsLayout), "ERROR", l.role, len(recs))
	l.emit([]byte(header))
	for _, r := range recs {
		l.emit(l.formatRecord(r))
	}
}
