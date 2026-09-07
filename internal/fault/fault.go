// Package fault is the error model for esync (ARCHITECTURE §14).
//
// Every failure that crosses a package boundary is a *Fault carrying a stable
// E<NNNN> Code from the §14.2 catalogue. The Code determines the Class (Fatal,
// Item, Warn), the Retryable flag, and the operator guidance — all read from the
// one policy table in catalogue.go, never decided at the throw site (REQ-ERR-002).
//
// fault must not import obs (obs imports fault); Fields is defined here so obs can
// consume a Fault's trace context without a cycle.
package fault

import (
	"errors"
	"fmt"
	"strconv"
)

// Code is a stable error identifier, e.g. Code("E7003").
type Code string

// Class is the severity of a Code (ARCHITECTURE §14.1).
type Class int

const (
	// Fatal: the session cannot continue correctly. Exit 2 (or 4 for pairing/auth).
	Fatal Class = iota
	// Item: one file, entry, or channel failed. Count it, continue, exit 1 at the end.
	Item
	// Warn: degraded but correct. Log at WARN, count it, does not affect the exit code.
	Warn
)

func (c Class) String() string {
	switch c {
	case Fatal:
		return "fatal"
	case Item:
		return "item"
	case Warn:
		return "warn"
	default:
		return "unknown"
	}
}

// Fields is trace context attached to a Fault (ARCHITECTURE §14.1). obs consumes it.
type Fields map[string]any

// Fault is the single failure type (ARCHITECTURE §14.1).
type Fault struct {
	Code      Code   // stable identifier, e.g. E7003
	Class     Class  // property of the code, from the catalogue
	Retryable bool   // property of the code, from the catalogue
	Op        string // what was attempted: "open source file"
	Subject   string // what it was attempted on: a path, an endpoint, a file id
	Cause     error  // the wrapped underlying error, if any
	Ctx       Fields // trace context at the point of failure

	detail string // optional formatted message from Newf
}

// New builds a Fault, filling Class and Retryable from the catalogue.
func New(code Code, op, subject string, cause error) *Fault {
	p := lookup(code)
	return &Fault{
		Code:      code,
		Class:     p.class,
		Retryable: p.retryable,
		Op:        op,
		Subject:   subject,
		Cause:     cause,
	}
}

// Newf is New with a formatted detail message.
func Newf(code Code, op, subject string, cause error, format string, args ...any) *Fault {
	f := New(code, op, subject, cause)
	f.detail = fmt.Sprintf(format, args...)
	return f
}

// WithCtx merges trace fields into the Fault and returns it for chaining.
func (f *Fault) WithCtx(ctx Fields) *Fault {
	if len(ctx) == 0 {
		return f
	}
	if f.Ctx == nil {
		f.Ctx = make(Fields, len(ctx))
	}
	for k, v := range ctx {
		f.Ctx[k] = v
	}
	return f
}

// Detail returns the formatted message set by Newf, if any.
func (f *Fault) Detail() string { return f.detail }

func (f *Fault) Error() string {
	b := string(f.Code) + ": " + f.Op
	if f.Subject != "" {
		b += " " + strconv.Quote(f.Subject)
	}
	if f.detail != "" {
		b += ": " + f.detail
	}
	if f.Cause != nil {
		b += ": " + f.Cause.Error()
	}
	return b
}

// Unwrap exposes the wrapped cause to errors.Is/As.
func (f *Fault) Unwrap() error { return f.Cause }

// Is matches another *Fault by Code, so errors.Is(err, &Fault{Code: E7004}) works.
func (f *Fault) Is(target error) bool {
	t, ok := target.(*Fault)
	return ok && t.Code == f.Code
}

// Wrap returns err unchanged if it already carries a Fault code; otherwise it
// wraps err in a new Fault with the given code (ARCHITECTURE §14, the wrap helper).
func Wrap(code Code, op, subject string, err error) *Fault {
	var f *Fault
	if errors.As(err, &f) && f.Code != "" {
		return f
	}
	return New(code, op, subject, err)
}

// GetCode returns the Code of the first Fault in err's chain, or "" if none.
func GetCode(err error) Code {
	var f *Fault
	if errors.As(err, &f) {
		return f.Code
	}
	return ""
}

// HasCode reports whether err's chain contains a Fault with the given Code.
func HasCode(err error, c Code) bool {
	for err != nil {
		var f *Fault
		if !errors.As(err, &f) {
			return false
		}
		if f.Code == c {
			return true
		}
		err = f.Cause
	}
	return false
}

// IsRetryable reports whether err carries a Fault whose Code is retryable.
func IsRetryable(err error) bool {
	var f *Fault
	if errors.As(err, &f) {
		return f.Retryable
	}
	return false
}

// IsFatal reports whether err is Fatal-class. A non-Fault error is treated as Fatal.
func IsFatal(err error) bool {
	if err == nil {
		return false
	}
	var f *Fault
	if errors.As(err, &f) {
		return f.Class == Fatal
	}
	return true
}

// ErrSignal is the sentinel for "interrupted by a signal" (§14.6 exit 5).
var ErrSignal error = New(Signal, "interrupted by signal", "", nil)

// ExitCode maps err to a process exit code (ARCHITECTURE §14.6):
//
//	0 clean, 1 item failures/skips, 2 fatal, 3 usage, 4 pairing/auth, 5 signal.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var f *Fault
	if !errors.As(err, &f) {
		return 2
	}
	switch f.Code {
	case Signal:
		return 5
	case E1001, E1002, E1007:
		return 3
	case E2001, E2002, E2003, E2004, E2005, E2006, E2007, E4001, E4002:
		return 4
	}
	switch f.Class {
	case Warn:
		return 0
	case Item:
		return 1
	default:
		return 2
	}
}
