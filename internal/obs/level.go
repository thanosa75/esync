package obs

import (
	"strings"

	"esync/internal/fault"
)

// Level is a log severity. Lower value = more severe. The zero value is "unset"
// and Init resolves it to LevelInfo.
type Level int

// Level contracts (ARCHITECTURE §15.2). A level is chosen by matching against
// these, not by feel; code review checks the match.
const (
	// LevelError: the session, or an item within it, has definitively failed.
	// Every record has a code and states the operator action. Zero on a clean run.
	LevelError Level = iota + 1
	// LevelWarn: correct but degraded, or a fact the user would regret not
	// knowing: skipped entries, unsupported types, metadata not applied, hard
	// link fallback, low disk space. Zero on a clean homogeneous run.
	LevelWarn
	// LevelInfo: the story of the session at a glance: version, session id,
	// endpoints, pairing, peer identity, totals, coarse group progress, the final
	// summary. Budget: <= 30 records for a typical session, independent of tree size.
	LevelInfo
	// LevelDebug: decisions and their inputs: per-group decision counts, channel
	// ramp decisions, retries with cause, cache hit/miss rates, per-stage timings,
	// path rejections with the failing rule. Volume O(groups)+O(channels).
	LevelDebug
	// LevelTrace: every protocol message (type, size, correlation ids — never
	// payload bytes), every state transition, every file start/end, every queue
	// push/pop, every semaphore acquisition. Volume O(files)+O(records).
	LevelTrace
)

func (l Level) String() string {
	switch l {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	case LevelTrace:
		return "TRACE"
	default:
		return "INFO"
	}
}

// ParseLevel maps a level name (case-insensitive) to a Level. An unknown name is
// an E1007 fault (flag value outside its documented range).
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "error":
		return LevelError, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "info":
		return LevelInfo, nil
	case "debug":
		return LevelDebug, nil
	case "trace":
		return LevelTrace, nil
	default:
		return LevelInfo, fault.Newf(fault.E1007, "parse log level", s, nil,
			"unknown level %q; want one of error, warn, info, debug, trace", s)
	}
}
