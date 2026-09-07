package fault

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// T-ERR-02: every code has a class + retryable + human strings.
func TestCatalogueRoundTrip(t *testing.T) {
	want := []Code{
		E1001, E1002, E1003, E1004, E1005, E1006, E1007, E1008,
		E2001, E2002, E2003, E2004, E2005, E2006, E2007,
		E3001, E3002, E3003, E3004, E3005, E3006, E3007,
		E4001, E4002, E4003, E4004, E4005, E4006, E4007,
		E5001, E5002, E5003, E5004, E5005, E5006, E5007, E5008, E5009, E5010, E5011,
		E6001, E6002, E6003, E6004, E6005, E6006, E6007, E6008,
		E7001, E7002, E7003, E7004, E7005, E7006, E7007, E7008, E7009, E7010, E7011, E7012, E7013, E7014,
		E8001, E8002, E8003, E8004, E8005,
		E9001, E9002, E9003, E9004,
		Signal,
	}
	for _, c := range want {
		p, ok := catalogue[c]
		if !ok {
			t.Errorf("code %s missing from catalogue", c)
			continue
		}
		switch p.class {
		case Fatal, Item, Warn:
		default:
			t.Errorf("code %s has invalid class %d", c, p.class)
		}
		if strings.TrimSpace(p.condition) == "" {
			t.Errorf("code %s has no condition", c)
		}
		if strings.TrimSpace(p.action) == "" {
			t.Errorf("code %s has no operator action", c)
		}
	}
	if len(catalogue) != len(want) {
		t.Errorf("catalogue has %d entries, expected %d", len(catalogue), len(want))
	}
	// Constructors must reflect the table.
	f := New(E7004, "write", "x", nil)
	if f.Class != Item || !f.Retryable {
		t.Errorf("E7004: class=%v retryable=%v, want item/true", f.Class, f.Retryable)
	}
	f = New(E6005, "scan", "d", nil)
	if f.Class != Warn || f.Retryable {
		t.Errorf("E6005: class=%v retryable=%v, want warn/false", f.Class, f.Retryable)
	}
}

// T-ERR-05: exit-code mapping (§14.6).
func TestExitCode(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nil, 0},
		{New(E1001, "", "", nil), 3},
		{New(E1002, "", "", nil), 3},
		{New(E1007, "", "", nil), 3},
		{New(E2001, "", "", nil), 4},
		{New(E2007, "", "", nil), 4},
		{New(E4001, "", "", nil), 4},
		{New(E4002, "", "", nil), 4},
		{New(E4003, "", "", nil), 2},
		{New(E7003, "", "", nil), 2},
		{New(E6001, "", "", nil), 1},
		{New(E6005, "", "", nil), 0},
		{New(E8005, "", "", nil), 0},
		{ErrSignal, 5},
		{errors.New("bare"), 2},
		{fmt.Errorf("wrap: %w", New(E6001, "", "", nil)), 1},
	}
	for _, tc := range cases {
		if got := ExitCode(tc.err); got != tc.want {
			t.Errorf("ExitCode(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// T-ERR-01: backoff stays in [100ms, 7.5s] and grows with the attempt.
func TestBackoff(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	const lo = 100 * time.Millisecond
	const hi = time.Duration(float64(backoffCap) * 1.5)

	var means [5]time.Duration
	for attempt := 0; attempt < 5; attempt++ {
		var sum time.Duration
		const n = 2000
		for i := 0; i < n; i++ {
			d := Backoff(attempt, rnd)
			if d < lo || d > hi {
				t.Fatalf("attempt %d: Backoff = %v, outside [%v, %v]", attempt, d, lo, hi)
			}
			sum += d
		}
		means[attempt] = sum / n
	}
	for i := 1; i < 4; i++ { // grows until the cap bites
		if means[i] <= means[i-1] {
			t.Errorf("mean backoff did not grow: attempt %d %v <= attempt %d %v", i, means[i], i-1, means[i-1])
		}
	}
	// Large attempts are clamped, never negative or absurd.
	if d := Backoff(1000, rnd); d < lo || d > hi {
		t.Errorf("Backoff(1000) = %v, outside bounds", d)
	}
	if d := Backoff(5, nil); d != backoffCap {
		t.Errorf("Backoff(5, nil) = %v, want %v (un-jittered cap)", d, backoffCap)
	}
}

func TestIsAsWrap(t *testing.T) {
	base := errors.New("disk gone")
	f := Newf(E7004, "write chunk", "/dst/a", base, "short write %d/%d", 3, 10)

	if !errors.Is(f, &Fault{Code: E7004}) {
		t.Error("errors.Is by code failed")
	}
	if errors.Is(f, &Fault{Code: E7005}) {
		t.Error("errors.Is matched the wrong code")
	}
	var got *Fault
	if !errors.As(f, &got) || got.Code != E7004 {
		t.Error("errors.As failed")
	}
	if !errors.Is(f, base) {
		t.Error("Unwrap chain broken")
	}
	if !strings.Contains(f.Error(), "short write 3/10") || f.Detail() != "short write 3/10" {
		t.Errorf("Newf detail wrong: Error()=%q Detail()=%q", f.Error(), f.Detail())
	}
	if !HasCode(f, E7004) || GetCode(f) != E7004 {
		t.Error("HasCode/GetCode failed")
	}

	// Wrap is idempotent over an existing coded fault.
	w := Wrap(E9003, "op", "subj", f)
	if w.Code != E7004 {
		t.Errorf("Wrap re-coded an existing fault: got %s", w.Code)
	}
	w = Wrap(E6004, "read", "/src/a", base)
	if w.Code != E6004 || !w.Retryable {
		t.Errorf("Wrap of a bare error: code=%s retryable=%v", w.Code, w.Retryable)
	}
}

func TestRetryClassHelpers(t *testing.T) {
	if !IsRetryable(New(E7004, "", "", nil)) {
		t.Error("E7004 should be retryable")
	}
	if IsRetryable(New(E7002, "", "", nil)) {
		t.Error("E7002 should not be retryable")
	}
	if IsRetryable(errors.New("bare")) {
		t.Error("bare error is not retryable")
	}
	if !IsFatal(New(E4003, "", "", nil)) {
		t.Error("E4003 is fatal")
	}
	if IsFatal(New(E6001, "", "", nil)) {
		t.Error("E6001 is item-class, not fatal")
	}
	if !IsFatal(errors.New("bare")) {
		t.Error("bare error is treated as fatal")
	}
}

func TestWithCtx(t *testing.T) {
	f := New(E7010, "validate path", "a/../b", nil).WithCtx(Fields{"file": uint64(317), "rule": "parent-traversal"})
	if f.Ctx["file"] != uint64(317) || f.Ctx["rule"] != "parent-traversal" {
		t.Errorf("WithCtx did not attach fields: %v", f.Ctx)
	}
	if New(E7010, "", "", nil).WithCtx(nil).Ctx != nil {
		t.Error("WithCtx(nil) should be a no-op")
	}
	// second merge onto an existing Ctx map
	f.WithCtx(Fields{"attempt": 2})
	if f.Ctx["attempt"] != 2 || f.Ctx["file"] != uint64(317) {
		t.Errorf("second WithCtx merge lost fields: %v", f.Ctx)
	}
}

func TestCodeAndClassStrings(t *testing.T) {
	if E7010.Condition() == "" || E7010.Action() == "" {
		t.Error("E7010 has empty condition/action")
	}
	if Code("E0000").Condition() == "" || Code("E0000").Action() == "" {
		t.Error("unknown code should still yield fallback strings")
	}
	for c, want := range map[Class]string{Fatal: "fatal", Item: "item", Warn: "warn", Class(99): "unknown"} {
		if c.String() != want {
			t.Errorf("Class(%d).String() = %q, want %q", c, c.String(), want)
		}
	}
	if GetCode(errors.New("bare")) != "" {
		t.Error("GetCode of a bare error should be empty")
	}
	if HasCode(errors.New("bare"), E7010) {
		t.Error("HasCode of a bare error should be false")
	}
	nested := fmt.Errorf("outer: %w", Newf(E7005, "publish", "x", New(E7004, "write", "y", nil), "rename failed"))
	if !HasCode(nested, E7005) {
		t.Error("HasCode should find E7005 in the chain")
	}
	if IsFatal(nil) {
		t.Error("IsFatal(nil) should be false")
	}
}
