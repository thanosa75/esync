package main

import (
	"flag"
	"io"
	"os"
	"testing"
	"time"

	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/obs"
)

// quietStd redirects stdout+stderr to a drain for the duration of a test so the
// usage/help/version paths don't clutter the test log.
func quietStd(t *testing.T) {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	or, ow, _ := os.Pipe()
	er, ew, _ := os.Pipe()
	os.Stdout, os.Stderr = ow, ew
	go io.Copy(io.Discard, or)
	go io.Copy(io.Discard, er)
	t.Cleanup(func() {
		os.Stdout, os.Stderr = origOut, origErr
		ow.Close()
		ew.Close()
	})
}

func TestHasLinkFlag(t *testing.T) {
	cases := []struct {
		args []string
		want bool
	}{
		{[]string{"/src"}, false},
		{[]string{"--link", "ABC"}, true},
		{[]string{"--link=ABC"}, true},
		{[]string{"-link", "ABC"}, true},
		{[]string{"-v", "--link", "ABC"}, true},
		{[]string{"--", "--link"}, false},
		{nil, false},
	}
	for _, c := range cases {
		if got := hasLinkFlag(c.args); got != c.want {
			t.Errorf("hasLinkFlag(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}

func TestRunModeSelection(t *testing.T) {
	if got := run(nil); got != 3 {
		t.Errorf("run(nil) = %d, want 3 (usage)", got)
	}
	if got := run([]string{"--version"}); got != 0 {
		t.Errorf("run(--version) = %d, want 0", got)
	}
	if got := run([]string{"--help"}); got != 0 {
		t.Errorf("run(--help) = %d, want 0", got)
	}
}

func TestRunSenderUsageErrors(t *testing.T) {
	cases := [][]string{
		{},                             // no path
		{"/a", "/b"},                   // two paths
		{"--hash", "blake3", "/tmp"},   // reserved algo
		{"--hash", "nonsense", "/tmp"}, // unknown algo
		{"--chunk-size", "10", "/tmp"}, // below minimum
		{"--read-concurrency", "0", "/tmp"},
		{"--log-level", "bogus", "/tmp"},
	}
	for _, args := range cases {
		if got := runSender(args); got != 3 {
			t.Errorf("runSender(%q) = %d, want 3", args, got)
		}
	}
}

func TestRunReceiverUsageErrors(t *testing.T) {
	cases := [][]string{
		{"--dest", "/tmp/x"},                                            // missing --link
		{"--link", "  ", "--dest", "/tmp/x"},                            // blank --link
		{"--link", "ABC", "positional"},                                 // positional not allowed
		{"--link", "ABC", "--channels", "0"},                            // below minimum
		{"--link", "ABC", "--max-channels", "1", "--min-channels", "4"}, // max < min
	}
	for _, args := range cases {
		if got := runReceiver(args); got != 3 {
			t.Errorf("runReceiver(%q) = %d, want 3", args, got)
		}
	}
}

func TestResolveLevel(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c := registerCommon(fs)
	if err := fs.Parse([]string{"-vv"}); err != nil {
		t.Fatal(err)
	}
	if lvl, _ := c.resolveLevel(); lvl != obs.LevelTrace {
		t.Errorf("-vv resolved to %v, want TRACE", lvl)
	}

	fs = flag.NewFlagSet("t", flag.ContinueOnError)
	c = registerCommon(fs)
	_ = fs.Parse([]string{"-vv", "--log-level", "warn"})
	if lvl, _ := c.resolveLevel(); lvl != obs.LevelWarn {
		t.Errorf("--log-level should win over -vv, got %v", lvl)
	}
}

func TestApplyEnv(t *testing.T) {
	t.Setenv("ESYNC_LOG_FORMAT", "json")
	t.Setenv("ESYNC_MAX_RETRIES", "7")

	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c := registerCommon(fs)
	if err := fs.Parse([]string{"--max-retries", "2"}); err != nil {
		t.Fatal(err)
	}
	if err := applyEnv(fs); err != nil {
		t.Fatalf("applyEnv: %v", err)
	}
	if *c.logFormat != "json" {
		t.Errorf("env ESYNC_LOG_FORMAT not applied: %q", *c.logFormat)
	}
	if *c.maxRetries != 2 {
		t.Errorf("explicit flag should beat env: got %d, want 2", *c.maxRetries)
	}
}

func TestApplyEnvInvalid(t *testing.T) {
	t.Setenv("ESYNC_LOG_RING", "not-a-number")
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registerCommon(fs)
	_ = fs.Parse(nil)
	if err := applyEnv(fs); err == nil {
		t.Fatal("applyEnv accepted a non-numeric ESYNC_LOG_RING")
	}
}

func TestParseHash(t *testing.T) {
	if a, err := parseHash("sha256"); err != nil || a != digest.SHA256 {
		t.Errorf("parseHash(sha256) = %v, %v", a, err)
	}
	if _, err := parseHash("blake3"); err == nil {
		t.Error("parseHash(blake3) should be rejected")
	}
}

func TestAtLeast(t *testing.T) {
	if err := atLeast("x", 5, 1); err != nil {
		t.Errorf("atLeast(5,1) = %v", err)
	}
	err := atLeast("x", 0, 1)
	if err == nil || !fault.HasCode(err, fault.E1007) {
		t.Errorf("atLeast(0,1) = %v, want E1007", err)
	}
}

func TestOnce(t *testing.T) {
	n := 0
	f := once(func() { n++ })
	f()
	f()
	f()
	if n != 1 {
		t.Errorf("once ran %d times, want 1", n)
	}
}

func testCtx(t *testing.T) obs.Ctx {
	t.Helper()
	ctx, flush := obs.Init(obs.Config{Level: obs.LevelError, Sync: true, Role: "sender"})
	t.Cleanup(flush)
	return ctx
}

// TestSignalEscapeSecondSignal: the first signal is left to Run's own handler; a
// second forces exit 5 (ARCHITECTURE §14.7).
func TestSignalEscapeSecondSignal(t *testing.T) {
	sigc := make(chan os.Signal, 2)
	got := make(chan int, 1)
	signalEscape(testCtx(t), sigc, time.Hour, func(code int) { got <- code })

	sigc <- os.Interrupt
	sigc <- os.Interrupt
	select {
	case code := <-got:
		if code != 5 {
			t.Fatalf("exit code = %d, want 5", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second signal did not force exit")
	}
}

// TestSignalEscapeGraceTimeout: with only one signal, grace elapsing forces exit.
func TestSignalEscapeGraceTimeout(t *testing.T) {
	sigc := make(chan os.Signal, 2)
	got := make(chan int, 1)
	signalEscape(testCtx(t), sigc, 20*time.Millisecond, func(code int) { got <- code })

	sigc <- os.Interrupt
	select {
	case code := <-got:
		if code != 5 {
			t.Fatalf("exit code = %d, want 5", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("grace timeout did not force exit")
	}
}

// TestSignalEscapeNoSignal: absent any signal, the escape hatch never exits.
func TestSignalEscapeNoSignal(t *testing.T) {
	sigc := make(chan os.Signal, 2)
	got := make(chan int, 1)
	signalEscape(testCtx(t), sigc, 10*time.Millisecond, func(code int) { got <- code })

	select {
	case <-got:
		t.Fatal("exit called with no signal delivered")
	case <-time.After(100 * time.Millisecond):
	}
}
