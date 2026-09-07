package receiver

import (
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/fault"
	"esync/internal/paircode"
	"esync/internal/wire"
)

func ep(s string) paircode.Endpoint {
	ap := netip.MustParseAddrPort(s)
	return paircode.Endpoint{Addr: ap.Addr(), Port: ap.Port()}
}

func TestOrderCandidatesPrefersLAN(t *testing.T) {
	in := []paircode.Endpoint{
		ep("8.8.8.8:1"), ep("192.168.1.5:1"), ep("127.0.0.1:1"), ep("[2001:db8::1]:1"),
	}
	out := orderCandidates(in)
	if !out[0].Addr.IsLoopback() {
		t.Fatalf("first = %v, want loopback", out[0].Addr)
	}
	if out[1].Addr != in[1].Addr {
		t.Fatalf("second = %v, want private v4", out[1].Addr)
	}
	if !out[len(out)-1].Addr.Is6() {
		t.Fatalf("last = %v, want the public v6", out[len(out)-1].Addr)
	}
}

func TestAuthCodeFromTable(t *testing.T) {
	tab := []channel.EndpointErr{
		{Err: errors.New("connection refused")},
		{Err: fault.New(fault.E4001, "x", "", nil)},
	}
	if got := authCodeFromTable(tab); got != fault.E4001 {
		t.Fatalf("got %v, want E4001", got)
	}
	if got := authCodeFromTable([]channel.EndpointErr{{Err: errors.New("nope")}}); got != "" {
		t.Fatalf("got %v, want empty", got)
	}
}

func TestPeerErrorAndPad4(t *testing.T) {
	if pad4(7) != "0007" || pad4(1234) != "1234" {
		t.Fatalf("pad4 wrong: %q %q", pad4(7), pad4(1234))
	}
	err := peerError(&wire.Error{Code: 5001, Message: "boom"})
	if fault.GetCode(err) != fault.E5001 {
		t.Fatalf("peerError code = %v, want E5001", fault.GetCode(err))
	}
}

func TestTransportData(t *testing.T) {
	if transportData(nil) != nil {
		t.Fatal("nil should pass through")
	}
	coded := fault.New(fault.E3004, "x", "", nil)
	if transportData(coded) != coded {
		t.Fatal("coded fault should pass through unchanged")
	}
	if fault.GetCode(transportData(io.EOF)) != fault.E3005 {
		t.Fatal("EOF should map to E3005")
	}
}

func TestTransportControl(t *testing.T) {
	coded := fault.New(fault.E3004, "x", "", nil)
	if transportControl(coded) != coded {
		t.Fatal("coded fault should pass through")
	}
	if fault.GetCode(transportControl(errors.New("reset"))) != fault.E3004 {
		t.Fatal("raw error should wrap to E3004")
	}
}

func TestFaultFromFileError(t *testing.T) {
	// E8001 is retryable per the catalogue regardless of the sender hint.
	e := faultFromFileError(&wire.FileError{Code: 8001, Retryable: 0, Message: "digest"})
	if !fault.IsRetryable(e) {
		t.Fatalf("E8001 should be retryable")
	}
	// E5007 is fatal per the catalogue.
	if !fault.IsFatal(faultFromFileError(&wire.FileError{Code: 5007})) {
		t.Fatalf("E5007 should be fatal")
	}
}

func TestResolveDestDefault(t *testing.T) {
	got, err := resolveDest(Config{}, "some/nested/Project")
	if err != nil {
		t.Fatal(err)
	}
	if base := got[len(got)-len("Project"):]; base != "Project" {
		t.Fatalf("default dest = %q, want basename Project", got)
	}
	got2, _ := resolveDest(Config{Dest: "/explicit/dir"}, "ignored")
	if got2 != "/explicit/dir" {
		t.Fatalf("explicit dest = %q", got2)
	}
}

func TestResumeCommandMentionsDest(t *testing.T) {
	if s := resumeCommand("/data/out"); s == "" || !contains(s, "/data/out") {
		t.Fatalf("resume command = %q", s)
	}
}

func TestSpaceGuardWarnThenFatal(t *testing.T) {
	g := newSpaceGuard(1000, 200)
	if warn, ferr := g.reserve(700); warn || ferr != nil {
		t.Fatalf("first reserve: warn=%v ferr=%v, want false/nil", warn, ferr)
	}
	warn, ferr := g.reserve(200) // required 900 > 1000-200 => crosses
	if !warn || ferr != nil {
		t.Fatalf("second reserve: warn=%v ferr=%v, want true/nil", warn, ferr)
	}
	_, ferr = g.reserve(500) // required 1400, over budget by 600 > margin 200
	if fault.GetCode(ferr) != fault.E7003 {
		t.Fatalf("third reserve ferr = %v, want E7003", ferr)
	}
}

func TestHardlinkMapSecondaryFlow(t *testing.T) {
	h := newHardlinkMap()
	if h.claimSecondary(42, "b.txt") {
		t.Fatal("first sighting of a key must not be a secondary")
	}
	h.markPrimary(100, 42)
	if !h.claimSecondary(42, "b.txt") {
		t.Fatal("second sighting must be deferred as a secondary")
	}
	waiting := h.registerMaterialised(100, "a.txt")
	if len(waiting) != 1 || waiting[0] != "b.txt" {
		t.Fatalf("waiting = %v, want [b.txt]", waiting)
	}
	if p, ok := h.pathFor(42); !ok || p != "a.txt" {
		t.Fatalf("pathFor = %q,%v", p, ok)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

var _ = time.Second
