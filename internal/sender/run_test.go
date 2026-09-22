package sender

import (
	"bufio"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/wire"
)

// R-17 / MFR-0012: REQ-NET-010 — no code path may wait forever. The listener deadline in
// acceptData bounds Accept, not the CHANNEL_JOIN read behind it, so a silent
// peer on the LAN could park the accept loop on an unauthenticated conn for the
// rest of the session. The read must time out and the loop must move on.
func TestAcceptDataBoundsChannelJoinRead(t *testing.T) {
	lst, err := channel.Listen("127.0.0.1", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lst.Close()

	// A peer that connects and then says nothing at all.
	silent, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(lst.Port()))))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer silent.Close()

	cfg := Config{HandshakeTimeout: 150 * time.Millisecond}
	errc := make(chan error, 1)
	go func() {
		_, aerr := acceptData(obs.Ctx{}, lst, &handshake.Session{}, 1, cfg)
		errc <- aerr
	}()

	// Once the silent conn's join read has timed out, acceptData is back in
	// Accept — which closing the listener ends. If the read is unbounded,
	// acceptData is still blocked on the conn and closing changes nothing.
	time.Sleep(500 * time.Millisecond)
	_ = lst.Close()

	select {
	case aerr := <-errc:
		if aerr == nil {
			t.Fatal("acceptData returned no error after the listener closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("acceptData is parked on the CHANNEL_JOIN read of a silent peer")
	}
}

// R-02 (sender half): the receiver's fatal notification (a *wire.Error on the
// control channel, mirroring what this side already sends in finish) must be
// handled, not treated as an unexpected-message fatal or ignored as a clean
// EOF. Run must exit non-zero with a cause derived from the peer's E-code,
// never "ok".
//
// This drives the real Run over real sockets, playing the receiver's side of
// pairing/SESSION_PARAMS/SESSION_READY/CHANNEL_JOIN by hand (no fake-receiver
// harness exists in this package to reuse), then sends the fatal ERROR the
// receiver would send on its own disk-full/stall/protocol death.
func TestRunReactsToPeerFatalError(t *testing.T) {
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run prints the pairing code to stdout and nothing else (REQ-CLI-005);
	// capture it so the fake receiver below can decode the endpoint/secret.
	oldStdout := os.Stdout
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = pw

	// Log to a file (in addition to the obs.Ctx zero-value's usual silence) so
	// the test can verify Run surfaced the *peer's* E7003, not some other
	// cause (a bare non-zero exit alone would also pass for the unrelated
	// E5001 "unexpected control message" fatal, which proves nothing).
	logPath := filepath.Join(t.TempDir(), "sender.log")
	octx, flush := obs.Init(obs.Config{Sync: true, Role: "sender", File: logPath})

	type result struct {
		sum  Summary
		exit int
	}
	resc := make(chan result, 1)
	go func() {
		sum, exit := Run(octx, Config{
			SourcePath: srcDir, Bind: "127.0.0.1", Port: 0, MaxChannels: 1,
			HandshakeTimeout: 5 * time.Second, DrainTimeout: 3 * time.Second,
		})
		resc <- result{sum, exit}
	}()

	br := bufio.NewReader(pr)
	line, rerr := br.ReadString('\n')
	os.Stdout = oldStdout
	pw.Close()
	if rerr != nil {
		t.Fatalf("read pairing code: %v", rerr)
	}

	payload, derr := paircode.Decode(strings.TrimSpace(line))
	if derr != nil {
		t.Fatalf("decode pairing code: %v", derr)
	}
	ep := payload.Endpoints[0]
	addr := netip.AddrPortFrom(ep.Addr, ep.Port).String()
	kPair := paircode.KPair(payload.Secret)
	sid := paircode.SessionID(payload.Secret)

	nc, derr := net.Dial("tcp", addr)
	if derr != nil {
		t.Fatalf("dial control: %v", derr)
	}
	defer nc.Close()
	hsess, herr := handshake.ClientHandshake(obs.Ctx{}, nc, kPair, sid, 5*time.Second)
	if herr != nil {
		t.Fatalf("client handshake: %v", herr)
	}
	ctrl := channel.Control(nc, hsess, channel.Receiver)
	defer ctrl.Close()

	pm, perr := ctrl.RecvMsg()
	if perr != nil {
		t.Fatalf("recv SESSION_PARAMS: %v", perr)
	}
	if _, ok := pm.(*wire.SessionParams); !ok {
		t.Fatalf("want SESSION_PARAMS, got %T", pm)
	}
	if err := ctrl.SendMsg(&wire.SessionReady{Channels: 1, GroupCredit: 4, MaxChunk: 1 << 16}); err != nil {
		t.Fatalf("send SESSION_READY: %v", err)
	}

	// One data channel to satisfy the sender's acceptData(n=1).
	dnc, derr := net.Dial("tcp", addr)
	if derr != nil {
		t.Fatalf("dial data channel: %v", derr)
	}
	defer dnc.Close()
	if jerr := handshake.JoinChannel(obs.Ctx{}, dnc, hsess, 1); jerr != nil {
		t.Fatalf("join data channel: %v", jerr)
	}

	// The receiver hit a local fatal (E7003 disk full) and reports it before
	// exiting, exactly like the sender's own finish() does in reverse.
	if err := ctrl.SendMsg(&wire.Error{
		Code: 7003, Fatal: 1, Message: "E7003", Detail: "destination out of space",
	}); err != nil {
		t.Fatalf("send ERROR: %v", err)
	}

	var res result
	select {
	case res = <-resc:
		if res.exit == 0 || res.sum.Outcome == "ok" {
			t.Fatalf("exit = %d (outcome %q), want non-zero/non-ok: Run must react to the receiver's peer fatal instead of treating its departure as a clean end", res.exit, res.sum.Outcome)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not react to the peer ERROR and return")
	}
	flush()

	logged, rerr2 := os.ReadFile(logPath)
	if rerr2 != nil {
		t.Fatalf("read sender log: %v", rerr2)
	}
	if !strings.Contains(string(logged), "code=E7003") {
		t.Fatalf("sender log does not carry the peer's E7003 cause (got %q); a non-zero exit for the wrong reason (e.g. E5001 unexpected control message) proves nothing here", string(logged))
	}
}

// R-19 (sender half): --compact-code must cap the pairing code at two
// endpoints, and any code that still ends up carrying more than two must
// draw a §14.1 WARN so the operator knows why. Driving this through a full
// Run would depend on paircode.Discover enumerating this host's real network
// interfaces (unpredictable in a test environment — could be 0, 1, or
// several), so this tests the extracted decision (compactEndpoints) directly
// against a synthetic endpoint list instead.
func TestCompactEndpoints(t *testing.T) {
	fourEps := func() []paircode.Endpoint {
		return []paircode.Endpoint{
			{Addr: netip.MustParseAddr("10.0.0.1"), Port: 1},
			{Addr: netip.MustParseAddr("10.0.0.2"), Port: 1},
			{Addr: netip.MustParseAddr("10.0.0.3"), Port: 1},
			{Addr: netip.MustParseAddr("10.0.0.4"), Port: 1},
		}
	}

	t.Run("compact truncates to two and warns", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "sender.log")
		octx, flush := obs.Init(obs.Config{Sync: true, Role: "sender", File: logPath})

		got := compactEndpoints(octx, fourEps(), true)
		flush()
		if len(got) != 2 {
			t.Fatalf("compact-code: got %d endpoints, want exactly 2", len(got))
		}

		logged, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		// Compact brings the code down to two endpoints, so no §14.1 warning
		// should fire — the whole point of the flag is to avoid it.
		if strings.Contains(string(logged), "consider --compact-code") {
			t.Fatalf("compact-code truncated to 2 but still warned: %s", logged)
		}
	})

	t.Run("without compact-code, more than two endpoints warns", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "sender.log")
		octx, flush := obs.Init(obs.Config{Sync: true, Role: "sender", File: logPath})

		got := compactEndpoints(octx, fourEps(), false)
		flush()
		if len(got) != 4 {
			t.Fatalf("without --compact-code the endpoint list must be left alone here, got %d", len(got))
		}

		logged, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(logged), "consider --compact-code") {
			t.Fatalf("want a §14.1 WARN for a code carrying >2 endpoints, got log: %s", logged)
		}
	})

	t.Run("two or fewer endpoints never warns", func(t *testing.T) {
		logPath := filepath.Join(t.TempDir(), "sender.log")
		octx, flush := obs.Init(obs.Config{Sync: true, Role: "sender", File: logPath})

		two := []paircode.Endpoint{
			{Addr: netip.MustParseAddr("10.0.0.1"), Port: 1},
			{Addr: netip.MustParseAddr("10.0.0.2"), Port: 1},
		}
		got := compactEndpoints(octx, two, false)
		flush()
		if len(got) != 2 {
			t.Fatalf("got %d endpoints, want 2 (no truncation needed)", len(got))
		}
		logged, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(logged), "consider --compact-code") {
			t.Fatalf("must not warn when the code already carries <= 2 endpoints: %s", logged)
		}
	})
}

// R-23: entries the walker warns-and-skips (E6005/E6006/E6007) never reach
// the receiver as FilesFailed, so without threading walkSkipped into the exit
// decision a source subtree the sender could not even read produces a silent
// exit 0 "ok" (REQ-SCAN-032 / ARCHITECTURE §14.6 both require this to count).
// This drives a full session, with an unreadable subdirectory, through a fake
// receiver that completes the whole protocol (GROUP_DECISION through
// SESSION_SUMMARY_ACK) so the run actually finishes rather than aborting —
// proving the exit is 1 "partial" *because of the walk-skip*, not because of
// some other failure (which is why FilesFailed == 0 is asserted too; a
// failure-driven exit 1 could otherwise masquerade as a walk-skip-driven one,
// same trap as TestRunReactsToPeerFatalError).
func TestRunPartialExitOnWalkSkip(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits on the unreadable subdir would be ignored, so the walk-skip this test relies on would not happen")
	}

	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(srcDir, "blocked")
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "b.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(blocked, 0o755) // let TempDir cleanup remove it

	oldStdout := os.Stdout
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = pw

	type result struct {
		sum  Summary
		exit int
	}
	logPath := filepath.Join(t.TempDir(), "sender.log")
	octx, flush := obs.Init(obs.Config{Sync: true, Role: "sender", File: logPath})
	defer flush()

	resc := make(chan result, 1)
	go func() {
		sum, exit := Run(octx, Config{
			SourcePath: srcDir, Bind: "127.0.0.1", Port: 0, MaxChannels: 1,
			HandshakeTimeout: 5 * time.Second, DrainTimeout: 3 * time.Second,
		})
		resc <- result{sum, exit}
	}()

	br := bufio.NewReader(pr)
	line, rerr := br.ReadString('\n')
	os.Stdout = oldStdout
	pw.Close()
	if rerr != nil {
		t.Fatalf("read pairing code: %v", rerr)
	}

	payload, derr := paircode.Decode(strings.TrimSpace(line))
	if derr != nil {
		t.Fatalf("decode pairing code: %v", derr)
	}
	ep := payload.Endpoints[0]
	addr := netip.AddrPortFrom(ep.Addr, ep.Port).String()
	kPair := paircode.KPair(payload.Secret)
	sid := paircode.SessionID(payload.Secret)

	nc, derr := net.Dial("tcp", addr)
	if derr != nil {
		t.Fatalf("dial control: %v", derr)
	}
	defer nc.Close()
	hsess, herr := handshake.ClientHandshake(obs.Ctx{}, nc, kPair, sid, 5*time.Second)
	if herr != nil {
		t.Fatalf("client handshake: %v", herr)
	}
	ctrl := channel.Control(nc, hsess, channel.Receiver)
	defer ctrl.Close()

	pm, perr := ctrl.RecvMsg()
	if perr != nil {
		t.Fatalf("recv SESSION_PARAMS: %v", perr)
	}
	if _, ok := pm.(*wire.SessionParams); !ok {
		t.Fatalf("want SESSION_PARAMS, got %T", pm)
	}
	if err := ctrl.SendMsg(&wire.SessionReady{Channels: 1, GroupCredit: 4, MaxChunk: 1 << 16}); err != nil {
		t.Fatalf("send SESSION_READY: %v", err)
	}

	dnc, derr := net.Dial("tcp", addr)
	if derr != nil {
		t.Fatalf("dial data channel: %v", derr)
	}
	defer dnc.Close()
	if jerr := handshake.JoinChannel(obs.Ctx{}, dnc, hsess, 1); jerr != nil {
		t.Fatalf("join data channel: %v", jerr)
	}
	dc := channel.Data(dnc, 1, hsess, channel.Receiver, 1<<20)
	defer dc.Close()

	// SCAN_COMPLETE, then a GROUP_MANIFEST for the one readable file.
	sc, serr := ctrl.RecvMsg()
	if serr != nil {
		t.Fatalf("recv SCAN_COMPLETE: %v", serr)
	}
	if _, ok := sc.(*wire.ScanComplete); !ok {
		t.Fatalf("want SCAN_COMPLETE, got %T", sc)
	}

	gm, gerr := ctrl.RecvMsg()
	if gerr != nil {
		t.Fatalf("recv GROUP_MANIFEST: %v", gerr)
	}
	manifest, ok := gm.(*wire.GroupManifest)
	if !ok {
		t.Fatalf("want GROUP_MANIFEST, got %T", gm)
	}
	// The walker still emits the unreadable "blocked" directory itself (its
	// entry, not its contents) plus the one readable file; only the file
	// inside "blocked" (b.txt) is the walk-skip this test is about, and it
	// must never show up here.
	if len(manifest.Entries) != 2 {
		t.Fatalf("want the readable file plus the blocked directory's own entry (not its unreadable contents), got %d entries", len(manifest.Entries))
	}
	var fileIdx uint16 = 255
	var fileID uint64
	for i, e := range manifest.Entries {
		const entryTypeFile = 0
		if e.EntryType == entryTypeFile {
			fileIdx = uint16(i)
			fileID = manifest.FirstFileID + uint64(i)
		}
	}
	if fileIdx == 255 {
		t.Fatalf("no file entry found in manifest: %+v", manifest.Entries)
	}

	if err := ctrl.SendMsg(&wire.GroupDecision{GroupID: manifest.GroupID, Needed: []uint16{fileIdx}}); err != nil {
		t.Fatalf("send GROUP_DECISION: %v", err)
	}
	if err := ctrl.SendMsg(&wire.Credit{AdditionalGroups: 4}); err != nil {
		t.Fatalf("send CREDIT: %v", err)
	}
	if err := dc.SendMsg(&wire.FileRequest{RequestID: 1, FileID: fileID, Offset: 0, MaxChunk: 1 << 16}); err != nil {
		t.Fatalf("send FILE_REQUEST: %v", err)
	}

	var gotAllBytes []byte
	for {
		m, merr := dc.RecvMsg()
		if merr != nil {
			t.Fatalf("recv on data channel: %v", merr)
		}
		switch v := m.(type) {
		case *wire.FileHeader:
			// nothing to check here for this test
		case *wire.FileChunk:
			gotAllBytes = append(gotAllBytes, v.Data...)
		case *wire.FileComplete:
			goto fileDone
		case *wire.FileError:
			t.Fatalf("unexpected FILE_ERROR for the readable file: %+v", v)
		default:
			t.Fatalf("unexpected data-channel message %T", m)
		}
	}
fileDone:
	if string(gotAllBytes) != "hello" {
		t.Fatalf("got file content %q, want %q", gotAllBytes, "hello")
	}

	ssm, sserr := ctrl.RecvMsg()
	if sserr != nil {
		t.Fatalf("recv SESSION_SUMMARY: %v", sserr)
	}
	summary, ok := ssm.(*wire.SessionSummary)
	if !ok {
		t.Fatalf("want SESSION_SUMMARY, got %T", ssm)
	}
	if err := ctrl.SendMsg(&wire.SessionSummaryAck{CompletionDigest: summary.CompletionDigest}); err != nil {
		t.Fatalf("send SESSION_SUMMARY_ACK: %v", err)
	}

	var res result
	select {
	case res = <-resc:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the full session completed")
	}

	if res.exit != 1 || res.sum.Outcome != "partial" {
		flush()
		logged, _ := os.ReadFile(logPath)
		t.Fatalf("exit = %d outcome = %q, want exit 1 outcome \"partial\": an unreadable subtree the walker warned-and-skipped must still fail the exit code (REQ-SCAN-032); log:\n%s", res.exit, res.sum.Outcome, logged)
	}
	if res.sum.FilesFailed != 0 {
		t.Fatalf("FilesFailed = %d, want 0: this run's partial exit must come from the walk-skip, not from a receiver-reported failure (that would prove nothing about R-23)", res.sum.FilesFailed)
	}
	// The wire-level SCAN_COMPLETE reports the walker's own skip tally
	// (SCAN_COMPLETE.skipped_entries, unaffected by the wire-off-limits R-23
	// fold done for SESSION_SUMMARY.FilesSkipped) — confirm the blocked
	// subtree was actually counted there, not merely silently dropped.
	if summary.FilesSkipped == 0 {
		t.Fatalf("SESSION_SUMMARY.FilesSkipped = 0, want > 0: the walk-skipped file must be folded into the sender's own summary (R-23)")
	}
}
