package sender

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/plan"
	"esync/internal/wire"
)

// Run executes the sender state machine (ARCHITECTURE §4.1) and returns its own
// session summary and the process exit code (§14.6).
func Run(ctx obs.Ctx, cfg Config) (Summary, int) {
	cfg = cfg.withDefaults()
	start := time.Now()
	sessionEnd := obs.Start(ctx, "session")

	var fatal atomic.Value
	setFatal := func(err error) {
		if err != nil {
			fatal.CompareAndSwap(nil, err)
		}
	}
	// ctrl is assigned once the control channel is up (step 4); finish reads it
	// to tell the receiver *why* the session is ending. Without this the receiver
	// only sees the control connection close and cannot tell an abort from a
	// clean shutdown.
	var ctrl *channel.Conn
	var errNotified bool
	// Both are set once the walker finishes (step 9). finish reads them here
	// rather than through Summary, whose FilesSkipped already means something
	// else (R-23). walkSkipped is every warn-and-skip (E6005/6/7) and only
	// feeds the reported counts; walkUnreadable is the E6005 subset — source
	// content that may be missing from this transfer — and is what makes the
	// run exit 1, since it never reached the receiver and so never became a
	// FilesFailed count. Sockets and symlink cycles are Warn class and §14.6
	// keeps Warn out of the exit code, so they deliberately do not count here.
	var walkSkipped, walkUnreadable uint32
	finish := func(sum Summary) (Summary, int) {
		var ferr error
		if v := fatal.Load(); v != nil {
			ferr = v.(error)
		}
		exit, outcome := 0, "ok"
		switch {
		case ferr != nil:
			exit, outcome = fault.ExitCode(ferr), "error"
			if ctrl != nil && !errNotified && !errors.Is(ferr, fault.ErrSignal) {
				errNotified = true
				code := fault.GetCode(ferr)
				_ = ctrl.SendMsg(&wire.Error{
					Code:    codeNum(code),
					Fatal:   1,
					Message: string(code),
					Detail:  code.Condition(),
				})
			}
			obs.LogFault(ctx, ferr)
		case sum.FilesFailed > 0 || walkUnreadable > 0:
			exit, outcome = 1, "partial"
		}
		sum.Elapsed = time.Since(start)
		sum.Outcome = outcome
		sessionEnd(outcome)
		obs.Summary(ctx, outcome, exit, sum.Elapsed)
		return sum, exit
	}
	fail := func(err error) (Summary, int) {
		setFatal(err)
		return finish(Summary{})
	}

	// --- 1. validate the source path (E1003 / E1004) --------------------------
	absRoot, err := filepath.Abs(cfg.SourcePath)
	if err != nil {
		return fail(fault.Wrap(fault.E1003, "resolve source path", cfg.SourcePath, err))
	}
	fi, err := os.Lstat(absRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fail(fault.Wrap(fault.E1003, "stat source", absRoot, err))
		}
		return fail(fault.Wrap(fault.E1004, "stat source", absRoot, err))
	}
	sourceKind := uint8(0)
	if !fi.IsDir() {
		sourceKind = 1
	}
	if err := checkReadable(absRoot, fi); err != nil {
		return fail(err)
	}

	// --- 2. discover endpoints ---------------------------------------------
	eps, err := paircode.Discover(cfg.Bind)
	if err != nil {
		return fail(err)
	}

	// --- 3. derive the session secret (E4007) -----------------------------
	sb := make([]byte, paircode.SecretLen)
	if _, err := rand.Read(sb); err != nil {
		return fail(fault.Wrap(fault.E4007, "generate session secret", "", err))
	}
	secret := obs.NewSecret(sb)
	defer secret.Zero()
	sessionID := paircode.SessionID(secret)
	kPair := paircode.KPair(secret)
	ctx = obs.With(ctx, obs.F("session", hex.EncodeToString(sessionID[:])))

	// --- 4. listen -------------------------------------------------------
	lst, err := channel.Listen(cfg.Bind, cfg.Port)
	if err != nil {
		return fail(err)
	}
	defer lst.Close()
	port := lst.Port()
	for i := range eps {
		eps[i].Port = port
	}
	if len(eps) > 4 {
		eps = eps[:4]
	}
	eps = compactEndpoints(ctx, eps, cfg.CompactCode)

	// --- 5. print the pairing code (stdout: the code only) ---------------
	code := paircode.Encode(&paircode.Payload{Endpoints: eps, Secret: secret, Restricted: cfg.Bind != ""})
	fmt.Fprintln(os.Stdout, code)
	fmt.Fprintf(os.Stderr, "\n  esync pairing code (valid for %s, single use):\n\n      %s\n\n  on the other machine:  esync --link %s\n\n",
		cfg.PairTimeout, code, code)
	obs.Info(ctx, "listening for a receiver", obs.F("endpoints", len(eps)), obs.F("port", port))

	// signal handling (§14.7)
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigc)
	octx := obs.WithContext(ctx, rootCtx)
	obs.Go(octx, "signal.watch", func() error {
		select {
		case <-sigc:
			obs.Warn(ctx, "signal received, terminating")
			setFatal(fault.ErrSignal)
			cancel()
			_ = lst.Close() // unblock a pending Accept during pairing
		case <-rootCtx.Done():
		}
		return nil
	})

	// --- 6. pair: accept + handshake (E2006 / E2007) --------------------
	sess, ctrlNC, err := pair(ctx, lst, kPair, sessionID, cfg)
	if err != nil {
		return fail(err)
	}
	ctrl = channel.Control(ctrlNC, sess, channel.Sender)
	defer ctrl.Close()
	pairEnd := obs.Start(ctx, "pair")
	pairEnd("ok", obs.F("peer_version", sess.PeerVersion))

	// --- 7. SESSION_PARAMS -> SESSION_READY ----------------------------
	if err := ctrl.SendMsg(sessionParams(cfg, absRoot, sourceKind)); err != nil {
		return fail(err)
	}
	msg, err := ctrl.RecvMsg()
	if err != nil {
		return fail(err)
	}
	ready, ok := msg.(*wire.SessionReady)
	if !ok {
		return fail(fault.Newf(fault.E5001, "await SESSION_READY", msg.Type().String(), nil, "expected SESSION_READY"))
	}
	n := int(ready.Channels)
	if n > cfg.MaxChannels {
		n = cfg.MaxChannels
	}
	chunkSize := cfg.ChunkSize
	if mc := int(ready.MaxChunk); mc > 0 && mc < chunkSize {
		chunkSize = mc
	}
	obs.Info(ctx, "receiver linked", obs.F("channels", n), obs.F("credit", ready.GroupCredit), obs.F("chunk", chunkSize))

	// --- 8. accept N data channels ------------------------------------
	dataConns, err := acceptData(octx, lst, sess, n, cfg)
	if err != nil {
		return fail(err)
	}
	for _, dc := range dataConns {
		defer dc.Close()
	}
	// lst stays open (closed only by the step-4 defer, on Run's return): the
	// receiver's adaptive tuner (§13.4) may CHANNEL_JOIN more data channels than
	// this initial batch, up to cfg.MaxChannels (§7.1), and needs somewhere to
	// join them.

	ctrl.EnableKeepalive(octx)

	// --- 9. scan / hash / manifest / serve ---------------------------
	entriesCh := make(chan plan.Entry, 4096)
	var walkErr error
	scanEnd := obs.Start(ctx, "scan")
	obs.Go(octx, "walker", func() error {
		s, u, e := walkTree(octx, walkConfig{
			root:           absRoot,
			followSymlinks: cfg.FollowSymlinks,
			oneFileSystem:  cfg.OneFileSystem,
			hardlinks:      cfg.Hardlinks,
		}, entriesCh)
		walkSkipped, walkUnreadable, walkErr = s, u, e
		close(entriesCh)
		return nil
	})
	var collected []plan.Entry
	for e := range entriesCh {
		collected = append(collected, e)
	}
	if walkErr != nil {
		scanEnd("error")
		return fail(walkErr)
	}
	scanEnd("ok", obs.F("entries", len(collected)))

	pl, err := plan.Build(collected, cfg.planOptions())
	if err != nil {
		return fail(err)
	}

	ds := newDigestStore()
	cg := newCreditGate(int(ready.GroupCredit))
	tr := newTracker(pl.GroupFirsts())

	var cache *digest.Cache
	if !cfg.NoCache {
		if p, e := digest.DefaultCachePath(); e == nil {
			cache = digest.OpenCache(ctx, p)
		}
	}

	// control reader: decisions, credit, and the summary ack
	ackc := make(chan *wire.SessionSummaryAck, 1)
	obs.Go(octx, "control.reader", func() error {
		for {
			m, err := ctrl.RecvMsg()
			if err != nil {
				if !errors.Is(err, io.EOF) && rootCtx.Err() == nil {
					setFatal(err)
				}
				cancel()
				return nil
			}
			switch v := m.(type) {
			case *wire.GroupDecision:
				if derr := tr.decision(v); derr != nil {
					setFatal(derr)
					cancel()
					return nil
				}
				cs := ctx.Counters()
				cs.FilesSkipped.Add(int64(v.SkippedCount))
				cs.FilesRejected.Add(int64(len(v.Rejected)))
			case *wire.Credit:
				cg.add(int(v.AdditionalGroups))
			case *wire.SessionSummaryAck:
				select {
				case ackc <- v:
				default:
				}
			case *wire.Error:
				// The receiver hit a fatal (E7003 disk full, E3005 stall, E5006
				// protocol, ...) and is telling us before it exits (ARCHITECTURE
				// §14.4 / REQ-ERR-005: "both sides log the same cause"). Without
				// this the reader falls through to a bare EOF on the receiver's
				// close and treats a receiver death as a clean end of session.
				setFatal(peerError(v))
				cancel()
				return nil
			default:
				setFatal(fault.Newf(fault.E5001, "control.reader", m.Type().String(), nil, "unexpected control message"))
				cancel()
				return nil
			}
		}
	})

	readSem := make(chan struct{}, cfg.ReadConcurrency)
	entries := pl.Entries()
	var activeChannels atomic.Int64
	startServicer := func(dc *channel.Conn) {
		s := &servicer{
			ctx:       obs.With(octx, obs.F("chan", int(dc.ID()))),
			conn:      dc,
			entries:   entries,
			root:      absRoot,
			algo:      cfg.HashAlg,
			chunkSize: chunkSize,
			readSem:   readSem,
			digests:   ds,
			tr:        tr,
		}
		activeChannels.Add(1)
		obs.Go(octx, "servicer", func() error {
			defer activeChannels.Add(-1)
			defer dc.Close() // free the socket as soon as this worker ends
			if err := s.run(rootCtx); err != nil {
				if rootCtx.Err() == nil {
					if fault.GetCode(err) == "" {
						// A raw transport error means the data channel itself died
						// (ARCHITECTURE §7.3 row 1): the receiver requeues its
						// in-flight file and rejoins with a fresh CHANNEL_JOIN,
						// which join.accept below keeps answering. This is not a
						// session failure. Coded faults (record/auth violations)
						// stay fatal.
						obs.Debug(ctx, "data channel lost, awaiting a receiver rejoin",
							obs.F("chan", int(dc.ID())), obs.F("err", err.Error()))
					} else {
						setFatal(fault.Wrap(fault.E3005, "data channel", "", err))
						cancel()
					}
				}
			}
			return nil
		})
	}
	for _, dc := range dataConns {
		startServicer(dc)
	}

	// Keep accepting CHANNEL_JOINs beyond the initial batch — and beyond the
	// count the receiver opened at SESSION_READY: the adaptive tuner (§13.4)
	// grows N up to cfg.MaxChannels, and a lost data channel is rejoined by a
	// fresh connection (§7.3). Each join gets its own servicer, exactly like the
	// initial batch; the per-join ceiling is enforced inside AcceptChannel
	// (channel id must stay ≤ cfg.MaxChannels), so this loop simply runs until
	// lst closes on Run's return.
	obs.Go(octx, "join.accept", func() error {
		for {
			nc, err := lst.Accept()
			if err != nil {
				return nil // lst closed: session ending
			}
			id, jerr := handshake.AcceptChannel(octx, nc, sess, uint8(cfg.MaxChannels))
			if jerr != nil {
				_ = nc.Close()
				obs.Warn(ctx, "channel join rejected", obs.F("cause", jerr))
				continue
			}
			startServicer(channel.Data(nc, id, sess, channel.Sender, cfg.SocketBuffer))
		}
	})

	mp := &manifestPublisher{
		ctx:     octx,
		ctrl:    ctrl,
		pl:      pl,
		root:    absRoot,
		algo:    cfg.HashAlg,
		cache:   cache,
		quick:   cfg.Quick,
		workers: cfg.HashWorkers,
		skipped: walkSkipped,
		digests: ds,
		credit:  cg,
	}
	obs.Go(octx, "manifest", func() error {
		if err := mp.run(rootCtx); err != nil {
			if rootCtx.Err() == nil {
				setFatal(err)
			}
			cancel()
		}
		return nil
	})

	hb := obs.NewHeartbeat(octx, 0, 0, tr.inProgressGroups, func() int { return int(activeChannels.Load()) })
	defer hb.Stop()

	// Progress sink (§15.10). The receiver drives which files are skipped, so the
	// sender does not know its own total; it reports bytes and files sent only.
	prog := obs.NewProgress(octx, cfg.ProgressInterval)
	defer prog.Stop()
	prog.Feed(octx, func() (int64, int64, int64, int64) {
		transferred, _, _, bytesSent := tr.counts()
		return int64(bytesSent), 0, int64(transferred), 0
	})

	// --- 10. wait for termination, then drain -----------------------
	drainEnd := obs.Start(ctx, "drain")
	if err := tr.wait(rootCtx, cfg.DrainTimeout); err != nil {
		drainEnd("error")
		if v := fatal.Load(); v != nil {
			return finish(Summary{})
		}
		return fail(err)
	}
	drainEnd("ok")

	comp := tr.completionDigest()
	transferred, skipped, failed, bytesSent := tr.counts()
	// R-23: fold the walker's warn-and-skip count (E6005/E6006/E6007, entries
	// the sender itself could not read and so never offered to the receiver)
	// into FilesSkipped alongside receiver-driven skips + excluded entries.
	// wire.SessionSummary has no separate field for this and internal/wire is
	// outside this fix's ownership, so the two are conflated here rather than
	// left to silently vanish (REQ-SCAN-032).
	sum := &wire.SessionSummary{
		FilesTotal:       pl.TotalFiles,
		FilesTransferred: transferred,
		FilesSkipped:     skipped + uint64(pl.ExcludedCount) + uint64(walkSkipped),
		FilesFailed:      failed,
		BytesTransferred: bytesSent,
		GroupsTotal:      uint32(pl.NumGroups()),
		CompletionDigest: comp[:],
	}
	if err := ctrl.SendMsg(sum); err != nil {
		return fail(err)
	}
	select {
	case ack := <-ackc:
		if len(ack.CompletionDigest) != len(comp) || string(ack.CompletionDigest) != string(comp[:]) {
			return fail(fault.New(fault.E5005, "compare completion digest", "", nil))
		}
	case <-rootCtx.Done():
		return finish(Summary{})
	case <-time.After(cfg.DrainTimeout):
		return fail(fault.New(fault.E9002, "await SESSION_SUMMARY_ACK", "", nil))
	}

	return finish(Summary{
		FilesTotal:       pl.TotalFiles,
		FilesTransferred: transferred,
		FilesSkipped:     sum.FilesSkipped,
		FilesFailed:      failed,
		BytesTransferred: bytesSent,
		GroupsTotal:      uint32(pl.NumGroups()),
	})
}

func checkReadable(abs string, fi os.FileInfo) error {
	if fi.IsDir() {
		f, err := os.Open(abs)
		if err != nil {
			return fault.Wrap(fault.E1004, "open source", abs, err)
		}
		_, err = f.ReadDir(1)
		f.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			return fault.Wrap(fault.E1004, "read source", abs, err)
		}
		return nil
	}
	f, err := os.Open(abs)
	if err != nil {
		return fault.Wrap(fault.E1004, "open source", abs, err)
	}
	f.Close()
	return nil
}

// pair runs the accept/handshake loop until a receiver completes the handshake,
// the pair timeout elapses (E2006), or the attempt budget is spent (E2007).
func pair(ctx obs.Ctx, lst *channel.Listener, kPair obs.Secret, sessionID [8]byte, cfg Config) (*handshake.Session, net.Conn, error) {
	_ = lst.SetDeadline(time.Now().Add(cfg.PairTimeout))
	defer lst.SetDeadline(time.Time{})

	attempts := 0
	for {
		nc, err := lst.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, nil, fault.New(fault.E2006, "await receiver", "", nil)
			}
			return nil, nil, err
		}
		sess, herr := handshake.ServerHandshake(ctx, nc, kPair, sessionID, cfg.HandshakeTimeout)
		if herr != nil {
			_ = nc.Close()
			attempts++
			obs.Warn(ctx, "handshake rejected", obs.F("attempt", attempts), obs.F("max", cfg.MaxPairAttempts))
			if attempts >= cfg.MaxPairAttempts {
				return nil, nil, fault.New(fault.E2007, "pair", "", nil)
			}
			continue
		}
		return sess, nc, nil
	}
}

func acceptData(ctx obs.Ctx, lst *channel.Listener, sess *handshake.Session, n int, cfg Config) ([]*channel.Conn, error) {
	if n <= 0 {
		return nil, nil
	}
	_ = lst.SetDeadline(time.Now().Add(cfg.HandshakeTimeout + 10*time.Second))
	defer lst.SetDeadline(time.Time{})

	conns := make([]*channel.Conn, 0, n)
	for len(conns) < n {
		nc, err := lst.Accept()
		if err != nil {
			return nil, fault.Wrap(fault.E4002, "accept data channel", "", err)
		}
		// REQ-NET-010: no code path may wait forever. The listener deadline above
		// bounds Accept, not the CHANNEL_JOIN read that follows it — without this
		// any host on the LAN can connect, stay silent, and park AcceptChannel's
		// io.ReadFull on a still-unauthenticated conn, stalling the accept loop for
		// the rest of the session. Mirrors the receiver's dialOneData.
		_ = nc.SetDeadline(time.Now().Add(cfg.HandshakeTimeout))
		id, jerr := handshake.AcceptChannel(ctx, nc, sess, uint8(n))
		if jerr != nil {
			_ = nc.Close()
			obs.Warn(ctx, "channel join rejected", obs.F("cause", jerr))
			continue
		}
		_ = nc.SetDeadline(time.Time{})
		conns = append(conns, channel.Data(nc, id, sess, channel.Sender, cfg.SocketBuffer))
	}
	return conns, nil
}

func sessionParams(cfg Config, absRoot string, sourceKind uint8) *wire.SessionParams {
	return &wire.SessionParams{
		RootName:        filepath.Base(absRoot),
		SourceKind:      sourceKind,
		HashAlg:         uint8(cfg.HashAlg),
		GroupBytes:      uint64(cfg.GroupBytes),
		Flags:           cfg.sessionFlags(),
		SourcePlatform:  platformID(),
		PathNorm:        1,
		SenderVersion:   cfg.Version,
		MaxChannels:     uint32(cfg.MaxChannels),
		FilterSignature: cfg.planOptions().FilterSignature(),
		SysexcludeTag:   sysexcludeTag(cfg),
	}
}

// compactEndpoints applies --compact-code (R-19, §16.2/§5.2) and the §14.1
// warning: with compact set, the code is capped at two endpoints so it stays
// inside the REQ-PAIR-002 64-character budget; either way, a code still
// carrying more than two endpoints gets a stderr WARN (obs — stdout carries
// only the pairing code itself, REQ-CLI-005) rather than growing silently.
// Pulled out of Run so the truncate/warn decision can be tested directly
// against a synthetic endpoint list, without depending on this host's real
// network interfaces via paircode.Discover.
func compactEndpoints(ctx obs.Ctx, eps []paircode.Endpoint, compact bool) []paircode.Endpoint {
	if compact && len(eps) > 2 {
		eps = eps[:2]
	}
	if len(eps) > 2 {
		obs.Warn(ctx, "pairing code carries more than two endpoints, consider --compact-code",
			obs.F("endpoints", len(eps)))
	}
	return eps
}

// peerError converts an inbound *wire.Error from the receiver (its fatal
// notification, mirroring this side's own send in finish) into a *fault.Fault
// carrying the receiver's E-code, so the sender's exit code and logged cause
// match what the receiver reported instead of a bare "clean EOF".
func peerError(e *wire.Error) error {
	code := fault.Code(fmt.Sprintf("E%04d", e.Code))
	msg := e.Message
	if msg == "" {
		msg = "receiver reported a fatal error"
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return fault.Newf(code, "receiver reported an error", "", nil, "%s", msg)
}

func sysexcludeTag(cfg Config) string {
	if cfg.KeepSystemFiles {
		return ""
	}
	return "sys-v1"
}
