package receiver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"esync/internal/channel"
	"esync/internal/crypto/handshake"
	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/fsx"
	"esync/internal/obs"
	"esync/internal/paircode"
	"esync/internal/wire"
)

// workerStopGrace bounds how long Run waits for the decide/fetch goroutines to
// unwind once the root context is cancelled.
const workerStopGrace = 5 * time.Second

// session is the mutable state shared by Run and its goroutines. fail records
// the first fatal error and cancels the root context; every obs.Go closure
// returns nil and routes errors here (obs.Go escalates Fatal returns to the
// process-wide OnFatal handler, which Run does not want).
type session struct {
	octx     obs.Ctx
	cfg      Config
	rootCtx  context.Context
	cancel   context.CancelFunc
	destPath string
	cnt      *obs.Counters

	reqID atomic.Uint64

	fatalMu  sync.Mutex
	fatalErr error

	compMu    sync.Mutex
	completed []uint64

	// Dynamic data-channel pool (ARCHITECTURE §13.4): grown by the adaptive
	// tuner up to senderMaxChannels/cfg.MaxChannels and shrunk back down to
	// cfg.MinChannels, or fixed at cfg.Channels when cfg.ChannelsPinned. addr,
	// hsess, and the pipeline fields below are set once in Run before the
	// pipeline starts, so addChannel can dial and wire up a fetcher on its own.
	chMu              sync.Mutex
	chWg              sync.WaitGroup
	workers           []*chanWorker
	nextChID          uint8
	pipelineDone      bool
	senderMaxChannels int

	addr    string
	hsess   *handshake.Session
	dest    *fsx.Dest
	journal *fsx.Journal
	algo    digest.Algo
	q       *needQueue
	meta    *metaStore
	hl      *hardlinkMap
}

// chanWorker is one live data-channel connection plus the cancel func that
// retires it (§13.4 "retire idle channels").
type chanWorker struct {
	id     uint8
	conn   *channel.Conn
	cancel context.CancelFunc
}

var errPipelineDone = errors.New("channel pool is draining")

func (s *session) fail(err error) {
	if err == nil {
		return
	}
	s.fatalMu.Lock()
	if s.fatalErr == nil {
		s.fatalErr = err
	}
	s.fatalMu.Unlock()
	s.cancel()
}

func (s *session) err() error {
	s.fatalMu.Lock()
	defer s.fatalMu.Unlock()
	return s.fatalErr
}

func (s *session) recordComplete(fileID uint64) {
	s.compMu.Lock()
	s.completed = append(s.completed, fileID)
	s.compMu.Unlock()
}

// completionDigest is SHA-256 over the ascending-sorted file_ids that reached
// FILE_COMPLETE this session, each written big-endian u64 (matches the sender's
// tracker.completionDigest, ARCHITECTURE §13.7 / T-PROTO-06).
func (s *session) completionDigest() [32]byte {
	s.compMu.Lock()
	ids := append([]uint64(nil), s.completed...)
	s.compMu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	var b [8]byte
	for _, id := range ids {
		binary.BigEndian.PutUint64(b[:], id)
		h.Write(b[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// dialOneData dials and joins one more data channel to the already-chosen
// endpoint/session (s.addr/s.hsess, set by dialData for the initial batch).
func (s *session) dialOneData(octx obs.Ctx, id uint8) (*channel.Conn, error) {
	var d net.Dialer
	dctx, dcancel := context.WithTimeout(s.rootCtx, s.cfg.ConnectTimeout)
	defer dcancel()
	raw, err := d.DialContext(dctx, "tcp", s.addr)
	if err != nil {
		return nil, fault.Wrap(fault.E3006, "dial data channel", s.addr, err)
	}
	if err := handshake.JoinChannel(octx, raw, s.hsess, id); err != nil {
		raw.Close()
		return nil, err
	}
	return channel.Data(raw, id, s.hsess, channel.Receiver, s.cfg.SocketBuffer), nil
}

// newFetcher builds the fetcher for one data channel from the pipeline fields
// stashed on s once (dest/journal/algo/q/meta/hl), shared by the initial batch
// and every channel the tuner opens later.
func (s *session) newFetcher(octx obs.Ctx, conn *channel.Conn) *fetcher {
	return &fetcher{
		octx:       octx,
		cfg:        s.cfg,
		conn:       conn,
		dest:       s.dest,
		journal:    s.journal,
		algo:       s.algo,
		q:          s.q,
		meta:       s.meta,
		hl:         s.hl,
		counters:   s.cnt,
		reqID:      &s.reqID,
		rnd:        rand.New(rand.NewSource(int64(conn.ID())*7919 + time.Now().UnixNano())),
		onComplete: s.recordComplete,
		fail:       s.fail,
	}
}

// registerAndStart adds an already-dialed connection to the live worker list
// and starts its fetch loop. The caller must already have accounted for it in
// s.chWg (a chWg.Add(1) done under s.chMu, so it can never race the pipeline's
// final chWg.Wait — see addChannel and the initial dial in Run).
func (s *session) registerAndStart(octx obs.Ctx, id uint8, conn *channel.Conn) {
	chCtx, cancel := context.WithCancel(s.rootCtx)
	w := &chanWorker{id: id, conn: conn, cancel: cancel}
	s.chMu.Lock()
	s.workers = append(s.workers, w)
	s.chMu.Unlock()

	f := s.newFetcher(obs.With(octx, obs.F("chan", int(id))), conn)
	obs.Go(octx, "fetch", func() error {
		defer func() {
			conn.Close() // idempotent; frees the sender-side slot immediately on retire
			s.removeWorker(id)
			s.chWg.Done()
		}()
		f.loop(chCtx)
		return nil
	})
}

// addChannel dials, registers, and starts one more data-channel worker
// (ARCHITECTURE §7.1 "N ... may grow later via CHANNEL_JOIN", §13.4 ramp-up).
// It returns errPipelineDone once the pipeline has begun its final drain — a
// ramp-up racing the very end of a session is simply skipped.
func (s *session) addChannel(octx obs.Ctx) error {
	s.chMu.Lock()
	if s.pipelineDone {
		s.chMu.Unlock()
		return errPipelineDone
	}
	s.nextChID++
	id := s.nextChID
	s.chWg.Add(1)
	s.chMu.Unlock()

	conn, err := s.dialOneData(octx, id)
	if err != nil {
		s.chWg.Done()
		return err
	}
	s.registerAndStart(octx, id, conn)
	return nil
}

// retireOne cancels the most recently opened channel's fetch loop, so it exits
// (and closes its connection) after finishing whatever it is doing right now.
// It never retires the last live channel — that is E3005, fatal, and never
// something the tuner should cause (§13.7). Reports whether it retired one.
func (s *session) retireOne() bool {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	if len(s.workers) <= 1 {
		return false
	}
	w := s.workers[len(s.workers)-1]
	w.cancel()
	return true
}

func (s *session) removeWorker(id uint8) {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	for i, w := range s.workers {
		if w.id == id {
			s.workers = append(s.workers[:i], s.workers[i+1:]...)
			return
		}
	}
}

func (s *session) channelCount() int {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	return len(s.workers)
}

// waitGrace blocks on ch for up to d longer, warning if it never fires —
// bounding how long Run waits for a goroutine to unwind once the root
// context has already been cancelled (workerStopGrace).
func waitGrace(ctx obs.Ctx, ch <-chan struct{}, d time.Duration) {
	select {
	case <-ch:
	case <-time.After(d):
		obs.Warn(ctx, "pipeline did not drain in time")
	}
}

// Run executes the receiver side of one session (ARCHITECTURE §4.2) and returns
// the receiver's own Summary and the process exit code (§14.6).
func Run(ctx obs.Ctx, cfg Config) (Summary, int) {
	cfg = cfg.withDefaults()
	start := time.Now()
	spanEnd := obs.Start(ctx, "session")

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &session{octx: ctx, cfg: cfg, rootCtx: rootCtx, cancel: cancel, cnt: ctx.Counters()}

	var (
		scComplete *wire.ScanComplete
		journal    *fsx.Journal
	)

	finish := func() (Summary, int) {
		ferr := s.err()
		sum := s.summary(scComplete)
		exit := 0
		outcome := "ok"
		switch {
		case ferr != nil && errors.Is(ferr, fault.ErrSignal):
			exit, outcome = 5, "signal"
		case ferr != nil:
			exit, outcome = fault.ExitCode(ferr), "error"
			obs.LogFault(ctx, ferr)
		case sum.FilesFailed > 0 || sum.FilesRejected > 0:
			exit, outcome = 1, "partial"
		}
		if exit != 0 && journal != nil {
			_ = journal.Sync()
			_ = journal.Close() // retain .esync for resume
			sum.ResumeCommand = resumeCommand(s.destPath)
		}
		sum.Outcome = outcome
		sum.Elapsed = time.Since(start)
		spanEnd(outcome)
		obs.Summary(ctx, outcome, exit, sum.Elapsed)
		if sum.ResumeCommand != "" {
			fmt.Fprintf(os.Stderr, "\nresume this transfer with:\n    %s\n", sum.ResumeCommand)
		}
		return sum, exit
	}
	fail := func(err error) (Summary, int) {
		s.fail(err)
		return finish()
	}

	// 1. Decode the pairing code before any I/O (E2001/E2002/E2004).
	payload, err := paircode.Decode(cfg.Link)
	if err != nil {
		return fail(err)
	}
	secret := payload.Secret
	defer secret.Zero()
	sessionID := paircode.SessionID(secret)
	kPair := paircode.KPair(secret)
	ctx = obs.With(ctx, obs.F("session", hex.EncodeToString(sessionID[:])))
	s.octx = ctx

	// 2. Race the candidate endpoints and run the client handshake (E3001/E4001).
	eps := orderCandidates(payload.Endpoints)
	var sessions sync.Map // net.Conn -> *handshake.Session
	hs := func(hctx context.Context, nc net.Conn) error {
		hsess, herr := handshake.ClientHandshake(obs.WithContext(ctx, hctx), nc, kPair, sessionID, cfg.HandshakeTimeout)
		if herr != nil {
			return herr
		}
		sessions.Store(nc, hsess)
		return nil
	}
	nc, chosen, tab, derr := channel.Dial(ctx, eps, cfg.ConnectTimeout, cfg.Stagger, hs)
	if derr != nil {
		if c := authCodeFromTable(tab); c != "" {
			return fail(fault.New(c, "authenticate to sender", "", nil))
		}
		return fail(derr)
	}
	sv, _ := sessions.Load(nc)
	sess := sv.(*handshake.Session)

	ctrl := channel.Control(nc, sess, channel.Receiver)
	defer ctrl.Close()

	// 3. SESSION_PARAMS.
	pm, err := ctrl.RecvMsg()
	if err != nil {
		return fail(fault.Wrap(fault.E4001, "await SESSION_PARAMS", "", err))
	}
	sp, ok := pm.(*wire.SessionParams)
	if !ok {
		return fail(fault.Newf(fault.E5001, "await SESSION_PARAMS", pm.Type().String(), nil, "unexpected control message"))
	}
	algo := digest.Algo(sp.HashAlg)
	if _, herr := digest.New(algo); herr != nil {
		return fail(herr) // E1007 for BLAKE3 / unknown
	}
	s.senderMaxChannels = int(sp.MaxChannels)

	// 4. Destination root.
	destPath, err := resolveDest(cfg, sp.RootName)
	if err != nil {
		return fail(err)
	}
	s.destPath = destPath
	if err := os.MkdirAll(destPath, 0o777); err != nil {
		return fail(fault.Wrap(fault.E1006, "create destination directory", destPath, err))
	}
	if err := checkWritable(destPath); err != nil {
		return fail(err)
	}
	dest, err := fsx.OpenDest(destPath, cfg.NoFsync)
	if err != nil {
		return fail(err)
	}
	defer dest.Close()

	freeBytes, fbErr := fsx.FreeBytes(destPath)
	if fbErr != nil {
		obs.Warn(ctx, "could not measure destination free space", obs.F("err", fbErr.Error()))
	}

	var cache *digest.Cache
	if !cfg.NoCache {
		if cp, cErr := digest.DefaultCachePath(); cErr == nil {
			cache = digest.OpenCache(ctx, cp)
			defer cache.Close()
		}
	}

	// 5. SESSION_READY. --channels pins the count; otherwise the adaptive tuner
	// (§13.4) owns N from here and starts low, at the configured floor.
	nCh := cfg.Channels
	if !cfg.ChannelsPinned {
		nCh = cfg.MinChannels
	}
	if sp.MaxChannels > 0 && nCh > int(sp.MaxChannels) {
		nCh = int(sp.MaxChannels)
	}
	if cfg.MaxChannels > 0 && nCh > cfg.MaxChannels {
		nCh = cfg.MaxChannels
	}
	if nCh < 1 {
		nCh = 1
	}
	if err := ctrl.SendMsg(&wire.SessionReady{
		Channels:        uint16(nCh),
		GroupCredit:     uint16(cfg.GroupCredit),
		PipelineDepth:   uint8(cfg.PipelineDepth),
		MaxChunk:        safeMaxChunk,
		DestPlatform:    platformID(),
		ReceiverVersion: cfg.Version,
		DestFreeBytes:   freeBytes,
	}); err != nil {
		return fail(err)
	}

	// 6. Open N data channels to the winning endpoint.
	dataConns, err := s.dialData(ctx, chosen, sess, nCh)
	if err != nil {
		return fail(err)
	}
	for _, dc := range dataConns {
		defer dc.Close()
	}

	octx := obs.WithContext(ctx, rootCtx)
	ctrl.EnableKeepalive(octx)

	// Signal handling (§14.7): first signal cancels; in-flight work checkpoints.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigc)
	obs.Go(octx, "signal.watch", func() error {
		select {
		case <-sigc:
			obs.Warn(ctx, "signal received, stopping transfer")
			s.fail(fault.ErrSignal)
		case <-rootCtx.Done():
		}
		return nil
	})

	// 7. Control reader: SCAN_COMPLETE, GROUP_MANIFEST, SESSION_SUMMARY.
	scanCh := make(chan *wire.ScanComplete, 1)
	manifestCh := make(chan *wire.GroupManifest, 2*cfg.GroupCredit+8)
	summaryCh := make(chan *wire.SessionSummary, 1)
	closeManifest := sync.OnceFunc(func() { close(manifestCh) })

	obs.Go(octx, "control.reader", func() error {
		defer closeManifest()
		total := -1
		seen := 0
		for {
			m, rerr := ctrl.RecvMsg()
			if rerr != nil {
				if !errors.Is(rerr, io.EOF) && rootCtx.Err() == nil {
					s.fail(transportControl(rerr))
				}
				s.cancel()
				return nil
			}
			switch v := m.(type) {
			case *wire.ScanComplete:
				total = int(v.TotalGroups)
				select {
				case scanCh <- v:
				default:
				}
				if total == 0 {
					closeManifest()
				}
			case *wire.GroupManifest:
				select {
				case manifestCh <- v:
				case <-rootCtx.Done():
					return nil
				}
				seen++
				if total >= 0 && seen >= total {
					closeManifest()
				}
			case *wire.SessionSummary:
				select {
				case summaryCh <- v:
				default:
				}
			case *wire.Error:
				s.fail(peerError(v))
				s.cancel()
				return nil
			default:
				s.fail(fault.Newf(fault.E5001, "control.reader", m.Type().String(), nil, "unexpected control message"))
				s.cancel()
				return nil
			}
		}
	})

	// 8. Wait for SCAN_COMPLETE, then open the journal.
	select {
	case scComplete = <-scanCh:
	case <-rootCtx.Done():
		return finish()
	case <-time.After(cfg.DrainTimeout):
		return fail(fault.New(fault.E9002, "await SCAN_COMPLETE", "", nil))
	}

	var manifestDigest [32]byte
	copy(manifestDigest[:], scComplete.ManifestDigest)

	prior := map[uint64]fsx.Record{}
	if !cfg.DryRun {
		j, p, jerr := fsx.Open(destPath, hex.EncodeToString(sessionID[:]), manifestDigest)
		if jerr != nil {
			return fail(jerr)
		}
		journal, prior = j, p
		if j.Resumed {
			obs.Info(ctx, "resuming a prior transfer", obs.F("already_complete", len(p)))
		}
	}

	// 9. Build the decide + fetch pipeline.
	q := newNeedQueue(needQueueDepth)
	guard := newSpaceGuard(freeBytes, spaceMarginBytes)
	meta := newMetaStore()
	hl := newHardlinkMap()
	destPlat := fsx.Platform(platformID())

	s.dest, s.journal, s.algo, s.q, s.meta, s.hl = dest, journal, algo, q, meta, hl

	dec := &decider{
		octx:       octx,
		cfg:        cfg,
		dest:       dest,
		q:          q,
		guard:      guard,
		algo:       algo,
		destPlat:   destPlat,
		cache:      cache,
		dryRun:     cfg.DryRun,
		collisions: fsx.NewCollisions(destPlat),
		hl:         hl,
		meta:       meta,
		prior:      prior,
		dirMeta:    map[string]dirRec{},
		send:       ctrl.SendMsg,
		counters:   s.cnt,
	}
	if journal != nil {
		dec.sync = journal.Sync
	}

	var decideWg sync.WaitGroup
	for i := 0; i < cfg.DecideWorkers; i++ {
		decideWg.Add(1)
		obs.Go(octx, "decide", func() error {
			defer decideWg.Done()
			for gm := range manifestCh {
				if derr := dec.decideGroup(rootCtx, gm); derr != nil {
					s.fail(derr)
					return nil
				}
			}
			return nil
		})
	}

	if !cfg.DryRun {
		for _, dc := range dataConns {
			s.chWg.Add(1)
			s.registerAndStart(octx, dc.ID(), dc)
		}
		if !cfg.ChannelsPinned {
			obs.Go(octx, "tune", func() error {
				s.tune(octx)
				return nil
			})
		}
	}

	done := make(chan struct{})
	reachable := make(chan struct{})
	obs.Go(octx, "pipeline.wait", func() error {
		decideWg.Wait()
		q.close()
		// decide finishing only means no *new* work will be enqueued — a large
		// backlog can still be draining, and the tuner should keep ramping
		// through it. Only once the queue is actually drained (waitDrained,
		// mirroring the same "nothing left" state pop signals via ok=false) do we
		// stop the tuner from opening more channels: pipelineDone and every
		// chWg.Add(1) are both under chMu, so no Add can start after this point
		// (see addChannel).
		q.waitDrained(rootCtx)
		s.chMu.Lock()
		s.pipelineDone = true
		s.chMu.Unlock()
		close(reachable)
		s.chWg.Wait()
		close(done)
		return nil
	})

	// The drain watchdog (§13.7) bounds only the tail: once termination is
	// reachable (every group decided, need queue empty), every issued
	// request_id terminating within DrainTimeout is required, or it's a fatal
	// accounting defect (E9002). Reaching "reachable" is real transfer time
	// and is not itself bounded by DrainTimeout — only once the root context
	// is separately cancelled (signal, another fatal error) does
	// workerStopGrace bound how long Run waits for the decide/fetch
	// goroutines to unwind.
	select {
	case <-reachable:
		select {
		case <-done:
		case <-time.After(cfg.DrainTimeout):
			s.fail(fault.New(fault.E9002, "drain watchdog", "", nil))
			waitGrace(ctx, done, workerStopGrace)
		case <-rootCtx.Done():
			waitGrace(ctx, done, workerStopGrace)
		}
	case <-rootCtx.Done():
		waitGrace(ctx, reachable, workerStopGrace)
	}

	if s.err() != nil {
		return finish()
	}

	// 10. Dry-run: report and stop. There is no clean protocol shutdown because
	// the sender is waiting for FILE_REQUESTs that will never come (see follow-ups).
	if cfg.DryRun {
		obs.Info(ctx, "dry run complete; nothing was written",
			obs.F("would_transfer", s.cnt.FilesTransferred.Load()))
		return finish()
	}

	// 11. Reconcile with the sender's SESSION_SUMMARY (T-PROTO-06).
	var ss *wire.SessionSummary
	select {
	case ss = <-summaryCh:
	case <-rootCtx.Done():
		return finish()
	case <-time.After(cfg.DrainTimeout):
		return fail(fault.New(fault.E9002, "await SESSION_SUMMARY", "", nil))
	}

	dec.applyDirMeta(octx)

	comp := s.completionDigest()
	ackMsg := &wire.SessionSummaryAck{
		FilesTotal:       scComplete.TotalFiles,
		FilesTransferred: uint64(s.cnt.FilesTransferred.Load()),
		FilesSkipped:     uint64(s.cnt.FilesSkipped.Load()),
		FilesFailed:      uint64(s.cnt.FilesFailed.Load()),
		BytesTransferred: uint64(s.cnt.Bytes.Load()),
		GroupsTotal:      scComplete.TotalGroups,
		CompletionDigest: comp[:],
	}
	if err := ctrl.SendMsg(ackMsg); err != nil {
		return fail(err)
	}

	if !bytes.Equal(ss.CompletionDigest, comp[:]) {
		obs.Error(ctx, "completion digests disagree",
			obs.F("code", string(fault.E5005)),
			obs.F("sender_transferred", ss.FilesTransferred),
			obs.F("receiver_transferred", ackMsg.FilesTransferred))
		return fail(fault.New(fault.E5005, "reconcile completion digest", "", nil))
	}

	if journal != nil {
		if cErr := journal.Clear(); cErr != nil {
			obs.LogFault(ctx, cErr)
		}
		journal = nil
	}
	obs.Info(ctx, "transfer complete",
		obs.F("files", ackMsg.FilesTransferred), obs.F("bytes", ackMsg.BytesTransferred))
	return finish()
}

func (s *session) summary(sc *wire.ScanComplete) Summary {
	c := s.cnt
	sum := Summary{
		FilesTransferred: u64(c.FilesTransferred.Load()),
		FilesSkipped:     u64(c.FilesSkipped.Load()),
		FilesFailed:      u64(c.FilesFailed.Load()),
		FilesRejected:    u64(c.FilesRejected.Load()),
		BytesTransferred: u64(c.Bytes.Load()),
	}
	if sc != nil {
		sum.FilesTotal = sc.TotalFiles
		sum.GroupsTotal = sc.TotalGroups
	}
	return sum
}

func u64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func (s *session) dialData(ctx obs.Ctx, ep paircode.Endpoint, sess *handshake.Session, n int) ([]*channel.Conn, error) {
	s.addr = netip.AddrPortFrom(ep.Addr, ep.Port).String()
	s.hsess = sess
	conns := make([]*channel.Conn, 0, n)
	closeAll := func() {
		for _, c := range conns {
			c.Close()
		}
	}
	for id := 1; id <= n; id++ {
		c, err := s.dialOneData(ctx, uint8(id))
		if err != nil {
			closeAll()
			return nil, err
		}
		conns = append(conns, c)
	}
	s.nextChID = uint8(n)
	return conns, nil
}

// applyDirMeta stamps directory mode+mtime once, deepest-first, after every file
// has been published (child creation would otherwise bump a parent's mtime,
// ARCHITECTURE §12.3).
func (d *decider) applyDirMeta(octx obs.Ctx) {
	d.dirMu.Lock()
	recs := make(map[string]dirRec, len(d.dirMeta))
	rels := make([]string, 0, len(d.dirMeta))
	for r, v := range d.dirMeta {
		recs[r] = v
		rels = append(rels, r)
	}
	d.dirMu.Unlock()
	sort.Slice(rels, func(i, j int) bool {
		return strings.Count(rels[i], string(filepath.Separator)) > strings.Count(rels[j], string(filepath.Separator))
	})
	for _, r := range rels {
		if err := d.dest.ApplyMeta(r, recs[r].mode, recs[r].mtime); err != nil {
			obs.LogFault(octx, err)
		}
	}
}

func orderCandidates(in []paircode.Endpoint) []paircode.Endpoint {
	out := append([]paircode.Endpoint(nil), in...)
	sort.SliceStable(out, func(i, j int) bool { return candRank(out[i]) < candRank(out[j]) })
	return out
}

func candRank(e paircode.Endpoint) int {
	a := e.Addr
	switch {
	case a.IsLoopback():
		return 0
	case a.Is4() && a.IsPrivate():
		return 1
	case a.Is4():
		return 2
	case a.IsPrivate():
		return 3
	default:
		return 4
	}
}

func authCodeFromTable(tab []channel.EndpointErr) fault.Code {
	for _, e := range tab {
		switch fault.GetCode(e.Err) {
		case fault.E4001:
			return fault.E4001
		case fault.E4002:
			return fault.E4002
		case fault.E2003:
			return fault.E2003
		}
	}
	return ""
}

func transportControl(err error) error {
	if fault.GetCode(err) != "" {
		return err
	}
	return fault.Wrap(fault.E3004, "control channel transport", "", err)
}

func peerError(e *wire.Error) error {
	code := fault.Code("E" + pad4(e.Code))
	msg := e.Message
	if msg == "" {
		msg = "sender reported a fatal error"
	}
	return fault.Newf(code, "sender reported an error", "", nil, "%s", msg)
}

func resolveDest(cfg Config, rootName string) (string, error) {
	d := cfg.Dest
	if d == "" {
		base := filepath.Base(filepath.Clean(filepath.FromSlash(rootName)))
		if base == "" || base == "." || base == string(filepath.Separator) {
			base = "esync-received"
		}
		d = base
	}
	abs, err := filepath.Abs(d)
	if err != nil {
		return "", fault.Wrap(fault.E1006, "resolve destination path", d, err)
	}
	return abs, nil
}

func checkWritable(dir string) error {
	probe := filepath.Join(dir, ".esync-write-test")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fault.Wrap(fault.E1006, "verify destination is writable", dir, err)
	}
	_ = f.Close()
	_ = os.Remove(probe)
	return nil
}

func resumeCommand(destPath string) string {
	return fmt.Sprintf("esync --link <new code from the sender> --dest %s", destPath)
}
