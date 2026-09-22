# esync — Working Memory

This file is the project's durable memory across sessions. Two parts:

- **MFRs** (Memorised Failure Rules): one rule per bug ever fixed, so it is never
  reintroduced. Each MFR has an id, the rule, and the one-line reason. Review every
  MFR before coding (AGENTS.md Pre-Commit Checklist).
- **Session Log**: one dated entry per working session, newest first.

Keep this file under 400 lines. When it grows past that, prune the oldest resolved
session-log entries (never prune MFRs).

---

## MFRs

MFR-0001 · receiver/decide · When materialising a directory from a GROUP_MANIFEST,
create it mode 0o755 and defer the manifest mode+mtime to the end-of-session
`applyDirMeta` pass (deepest-first) — a manifest dir with a read-only mode
(e.g. 0o555/0o444) applied at creation time makes `PublishPart`'s rename of the
first child fail with E7002 "permission denied".

MFR-0002 · receiver/tune+queue · Gate a dynamically-grown pool's "stop calling
`chWg.Add`" flag on the work queue actually being drained (`needQueue.waitDrained`:
closed, empty, zero in-flight) — not on the earlier, weaker signal that no *new*
work will be enqueued (`decideWg.Wait()` / `q.close()` returning). Gating on the
weaker signal made the §13.4 adaptive tuner unable to ramp past its initial
channel count on any transfer where decide finishes before fetch does (the
normal case: decide is fast metadata work, fetch is the slow I/O-bound part) —
`addChannel` silently returned `errPipelineDone` for the rest of the session,
with no error surfaced, so the bug only showed up as "the tuner never ramps"
under a real timed transfer, never in a build/vet/pure-unit-test pass.

MFR-0003 · receiver/run · Start the `--drain-timeout` (E9002) watchdog only
once termination is *reachable* (`decideWg.Wait()` + `q.close()` +
`q.waitDrained()` — every group decided, need queue empty), never from when
the fetch pipeline is first launched. (Originally this rule cited
`sender/state.go`'s `tracker.wait` as the model to mirror; that was wrong —
the sender cannot compute reachability at all. See MFR-0004.) Arming it at
pipeline start makes
`DrainTimeout` (default 60s) a cap on the *whole transfer*, not on the tail
described by ARCHITECTURE §13.7 ("once reachable, terminate within
drain-timeout") — any real transfer whose fetch phase runs past 60s (trivial
on a multi-GB tree) hits `E9002 await SESSION_SUMMARY` mid-transfer and gets
killed, even though it was still making steady progress (confirmed from a
real 43GB/176k-file run: `group.decide` spans kept completing for ~60s after
the watchdog fired). Once the root context is cancelled for some other
reason, `workerStopGrace` still bounds how long Run waits for the
decide/fetch goroutines to unwind — see `waitGrace`.

MFR-0004 · sender/state · Never treat `groups_decided == total_groups` as the
moment the sender's termination becomes *reachable*, and never arm a deadline
on it. The receiver sends `GROUP_DECISION` at the head of `decideGroup`,
*before* the `pushGroup` backpressure point and long before those files are
requested — so under load the last decision can precede the last
`FILE_REQUEST` by many minutes (observed: ~20 min, with `group.decide` spans
of 1.1–1.4e6 ms for groups 53/56/84/186 sitting blocked in `pushGroup`). The
sender has no signal for "the receiver will issue no more requests" — there is
no such R→S message in §9.2 — so `tracker.wait` bounds *inactivity* instead:
after `allDecidedCh` it samples the monotonic `tracker.progress` counter
(bumped by `reqStarted`/`reqCompleted`/`reqFailed`/`reqCancelled` and once per
streamed chunk in `serve`) and fires E9002 only after a whole `--drain-timeout`
with no movement. Bumping per *chunk* matters as much as per request: a single
large file in the tail streams for minutes with no request-level event, and a
request-only counter would kill it. The pre-fix deadline killed a healthy
43GB/176k-file run at exactly 60s after the final decision burst (13GB of 43GB
transferred, sender E9002 at 14:44:10 vs last decisions at 14:43:12), which
the operator saw as "an accounting defect; report with the log" — E9002 must
mean *stalled*, never merely *slow*.

MFR-0005 · channel/receiver-fetch · Every data-channel receive in the fetcher
(`fetchOne`'s FILE_HEADER wait and its FILE_CHUNK loop) MUST go through
`Conn.RecvMsgTimeout(cfg.StallTimeout)`, never bare `RecvMsg()`. Data channels
carry no keepalive (only the control channel does), so a sender that accepts a
FILE_REQUEST and then goes silent — process wedged, asymmetric partition, host
half-open — parks `RecvMsg` on `ReadFrame` forever while the control keepalive
stays happy (a wedged sender still answers PING). The receiver then hangs with
no error and no journal progress until the operator kills it. Expiry is E3005
(fatal, non-retryable — retrying a dead conn just burns MaxRetries ×
StallTimeout per file); the session ends, the journal is retained, a re-run
resumes.

MFR-0006 · plan/digest · The group-packing byte target (`--group-bytes`,
`plan.Options.GroupBytes`) MUST NOT be folded into `computeDigest`. Grouping is
transport tuning only; the manifest digest is the resume/peer-compat identity.
Folding it would force a full re-transfer of an in-progress job whenever the
operator retuned the group size (or the default changed between builds). The
digest still folds a `u32` slot where `group_size` used to live — now a literal
`0` — so on-disk journals from protocol v1 are invalidated by the version bump,
not by this field.

MFR-0007 · sender/receiver run · Both `sender.Run` and `receiver.Run` MUST
instantiate `obs.NewProgress` + `prog.Feed(...)` (inside `if !cfg.DryRun` on the
receiver) with `defer prog.Stop()`, next to the heartbeat. The renderer existed
in `internal/obs` from the first commit but neither run path ever created one,
so `--progress-interval` did nothing and no progress line was ever shown — the
operator's repeated "the progress bar still does not show". The heartbeat (a
log-sink mechanism) is not a substitute: it does not write the stderr progress
sink REQ-CLI-006/007 require.

MFR-0008 · main/signal · `sender.Run`/`receiver.Run` each `signal.Notify` for
SIGINT+SIGTERM and handle the first gracefully — which also disables the Go
runtime's default die-on-SIGTERM. Every later signal, and plain `kill`, is then
swallowed by the buffered channel, so only SIGKILL ends a wedged process. Keep
`cli.go`'s `installSignalEscape` (both run paths, fed by `--shutdown-grace`,
default 5s): after the first signal it exits 5 on a second signal or once grace
elapses (§14.7 / §14.4). Also: `receiver.Run`'s `signal.watch` MUST `ctrl.Close()`
after `s.fail` — `control.reader` is parked in `ctrl.RecvMsg()` with no rootCtx
awareness and otherwise stalls the graceful drain for the full `workerStopGrace`.

MFR-0009 · plan/Build · Excluded directories must PRUNE their whole subtree, not
just drop the directory entry. sys-set and user-filter matches on a directory
mean its contents are never seen: no file ids, no digest contribution, no
per-child exclusion record or count, and a later (even `+`) rule can never
re-admit a path beneath them. Implement as two passes over the entry set —
classify every entry (sys set first), collect the pruned directory prefixes,
then drop anything beneath one — so the outcome stays a pure function of the
entry set (P-SCAN-01 order independence holds when a child precedes its parent
in the input). A per-entry-only exclusion (the pre-fix behaviour) silently
transfers the contents of `.Trash-1000`, `__MACOSX`, `lost+found` and of every
`--exclude`d directory, pollutes the manifest digest and the resume-journal
identity (R-03).

MFR-0010 · receiver-fetch/sender-servicer · NEVER map a raw data-channel transport
error to a session-fatal E3005. A data channel that closes or errors mid-file is
recoverable per ARCHITECTURE §7.3 row 1: requeue the in-flight file (do not
fail/count it), end the channel worker, and rejoin with a fresh CHANNEL_JOIN
(fresh channel id = fresh per-channel keys + seq; never reuse crypto state
across physical connections). The sender must tolerate a servicer that dies
with its connection — log, close the socket, and keep `join.accept` answering
rejoins — and must drop the dead request WITHOUT resolving the file
(`tracker.reqFailed(reqID, resolve=false)`), or its in-flight accounting leaks.
Only a stall (per-receive deadline, half-open sender), a coded channel/record
fault, or the LAST channel retiring with work still queued is E3005 (R-24).

MFR-0014 · receiver/fetch · An item-class error raised BETWEEN the FILE_HEADER and
a terminal message (FILE_COMPLETE/FILE_ERROR) leaves the dead request's unread
FILE_CHUNKs on the wire. The channel is desynced and MUST NOT serve another
request: reusing it reads that tail as the next request's FILE_HEADER →
E5001 "unexpected message" → session-fatal. So a transient E7004 destination
write error — the one code the catalogue marks "retried" — currently cannot
retry, it kills the session. Tag such errors (`desynced`, which WRAPS the cause
so GetCode/IsFatal/IsRetryable still see it), give the item its normal fate
(retry with backoff, or fail once the budget is spent), then end the worker and
rejoin (§7.3 row 1). Do NOT reuse `channelLost` for this: it discards the
attempt count, so a persistent local write failure would rejoin forever instead
of failing. FILE_CANCEL cannot help — the receiver never sends it, and `serve()`
runs synchronously inside `run()` so the sender cannot read it mid-stream.

MFR-0015 · digest/cache · The digest cache key carries ctime at FULL timespec
resolution (`CtimeSec` + `CtimeNsec`). Second granularity silently breaks the
very guarantee ctime is there for: a rewrite preserving size and mtime that
lands in the same wall-clock second as the previous ctime is a stale HIT and the
receiver skips a file that changed. The cache is advisory for speed but
authoritative for the skip decision, so the key must be exact. Any change to
`CacheKey` or its codec must bump `cacheMagic` (now v2) so stale files are
discarded by the existing version check.

MFR-0011 · receiver/fetch loop · When a fetch ends because the worker's context
was cancelled, REQUEUE the item — never `q.done()` it. The adaptive tuner's
`retireOne` (§13.4) cancels one worker's ctx while the session keeps running, so
"ctx cancelled" does NOT mean "session over": `done()` there retires a file that
was never fetched (a silent hole in the destination on an otherwise exit-0 run),
counts it toward `waitDrained`, and swallows a fatal that another worker would
have reported. This holds at BOTH cancel sites — the post-`fetchOne` switch and
the `<-ctx.Done()` arm of the retry backoff (there the attempt is consumed first,
`it.attempt++`, because the retry was already decided and counted). Keep the
`ctx.Err() != nil` case ahead of the error classification: on a real shutdown the
conn closes and the resulting transport error would otherwise be reported as a
spurious E3005 fatal on a clean Ctrl-C.

MFR-0012 · sender/acceptData · A listener deadline bounds `Accept`, NOT the
`CHANNEL_JOIN` read behind it. Always `SetDeadline(now+HandshakeTimeout)` on the
accepted conn before `handshake.AcceptChannel` and clear it after — without it
any host on the LAN can connect, stay silent, and park `io.ReadFull` on a
still-unauthenticated socket for the rest of the session, stalling the accept
loop (REQ-NET-010: no code path may wait forever). The receiver's `dialOneData`
comment claiming the sender already bounds its accept window was wrong.

MFR-0013 · sender/serve · `serve()` reopens the file BY PATH, long after the
scan. Verify identity on the open fd (`Dev`/`Ino` from the plan entry, guarded by
`e.Ino != 0` for platforms without stat support) plus `Mode().IsRegular()` BEFORE
the first byte is sent. The end-of-stream manifest-digest check is not a
substitute: it only fires once the content is already on the wire, so a symlink
swapped in after the scan (same size, pointing outside the root) is exfiltrated
in full and only then reported. Do NOT reach for `os.Root` here — `--follow-symlinks`
walk mode legitimately serves content through symlinks that escape the root.

---

MFR-0016 · sender/receiver run · BOTH sides must tell the peer why a session
  died. The receiver used to return from every fatal path without sending
  anything, so the sender saw a bare EOF on the control channel, treated it as a
  clean end of session, and exited 0 printing "ok" while the destination was
  broken (disk full, stall, protocol fault). Any new fatal path on either side
  must route through that side's notify-peer guard — send-once, skipped for a
  signal interrupt, best-effort so it can never block shutdown or become a second
  fatal. §14.4 / REQ-ERR-005: both sides log the same cause. Beware when testing
  this: a wrong-reason fatal (e.g. the generic E5001 "unexpected control message")
  produces the same non-zero exit, so a test MUST assert the peer's actual E-code
  reached the log, not merely that the exit was non-zero.

MFR-0017 · plan/wire/record · Any control message assembled from an UNBOUNDED
  collection must be capped at build time against the record ceiling, not merely
  documented. GROUP_MANIFEST was capped by entry count and by *file* bytes, never
  by its own encoded size, so a deep or vendored tree produced a record above the
  ceiling, the sender died with E5003, and the resume journal replayed the same
  group forever — a deterministic, workaround-free failure on legal input. Cap by
  the real encoder's per-entry size, and when a field's true value is not known at
  build time (digests are hashed lazily, after grouping) use the wire's maximum so
  the estimate can only OVERestimate. An underestimate re-opens the bug. Keep the
  ceiling single-sourced: sender and receiver deriving it separately is a wire
  break waiting to happen.

MFR-0018 · receiver/hardlink · A hard-link secondary must NEVER depend solely on
  its primary publishing. Secondaries were parked until the primary published, so
  a primary resolved by SKIP left them parked forever — absent from the
  destination, journal clean, exit 0. A failed link was likewise warn-only. Every
  path that resolves a primary must materialise its parked secondaries, and any
  link failure must fall back to fetching the secondary's content (§12.4 /
  REQ-FS-003), with a FilesFailed increment as the last-resort safety net. The
  rule generalises: warn-only is never an acceptable outcome for a file the plan
  says belongs in the destination.

MFR-0019 · receiver/queue · NEVER call a capacity-blocking push from the
  goroutine that pops the queue. The hard-link content fallback (MFR-0018) first
  used `pushGroup`, which blocks while the queue is full, from inside `finish()`
  — which runs on a fetcher worker, i.e. a popper. With the queue at capacity and
  every worker in that path, nothing was left to pop, so no slot could ever free:
  the drain deadlocked until the watchdog fired a misleading E9002 ("stalled" for
  a session that was not). Enqueue paths reachable from a worker use
  `pushFallback`, which never blocks and overshoots capacity by a bounded amount.
  Blocking backpressure is correct only on the decide side, which is not a popper.

MFR-0020 · sender/run · A count that is supposed to affect the EXIT CODE must be
  threaded to the exit decision, not just into a progress message. Walk-skipped
  entries (E6005/6/7) were counted by the walker and reported in SCAN_COMPLETE,
  then dropped: they never reached SESSION_SUMMARY and the exit switch keyed on
  failures alone, so an unreadable subtree exited 0. REQ-SCAN-032 / §14.6. When
  testing an exit code, assert the *reason* too (here: outcome "partial" with
  FilesFailed == 0), or a failure-driven exit 1 will masquerade as the case under
  test.

## Session Log

### 2026-09-23 — owner-directed fix pass (R-02/12/13/16/19/22/23)

- Seven owner-selected findings fixed by four Sonnet agents in one worktree,
  partitioned by strict file ownership, then reviewed by an Opus pass.
  MFR-0016..0020 record the five that are bug *rules*; R-22 (--version reports
  the wire protocol) and R-19 (--compact-code) are feature gaps, not rules.
- Two new findings surfaced from the fixes themselves and are in
  STATUS_SUMMARY.md: **R-42**, a drain deadlock the R-16 fallback introduced
  (fixed here, MFR-0019); **R-43**, a test-only mutable global A4 added to
  cli.go, refactored away into a pure `senderConfig(args)` builder — the
  resulting test is strictly stronger, since an unregistered flag now surfaces
  as a usage exit instead of being silently ignored.
- Process note that paid off twice: require every fix to carry a test proven to
  FAIL on the pre-fix tree, and capture that failure verbatim. It caught R-42
  (routing pushFallback back through pushGroup hangs the test at its 2s guard)
  and the R-43 seam. Decline to fabricate a proof only when the behaviour did
  not exist in any form before (TestCompactEndpoints).
- Remaining work, ROI-ordered, is in `ROI-fixes-Review-20260922.md`: Tier A
  highest ROI (CI gap R-01, key zeroing R-21, statfs fail-open, drain watchdog,
  failed-item list R-08, dry-run writes R-10), Tier B real bugs (`.esync`
  case-fold is the last data-destruction path), Tier C doc integrity, Tier D
  deferred features, plus six owner decisions.

### 2026-09-22 — bug-hunt tier 1 (condensed)

- 4-agent Sonnet bug hunt at HEAD `d9177b3`, ROI-ranked, independently
  re-verified in source. Five findings, all five fixed this session:
  cancelled-worker data loss (MFR-0011), mid-stream desync (MFR-0014),
  unbounded CHANNEL_JOIN read (MFR-0012), `serve()` reopen-by-path TOCTOU
  (MFR-0013), digest-cache ctime granularity (MFR-0015, a spec erratum).
- All five were invisible to the suite; every new test was proven to fail on the
  pre-fix tree. The `serve` test's pre-fix failure is a `FILE_HEADER` — direct
  evidence the outside file was already being streamed.
- Two reusable test levers found here: a stub server that cancels *before*
  answering (no sleep race, since the fetcher is parked in the receive), and
  occupying `.esync/parts/<id>.part` with a **directory** to force a
  deterministic mid-stream EISDIR → E7001 (item-class, retryable) without any
  I/O fault injection.
- Gate: `make ci` green, coverage 85.4 % (from 84.7 %).

### 2026-09-08 — audit-driven hardening (condensed)

- **R-03 — excluded directories now prune their whole subtree (MFR-0009).**
  `plan.Build` recorded `Exclusion.Prune` but never dropped an excluded
  directory's contents, so children took file ids, polluted the manifest digest
  and resume identity, and were transferred. Fixed with a two-pass Build
  (classify → collect pruned prefixes → drop anything beneath one); subtree
  drops are silent, dominate any rule, and are order-independent (P-SCAN-01).
  Tests in `internal/plan/prune_test.go`. Commit `75fbff4`.
- **R-24 — a lost data channel is recoverable, not fatal (MFR-0010).** Raw
  transport errors (no E-code) classify as recoverable loss: the in-flight file
  is requeued (never failed or counted) and `channel.rejoin` re-dials with a
  fresh CHANNEL_JOIN (0.5/2/8 s; fresh id = fresh keys + seq). Exhaustion
  retires the channel; the last one retiring with work queued is E3005. Stalls
  and coded record/auth faults stay fatal. Sender side: a servicer transport
  error closes only its conn, and a request dying with its channel is dropped
  unresolved so the tracker never leaks a slot. Tests in
  `internal/receiver/rejoin_test.go` (real TCP + handshake + records). `a6d5c6f`.
- **SESSION_RESUME — design delivered, code awaits sign-off.** The doc
  contradicts itself here (E3004 vs E3007 on expiry; "fresh handshake" vs the
  single-use code; 0x70 body and `LastSeqSeen` undefined), so implementing blind
  would guess at a security-critical protocol. `doc/SESSION_RESUME_DESIGN.md`
  pins one contract and asks the operator to confirm D1–D4 first.

### 2026-09-08 — Ctrl-C not handled: signal force-exit backstop (MFR-0008, condensed)

- Operator: "ctrl-c does not get handled; I have to kill the processes and get
  <defunct> in linux."
- Root cause: both `Run`s `signal.Notify` SIGINT+SIGTERM and cancel gracefully on
  the first, which traps every later signal too (and SIGTERM, so plain `kill` is
  inert). `--shutdown-grace` had been a parsed-only flag since 2026-09-04.
- Fix (`cli.go`): `signalEscape` parks until the first signal (Run owns that
  one), then exits 5 on a second signal or on grace elapsing; wired into both run
  paths. Also `receiver/run.go`: `signal.watch` now closes the control conn after
  `s.fail(ErrSignal)`, because `control.reader` blocks in `ctrl.RecvMsg()` with no
  rootCtx awareness and previously waited out the full 5 s worker grace on every
  Ctrl-C.
- Tests: `cli_test.go` `TestSignalEscape{SecondSignal,GraceTimeout,NoSignal}`.

### 2026-09-07 — grouping + reliability bundle (condensed)

- One bundled batch from four operator complaints after a failed ~43GB resume,
  carrying a **protocol version bump 1→2** (handshake, wire, plan digest,
  journal `sessionInfo`): v1 journals are invalidated and cross-version pairing
  rejected — the operator accepted this.
- (1) `obs.Progress` was never instantiated by either run path (MFR-0007);
  added `Progress.Feed`, a smoothed rate + eta, and wiring in both `Run`s.
  Receiver total is a running sum from `decideGroup` (needed entries only);
  the sender renders sent-only, as it cannot know receiver skips.
- (2) Bogus E9002: control-loss before SESSION_SUMMARY is now a failure, and
  **stall detection (MFR-0005)** arrived as `Conn.RecvMsgTimeout(d)` +
  `--stall-timeout` (default 60s) on both data-channel receives. This also
  fixed (3), the half-written journal that broke clean re-runs.
- (4) Size-based grouping (MFR-0006): `computeGroups` opens a new group at
  ≥1024 entries or when `--group-bytes` (default 512 MiB) would be exceeded;
  oversize single file = its own group, entries never split.
  `SessionParams.GroupSize u32` → `GroupBytes u64` (advisory), digest slot
  literal `0`, `--group-bytes` deliberately NOT folded into the plan digest.
- FILE_CANCEL was considered here and deliberately **WONTFIX** as
  disproportionate — see MFR-0014, where its absence turned out to matter.

### 2026-09-07 — sender E9002 drain watchdog: deadline → inactivity
- Fully captured by MFR-0004: a 43GB/176k-file run died E9002 with 13GB sent
  because the watchdog armed on `allDecidedCh` while decide ran ~20 min ahead of
  fetch. Fixed by sampling `tracker.progress` instead.

### 2026-09-07 — 60s groups/channels/bandwidth heartbeat (condensed)

- `internal/obs/heartbeat.go` samples `Counters().Bytes` every 10s and logs
  `groups=… channels=… rate=…` at INFO every 60s. `Stop()` is synchronous (a
  `done` channel the loop closes) so callers can read shared state afterwards
  without a `-race` hazard. Group ids: `needQueue.pending` (receiver, hence
  `done(groupID)`) and `tracker.inProgressGroups()` (sender); channels from a
  new `activeChannels atomic.Int64`.

### 2026-09-07 — Fix: receiver drain watchdog fires mid-transfer (E9002)
- Fully captured by MFR-0003: the receiver armed `DrainTimeout +
  workerStopGrace` from pipeline launch, killing any fetch phase longer than
  60s. Fixed by arming only once termination is reachable (`decideWg.Wait()` /
  `q.close()` / `q.waitDrained()`), with `waitGrace` bounding the unwind.

### 2026-09-04 — Adaptive channel tuner (REQ-PAR-004) + sender-side dynamic join (condensed)

- Built the real §13.4 controller: `decideRamp` (pure, table-tested) + a 2s
  sampler over `cnt.Bytes`/`Retries`/`FilesFailed`; +2 channels on >8% goodput
  gain, -1 on >8% regression (never the last channel — that path is fatal
  E3005), stop after 3 unproductive ramps (RISK-05). `--channels` sets
  `Config.ChannelsPinned` and disables the tuner; unpinned starts at
  `--min-channels` (4).
- `session` gained a dynamic pool (`chMu` + `workers` + `nextChID` +
  `pipelineDone` + `chWg`); `pipelineDone` is the WaitGroup `Add`-vs-`Wait` race
  guard, and it must flip on the queue being *drained*, not merely closed — see
  MFR-0002. Sender keeps its listener open and accepts late CHANNEL_JOINs up to
  `cfg.MaxChannels` in a `join.accept` goroutine, or ramp-up is a no-op against
  a real sender.
- Caveat retained: which channel `retireOne` picks and whether a ramp-down
  counts as "unproductive" are my reading of an underspecified §13.4 pseudocode
  (ramp-up resets the streak, ramp-down advances it) — worth a sanity-check if
  real-world tuning ever looks wrong.
- Test-harness note: `fakeSender` has a late-join accept loop
  (`joinedCount()`) and `chunkDelay`/`chunkSize` pacing so a transfer can span
  several tuning windows cheaply; pre-existing E2E tests now pass
  `ChannelsPinned: true` (unpinned silently ignored their `Channels:` value).

### 2026-09-04 — earlier sessions (pruned)

- Entries for the initial vertical-slice implementation, the `internal/receiver`
  package build, and the CLI-wiring/E2E/coverage-gate session were pruned to keep
  this file under 400 lines. Their MFRs (MFR-0001) and follow-ups are retained
  above / below. See git history for detail.
- Retained from the CLI-wiring session: `cover-check` passes `-coverpkg=./...` so
  the root e2e test's coverage spreads to every package it exercises (hence low
  isolated per-package numbers for `sender`/`channel`/root but a passing total).
  Test-harness trap: a test driving both `Run`s in one process MUST give each its
  own `obs.Init` — a shared `obs.Ctx` double-counts via the live `obs.Counters`.
