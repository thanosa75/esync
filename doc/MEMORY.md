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

## Session Log

### 2026-09-22 — bug-hunt tier 1: cancelled-worker data loss, unbounded join read, serve TOCTOU

- 4-agent bug hunt (Sonnet) against HEAD `d9177b3` + independent source
  verification; findings ranked by ROI. The top 5 were: (1) cancelled fetch
  worker drops files, (2) E7004 retry desyncs the data channel, (3) unbounded
  CHANNEL_JOIN read, (4) serve() reopen-by-path TOCTOU, (5) digest-cache ctime
  second-granularity. This session fixed 1/3/4 (MFR-0011/0012/0013); 2 and 5 are
  deferred for a design/spec decision (see follow-ups).
- All three were invisible to the suite: every new test fails on the pre-fix
  tree for exactly the intended reason (verified by stashing the fixes), and the
  serve test's pre-fix failure is a `FILE_HEADER` — i.e. the outside content was
  already being streamed.
- New tests: `receiver/fetch_test.go` `TestFetchCancelledMidFetchRequeues`,
  `TestFetchCancelledDuringBackoffRequeues` (the stub server cancels *before*
  answering, so the fetcher is guaranteed to observe a cancelled ctx — no sleep
  race; the backoff case pushes `attempt: 1` so the un-jittered `rnd == nil`
  delay is 400ms, a wide cancel window). `sender/run_test.go`
  `TestAcceptDataBoundsChannelJoinRead` (a silent dialer; the listener is closed
  500ms in — only a bounded join read lets `acceptData` get back to `Accept` and
  notice). `sender/serve_test.go` `TestServeRejectsSwappedFileIdentity`
  (same-size symlink to an outside file swapped in post-scan → E6003 before any
  chunk).
- Full gate green: `make ci` (tidy-check, fmt-check, vet, check-goroutines,
  cover-check) — coverage 85.2%, up from 84.7%.
- Follow-ups added: **(a)** item 2 — a retryable E7004 aborts the chunk loop
  mid-file while the worker keeps the channel, so the remaining FILE_CHUNKs of
  the dead request are read as the *next* request's data. E7004 is the ONLY
  item-class retryable that does this (E5001/E5006/E7003 are fatal and tear the
  session down), i.e. the one error the catalogue marks retryable is exactly the
  one that breaks the protocol. Needs a design call: drain-to-FILE_COMPLETE
  before requeue, or tag FILE_CHUNK with the request id and discard stale ones.
  **(b)** item 5 — `digest.CacheKey` has `CtimeSec` and no `CtimeNsec`, so a
  sub-second mtime-preserving rewrite reuses a stale digest; ARCHITECTURE.md:1022
  specifies that key, so fixing it is a spec change, not just a code change.
  **(c)** R-01 — `.github/workflows/build.yml` runs only `make dist`: no test,
  race, vet, or coverage job, and it publishes a rolling `latest` release on
  every push to main.
- **Follow-ups (a) and (b) were then decided and implemented in the same
  session.** (a) item 2 → option **B**, drop the channel (MFR-0014): `fetchOne`
  is a named-return with a `streaming` flag set at the FILE_HEADER and cleared
  at FILE_COMPLETE/FILE_ERROR, and a deferred wrap tags anything returned in
  that window as `desynced`; `loop` reads the tag, applies the item's normal
  fate, then returns `lostChannel=true`. Tests
  `TestFetchMidStreamErrorDropsChannel{,WhenRetriesExhausted}` force a
  deterministic mid-stream E7001 by occupying `.esync/parts/<id>.part` with a
  *directory* (EISDIR → item-class, retryable) — a reusable lever for any
  "destination write fails mid-file" test. (b) item 5 → fixed as a **spec
  erratum** (MFR-0015): `CtimeNsec` added to `CacheKey`, `keyLen` 49→57,
  `cacheMagic` v1→v2, both `statkey_linux.go` and `statkey_darwin.go`
  populated, and ARCHITECTURE §11.3 amended with a dated erratum block.
- Final gate after all five changes: `make ci` green, coverage 85.4%.
- Remaining known gaps, ROI-ranked, are in STATUS_SUMMARY.md; the live top
  three are R-01 (CI runs no tests at all), the sender's `--owner`/
  `--shutdown-grace`/`--spill-threshold` flags being parsed but not wired, and
  SESSION_RESUME being designed but not implemented.

### 2026-09-08 — audit-driven hardening: excluded-dir pruning (R-03) + data-channel-loss recovery (R-24)

- Re-audit (3 agents, HEAD `ae19178`) confirmed the STATUS_SUMMARY findings; this
  session resolved two of the three HIGH gaps the operator prioritised.
- **R-03 / gap 3 — excluded directories now prune their whole subtree (MFR-0009).**
  `plan.Build` classified per entry and recorded `Exclusion.Prune` but never
  dropped the contents of an excluded directory — children of `.Trash-1000`,
  `__MACOSX`, `lost+found` and of any `--exclude` dir survived into the plan,
  took file ids, polluted the manifest digest + resume identity, and were
  transferred. Fixed with a two-pass Build (classify all entries → collect pruned
  prefixes → drop anything beneath one); subtree drops are silent (no per-child
  record/count), dominating any rule, order-independent (P-SCAN-01 preserved;
  `TestPruneOrderIndependent` shuffles a child before its pruned parent). Tests:
  `internal/plan/prune_test.go` (sys-dir contents incl. deeper prefix rules,
  filter-excluded dir subtrees, digest equality with a never-walked tree).
  Commit 75fbff4.
- **R-24 / gap 2 — a lost data channel is recoverable, not fatal (MFR-0010).**
  Receiver: raw transport errors (EOF/socket death, no E-code) classify as
  recoverable channel loss — the in-flight file is requeued (never failed/
  counted) and the worker ends; a `channel.rejoin` goroutine re-dials with a
  fresh CHANNEL_JOIN (0.5/2/8 s), fresh id = fresh keys+seq; exhaustion retires
  the channel, and the last one retiring with work queued is E3005. Stall
  (RecvMsgTimeout) and coded record/auth faults stay fatal. `dialOneData` bounds
  the join round-trip. Sender: a servicer transport error logs + exits (conn
  closed) instead of killing the session; `join.accept` now runs for the whole
  session; a request that dies with its channel is dropped unresolved
  (`reqFailed(resolve=false)`) so the tracker never leaks an in-flight slot.
  E3005 catalogue/§14.2 wording synced (stall / last-retired / coded fault).
  Tests: `internal/receiver/rejoin_test.go` (real TCP + handshake + record
  layer): mid-file kill recovers with both files published and exit 0; idle
  loss recovers; rejoin exhaustion → E3005. Commit a6d5c6f.
- **Gap 1 (SESSION_RESUME suspend/re-dial) — design delivered, code awaits
  sign-off.** The doc is internally contradictory on this feature (E3004 vs
  E3007 for expiry; "fresh handshake" vs single-use code/RISK-04; 0x70 body and
  `LastSeqSeen` undefined). Implementing blind would guess at a security-
  critical protocol; instead `doc/SESSION_RESUME_DESIGN.md` pins one concrete
  contract (reuse ch-0 keys on the re-dialed conn, 0x70 as first frame,
  E3007-on-expiry, sender replay ring keyed by `LastSeqSeen`) and asks the
  operator to confirm D1–D4 before the implementation (change list items 1–7 in
  that doc).
- Gates: gofmt/vet/build clean, `go test -race ./...` all green, full-suite
  `-coverpkg` still passes (plan 93.5%, receiver 84.2% in isolation).

### 2026-09-08 — Ctrl-C not handled: signal force-exit backstop (MFR-0008)

- Operator: "ctrl-c does not get handled; I have to kill the processes and get
  <defunct> in linux."
- Root cause: both `Run`s `signal.Notify` SIGINT+SIGTERM and gracefully cancel on
  the first, which traps every later signal too (and SIGTERM, so plain `kill` is
  inert) — the buffered sig channel drops them. `--shutdown-grace` had existed as
  a flag since the 2026-09-04 CLI wiring but was never consumed. A slow/silent
  graceful stop then looks dead and only `kill -9` works.
- Fix (`cli.go`): `signalEscape(ctx, sigc, grace, exit)` parks until the first
  signal (Run owns that one), then exits 5 on a second signal or `grace`
  elapsing. `installSignalEscape` wires it to real signals + `flush()`+`os.Exit`,
  called in both `runSender`/`runReceiver` after `installFatalHandler`, fed by
  `*c.shutdownGrace`. `--shutdown-grace` added to `printUsage`.
- Also (`receiver/run.go`): `signal.watch` now `ctrl.Close()`s after
  `s.fail(ErrSignal)` — `control.reader` blocks in `ctrl.RecvMsg()` (no rootCtx
  awareness), so the decide/fetch pipeline previously waited out the full 5s
  `workerStopGrace` on every Ctrl-C. Sender's first-signal path was already
  prompt (`tracker.wait` selects on `ctx.Done`).
- Tests: `cli_test.go` — `TestSignalEscape{SecondSignal,GraceTimeout,NoSignal}`.
  `installSignalEscape` stays 0% like its sibling `installFatalHandler` (real
  `os.Exit`); root `esync` pkg at 73.2% is pre-existing (cli wiring is covered by
  the e2e test, not unit tests). `make cover-check` 84.7% (min 80%); gofmt/vet/
  build clean, `go test -race ./...` all green.
- Follow-up cleared: "`--shutdown-grace` parsed but not consumed" (2026-09-04).
  Still open: `--spill-threshold` parsed-only, `--owner` not wired into fetch,
  and T-SIG-01/F-SIG-01 (the traceability matrix expects a signal E2E test; the
  full first-signal → summary → exit-5 path is still only exercised end-to-end
  by hand, not in CI).

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
- Compressed: fully captured by MFR-0004 and the follow-up line in the
  grouping+reliability entry. Original: a 43GB/176k-file run died E9002 at
  ~14:44 with 13GB transferred; the watchdog was armed on `allDecidedCh` while
  decide ran ~20 min ahead of fetch. Fixed by sampling `tracker.progress`.

### 2026-09-07 — 60s groups/channels/bandwidth heartbeat (condensed)

- `internal/obs/heartbeat.go` samples `Counters().Bytes` every 10s and logs
  `groups=… channels=… rate=…` at INFO every 60s. `Stop()` is synchronous (a
  `done` channel the loop closes) so callers can read shared state afterwards
  without a `-race` hazard. Group ids come from `needQueue.pending` on the
  receiver (hence `done(groupID)`) and `tracker.inProgressGroups()` on the
  sender; channel count from a new `activeChannels atomic.Int64`.

### 2026-09-07 — Fix: receiver drain watchdog fires mid-transfer (E9002)
- Compressed: fully captured by MFR-0003. Original: the receiver armed
  `DrainTimeout + workerStopGrace` from pipeline launch, killing any fetch
  phase longer than 60s; fixed by arming only once termination is reachable
  (`decideWg.Wait()`/`q.close()`/`q.waitDrained()`), with `waitGrace` bounding
  the post-cancel unwind.

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
