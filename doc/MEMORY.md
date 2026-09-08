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

---

## Session Log

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

### 2026-09-07 — grouping + reliability bundle (progress bar, stall detection, size-based groups)

- One bundled batch from four operator complaints after a failed ~43GB resume.
  Carries a **protocol version bump 1→2** (handshake `protocolVersion`, wire
  `ProtocolVersion`, plan digest `protocolVersion`, journal `sessionInfo`), so v1
  journals are invalidated and cross-version pairing is rejected — the operator
  accepted this.
- **Fix #1 — progress bar (MFR-0007).** `obs.Progress` was never instantiated by
  either run path. Added `Progress.Feed(ctx, sample)` (samples live counters on
  the render interval — keeps the push-model API `TestProgressNoDeadlock`
  pins), taught `render()` to drop "/ total" when a total is 0 ("not known
  yet"), and added a smoothed rate + `eta` (REQ-CLI-006). Wired into both
  `Run`s next to the heartbeat with `defer prog.Stop()`; `ProgressInterval`
  added to both Configs, plumbed from `*c.progressInterval` in cli.go. Receiver
  total = running sum from `decider.decideGroup` (`session.neededBytes/
  neededFiles`, needed entries only, grows per group); sender renders sent-only
  (it can't know receiver skips).
- **Fix #2 — bogus E9002.** Split: (a) E9002 reworded (done earlier this
  session-series, MFR-0004); (b) control-loss before SESSION_SUMMARY now a
  failure; (c) **stall detection (MFR-0005)** — new `Conn.RecvMsgTimeout(d)`
  (per-receive read deadline, `net.Error` timeout → E3005), fetcher gained a
  `stall` field fed from new `--stall-timeout` (default 60s), used on both
  data-channel receives in `fetchOne`. (d) FILE_CANCEL protocol change —
  **WONTFIX**, disproportionate (see follow-ups).
- **Fix #3 — clean re-run.** Addressed by Fix #2c: the hang that left journals
  half-written is now a clean E3005 exit, so the next run's digest-match resume
  path sees a consistent journal and skips completed files ("no transfers").
- **Fix #4 — size-based grouping (MFR-0006).** `computeGroups` walks sorted
  entries once, opening a new group when the current one is non-empty AND (holds
  ≥1024 entries OR adding this file's bytes exceeds the `--group-bytes` target,
  default 512 MiB). Oversize single file = its own group; entries never split.
  `wire.SessionParams.GroupSize uint32` → `GroupBytes uint64` (advisory).
  `GroupManifest.FirstFileID` already carried boundaries, so `decide.go` needed
  zero changes. Digest `group_size u32` slot → literal `0`; `--group-bytes` not
  folded (MFR-0006). ARCHITECTURE §10.4/§10.5/§9.3/§16.2 and INITIAL_REQS
  REQ-SCAN-010/024, §4.2, CON-04, glossary rewritten.
- Full gate green: `gofmt` clean, `go vet ./...`, `go build ./...`,
  `go test -race ./...` all packages. Coverage on changed packages: obs 87.8%,
  receiver 84.3%, plan 92.8%, wire 97.0%, channel 77.6%, fault 96.4%.
  `internal/sender` shows 39.2% in isolation — pre-existing (`Run`/`walk.go` are
  covered by the root e2e test, not sender unit tests); the 6-line Progress
  wiring mirrors the untested heartbeat wiring from commit 8a2067a and is
  exercised by `TestE2ETransfer` (now runs with a 10ms `ProgressInterval`).
- Tests added: `channel.TestRecvMsgTimeout`, `receiver.TestFetchStallTimeout` +
  `TestFetchStallTimeoutMidStream`, `receiver.TestDecideAccumulatesNeededTotals`,
  `obs.TestProgressFeed` + `TestProgressRenderLine`, `plan.TestGroupingBySize`,
  updated `wire.TestGoldenWire` golden vector for the 8-byte `group_bytes`.
- Branch: `grouping-and-reliability` (session started on `main`).
- Follow-ups (new): **FILE_CANCEL** — when the receiver gives up on a file after
  its retry budget, it does not tell the sender, which keeps the request_id
  inflight; harmless today (drain watchdog + summary reconcile) but a clean
  R→S FILE_CANCEL + sender reconciliation would be tidier. Needs a wire message
  → deferred as not worth another protocol change in this batch. Also: the
  sender `drain` span still bills the whole transfer (from the prior entry).

### 2026-09-07 — sender E9002 drain watchdog: deadline → inactivity

- User brought logs from a 43GB/176k-file run that died with `E9002 drain
  watchdog` on the sender ("an accounting defect; report with the log"), only
  13GB transferred, "not all groups were transferred". Asked for the
  highest-ROI fix.
- Diagnosis: **false positive**, not an accounting leak. The `needed`/`resolved`
  bookkeeping was correct; the *arming condition* was wrong. `tracker.wait`
  started the 60s timer on `allDecidedCh` (`groups_decided == total_groups`),
  but the receiver sends `GROUP_DECISION` at the head of `decideGroup`
  (`decide.go:250`), before the `pushGroup` backpressure point (`:281`).
  Under load decide ran ~20 min ahead of fetch — four decide workers blocked
  inside `pushGroup` (spans `group=53` 1.16e6 ms, `84` 1.21e6, `56` 1.42e6,
  `186` 2.8e5, all `outcome=ok`). Sender saw all 204 decisions at ~14:43:12,
  armed 60s, expired 14:44:10 while the receiver was still fetching 4096
  freshly-enqueued files. Receiver's `E3005 EOF` was downstream fallout of the
  sender's teardown; the E7011 absolute-symlink rejections were unrelated.
- This is the sender-side sibling of MFR-0003 (receiver side, fixed earlier
  from the *same* logs). MFR-0003 cited `sender/state.go` as the correct
  model — amended, since the sender cannot compute reachability at all.
- Fix (MFR-0004): `tracker.progress atomic.Uint64`, bumped by the four request
  lifecycle methods and once per streamed chunk in `serve`; `tracker.wait`
  loops sampling it and fires E9002 only after a full `--drain-timeout` with
  zero movement. No wire change, no receiver change, no protocol version bump
  — chosen over adding an R→S "no more requests" message (correct but touches
  the wire format) and over gating on CREDIT count (shrinks the window but
  still breaks on a large tail). ARCHITECTURE §13.7 rewritten to state the two
  sides' differing arming rules.
- Tests: `TestTrackerDrainWatchdogIgnoresLiveTransfer` (work spanning 8 drain
  windows must not trip — fails deterministically pre-fix with the user's exact
  error) and `TestTrackerDrainWatchdogTripsOnStall` (in-flight request, then
  silence, still trips — the watchdog is not defanged).
- Noticed, not changed (Rule 3): the sender's `drain` span in `run.go:349`
  starts before `tr.wait` blocks on `allDecidedCh`, so it bills the entire
  transfer to "drain" (`dur_ms=1.6e6` in the report) — misleading name, worth
  splitting into `transfer` + `drain` in a future pass.

### 2026-09-07 — 60s groups/channels/bandwidth heartbeat

- User request: "every 60s you should emit which groups are in progress ...
  add also channels and bandwidth averaged on the 10s interval, in human
  readable". Clarified via question: heartbeat runs on **both** sender and
  receiver; user noted the existing `Progress` renderer (§15.10) isn't wired
  up on either side, so this is a new, independent mechanism rather than an
  extension of it — see ARCHITECTURE §15.11.
- New `internal/obs/heartbeat.go`: `Heartbeat`/`NewHeartbeat(ctx,
  sampleInterval, logInterval, groups func() []uint32, channels func() int)`
  samples `ctx.Counters().Bytes` every 10s (default) to compute a trailing-
  window rate, logs at INFO every 60s (default): `groups=<ascending ids or
  "none"> channels=<n> rate=<humanRate>`. `Stop()` is synchronous (closes a
  `done` channel the loop goroutine closes on exit) so callers can safely
  read shared state (e.g. a test's log buffer) right after — an earlier
  fire-and-forget `Stop()` raced the loop's final possible log write under
  `-race`.
- New `humanRate` in `progress.go`: decimal (SI, base-1000) B/s→KB/s→MB/s...,
  deliberately distinct from `humanBytes` (binary, base-1024, for on-disk
  sizes) — matches how bandwidth is conventionally reported and the user's
  literal "MB/sec, KB/sec" phrasing.
- "In progress" derivation differs per side because sender and receiver
  track group/file state differently: sender's `tracker.inProgressGroups()`
  (`state.go`) reads the pre-existing `needed`/`resolved` maps (file ids ÷
  1024 = group id), no new state. Receiver's `needQueue` had no per-group
  bookkeeping at all, so `queue.go` gained a `pending map[uint32]int`
  incremented in `pushGroup` and decremented in `done`, which changed
  `done()`'s signature to `done(groupID uint32)` — updated all 5 call sites
  in `fetch.go` and the pre-existing calls in `queue_test.go`.
- Channel counts: receiver reused the existing `session.channelCount()`.
  Sender had no equivalent, so `run.go` gained a package-level-scoped
  `activeChannels atomic.Int64` incremented/decremented around each
  servicer's lifecycle in `startServicer`.
- Wired in: `sender/run.go` after the manifest goroutine starts (before the
  termination wait); `receiver/run.go` inside the existing `if
  !cfg.DryRun` block, alongside the per-channel `registerAndStart` calls.
  Both use `defer hb.Stop()`.
- Full gate green: `gofmt -l .` clean, `go vet ./...` clean, `go build
  ./...`, `go test -race ./...` all packages, `make check-goroutines` clean
  (heartbeat goroutine goes through `obs.Go` like everything else),
  `make cover-check` 84.1% (min 80%; `internal/sender` alone shows 39.1% in
  isolation but that's pre-existing — `Run()`/`walk.go` are mainly covered
  by e2e tests, not unit tests, and this change didn't touch that balance).
- Not done (not requested, and would violate surgical-changes): wiring up
  the existing `Progress` renderer, or extending `--progress-interval` to
  cover this — the heartbeat is a separate, always-on mechanism.

### 2026-09-07 — Fix: receiver drain watchdog fires mid-transfer (E9002)

- Diagnosed from a real sender/receiver log pair the user pasted: a
  43GB/176k-file resume (34k files / 4GB actually needed) died with
  `E9002 await SESSION_SUMMARY` at 2m12s despite `group.decide` spans still
  completing right up to the failure — i.e. it was progressing, not stuck.
  Root cause: `receiver/run.go`'s pipeline-drain `select` armed
  `cfg.DrainTimeout + workerStopGrace` (65s) from the moment the fetch
  pipeline was launched, not from when termination became *reachable*
  (§13.7). A 4GB fetch phase at the observed ~40-60Mbps/channel goodput
  routinely exceeds 60s, so the watchdog was firing on essentially every
  transfer of this size, independent of real health. See MFR-0003.
- Fix: split the single `select` into two phases mirroring
  `sender/state.go`'s `tracker.wait` — wait unbounded (only cancellable by
  `rootCtx`, graced by `workerStopGrace` once cancelled) for `reachable`
  (`decideWg.Wait()`/`q.close()`/`q.waitDrained()`, i.e. `pipelineDone`);
  only then arm `cfg.DrainTimeout` around `s.chWg.Wait()` (`done`), raising
  the fatal `E9002` itself on that specific timeout (previously the single
  select only logged a non-fatal WARN and fell through, and the real E9002
  came from a second, equally mistimed `DrainTimeout` wait on
  `SESSION_SUMMARY` in step 11). New `waitGrace` helper factors the
  post-cancellation bounded-wait-with-warning used in both phases.
- Full gate green: `gofmt -l .` clean, `go vet ./...` clean,
  `go test -race ./...` all packages, `internal/receiver` coverage 81.9%
  (min 80%).
- No test added that reproduces the exact multi-GB-fetch-past-60s timing
  (would need a paced fake sender run past `DrainTimeout`, similar to
  `tune_test.go`'s `TestRunAdaptiveRampUp` pattern) — flagged as a follow-up
  if this area gets touched again; the fix here is structural (same
  before/after control flow the existing `-race` suite already exercises)
  rather than behavior only a new timing test would catch.

### 2026-09-04 — Adaptive channel tuner (REQ-PAR-004) + sender-side dynamic join

- Replaced the `tune.go` stub with the real §13.4 controller: `decideRamp` is a
  pure, unit-tested decision function (n/minCh/maxCh/goodput/bestGoodput/errored
  → hold|up|down + carried-forward bestGoodput); `session.tune` samples
  `cnt.Bytes`/`Retries`/`FilesFailed` every 2s on a ticker, ramps +2 channels on
  >8% goodput improvement (zero errors, n<max), ramps -1 (retiring the most
  recently opened channel; never the last one — that path is fatal E3005) on
  >8% regression, holds otherwise, and stops for good after 3 unproductive ramps
  in a row (hysteresis, RISK-05). `--channels`/`ESYNC_CHANNELS` sets a new
  `Config.ChannelsPinned` (via `explicitlySet(fs, "channels")` — `fs.Visit` after
  `applyEnv`, since env application also routes through `fs.Set`) that disables
  the tuner entirely and pins N; unpinned sessions start at `--min-channels`
  (default 4) and the tuner owns N from there.
- `session` grew a dynamic channel pool: `chMu`-guarded `workers []*chanWorker`
  + `nextChID` + `pipelineDone` bool, with `addChannel`/`retireOne` as the only
  ways to grow/shrink it and `chWg sync.WaitGroup` tracking every live fetch
  goroutine. `pipelineDone` is the WaitGroup-race guard (Go: `Add` with a
  positive delta must not race with a `Wait` that could observe zero) — every
  `chWg.Add(1)` and the `pipelineDone` transition share `chMu`, so once the flag
  is set no further `Add` can land before the eventual `chWg.Wait()`. See
  MFR-0002 for the real bug this surfaced: that flag must flip on the queue
  being *drained* (`needQueue.waitDrained`, new), not merely closed, or the
  tuner can never ramp up in practice.
- Sender side: `internal/sender/run.go` no longer closes its listener right
  after the initial N data channels (`lst.Close() // the vertical slice does
  not add channels dynamically` is gone) — it now keeps accepting CHANNEL_JOINs
  up to `cfg.MaxChannels` in a background `obs.Go("join.accept", ...)` goroutine,
  starting a `servicer` for each late joiner exactly like the initial batch
  (`startServicer` factored out of the old inline loop). Without this the
  receiver's tuner would dial into a closed listener and ramp-up would be a
  no-op against the real sender, not just the test fake.
- Test harness (`internal/receiver/run_test.go`'s `fakeSender`): added a
  background accept-loop mirroring the real sender's late-join behaviour
  (`AcceptChannel(..., fakeSenderMaxChannels)`, a `joined`/`joinedCount()`
  counter) and `chunkDelay`/`chunkSize` fields to pace `dataLoop` so a test
  transfer can be made to span multiple 2s tuning windows without moving much
  data. The two pre-existing E2E tests now pass `ChannelsPinned: true` so
  they stay deterministic (previously their `Channels: 1`/`2` was silently
  ignored — unpinned always used `MinChannels` — harmless there since neither
  test asserted channel count, but worth pinning explicitly now that it means
  something).
- New tests: `tune_test.go` (`TestDecideRamp` — 7-case pure decision table;
  `TestRunAdaptiveRampUp` — a paced ~3.8s single-file transfer over a real
  loopback fake sender, asserting `fs.joinedCount() > 2` i.e. the live
  controller actually opened channels beyond the floor, not just the pure
  function in isolation). `fetch_test.go`: `TestFetchManifestDigestMismatch`
  closes the "md5 sent originally from the group manifest must match" gap —
  content that matches its own FILE_HEADER/streamed FILE_COMPLETE digest but
  not `fileMeta.digest` (the group manifest's `ManifestEntry.Digest`, copied in
  `decide.go`) is E8003, item-class/non-retryable/session-survives, and the
  file is never published.
- The manifest-digest verification itself (`fetch.go`'s `finish()`, checking
  local hash against `fc.DigestFull`→E8001, `fm.digest`/`hdr.Digest`→E8003) was
  already correct before this session — only the test above was missing.
- Full gate green: `gofmt -l .` clean, `go vet ./...` clean, `go build ./...`,
  `go test -race ./...` all packages, `make check-goroutines` clean,
  `go mod tidy -diff` clean, `make cover-check` 83.9% (min 80%).
- Follow-ups unchanged from prior entries (BLAKE3, `.part` checkpoint resume,
  SESSION_RESUME, `--owner` not wired into fetch, `--shutdown-grace`/
  `--spill-threshold` parsed-only) plus one new one: the tuner's "retire the
  most recently opened channel" ramp-down and its 3-strikes hysteresis are my
  own reading of an underspecified pseudocode in ARCHITECTURE §13.4 (which
  channel to retire, and whether a ramp-down counts as "unproductive" — I
  treated ramp-up as always resetting the streak and ramp-down as always
  advancing it) — worth a user sanity-check if real-world tuning behaviour
  ever looks wrong.

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
