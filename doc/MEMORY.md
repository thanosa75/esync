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

---

## Session Log

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
- Compressed: fully captured by MFR-0004 and the follow-up line in the
  grouping+reliability entry. Original: a 43GB/176k-file run died E9002 at
  ~14:44 with 13GB transferred; the watchdog was armed on `allDecidedCh` while
  decide ran ~20 min ahead of fetch. Fixed by sampling `tracker.progress`.

### 2026-09-07 — 60s groups/channels/bandwidth heartbeat

- New `internal/obs/heartbeat.go`: samples `ctx.Counters().Bytes` every 10s,
  logs at INFO every 60s: `groups=<ascending ids or "none"> channels=<n>
  rate=<humanRate>`. `Stop()` is synchronous (a `done` channel the loop closes)
  so callers can read shared state after it without a `-race` hazard. New
  `humanRate` (decimal SI) distinct from `humanBytes` (binary). Receiver group
  ids come from a new `pending map[uint32]int` in `needQueue` (`done(groupID)`
  signature change, 5 call sites); sender from `tracker.inProgressGroups()` and
  a new `activeChannels atomic.Int64` around each servicer. Wired into both
  runs next to the (previously uninstantiated) progress renderer with
  `defer hb.Stop()`.

### 2026-09-07 — Fix: receiver drain watchdog fires mid-transfer (E9002)
- Compressed: fully captured by MFR-0003. Original: the receiver armed
  `DrainTimeout + workerStopGrace` from pipeline launch, killing any fetch
  phase longer than 60s; fixed by arming only once termination is reachable
  (`decideWg.Wait()`/`q.close()`/`q.waitDrained()`), with `waitGrace` bounding
  the post-cancel unwind.

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
