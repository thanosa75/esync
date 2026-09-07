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
the fetch pipeline is first launched — mirror `sender/state.go`'s
`tracker.wait`, which correctly waits unbounded on `allDecidedCh` before
arming `drainTimeout` around `doneCh`. Arming it at pipeline start makes
`DrainTimeout` (default 60s) a cap on the *whole transfer*, not on the tail
described by ARCHITECTURE §13.7 ("once reachable, terminate within
drain-timeout") — any real transfer whose fetch phase runs past 60s (trivial
on a multi-GB tree) hits `E9002 await SESSION_SUMMARY` mid-transfer and gets
killed, even though it was still making steady progress (confirmed from a
real 43GB/176k-file run: `group.decide` spans kept completing for ~60s after
the watchdog fired). Once the root context is cancelled for some other
reason, `workerStopGrace` still bounds how long Run waits for the
decide/fetch goroutines to unwind — see `waitGrace`.

<!--
MFR-0004 · <area> · <the rule, imperative> — <why: the bug it prevents>
-->

---

## Session Log

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

### 2026-09-04 — CLI wiring, E2E test, coverage gate

- Closed out the vertical slice: the phased workflow had already landed all of
  `internal/*` (6 agents, 0 errors, full build/vet/test-race green); this session
  wrote the thin orchestrator on top per ARCHITECTURE §17.
- `main.go`: `run(args) int` does mode dispatch only — `--help`/`--version` short
  circuit; `hasLinkFlag` (scans for `--link`/`-link`/`--link=` before a `--`
  terminator) selects `runReceiver` vs `runSender`; no args → usage (exit 3). All
  state machines/signal handling/summary rendering already live in
  `sender.Run`/`receiver.Run` — main.go/cli.go never touch them beyond the call.
- `cli.go`: full §16.1 flag surface (`registerCommon`) + sender/receiver-specific
  flags, `ESYNC_<NAME>` env fallback applied only to flags `fs.Visit` didn't see
  (flag > env > default), numeric floors via `atLeast` → E1007, `--hash` rejects
  `blake3` (reserved, not implemented — E1007), `installFatalHandler` wires
  `obs.OnFatal` → log + flush(once) + `os.Exit(fault.ExitCode(err))`.
- E2E test (`e2e_test.go`, root `package main`): drives real `sender.Run` +
  `receiver.Run` over real loopback TCP with a real X25519 handshake, 5 files
  spanning empty/small/chunk-crossing/multi-MB, asserts byte-for-byte tree
  equality + no `.esync/` residue. Skips gracefully if `paircode.Discover` finds
  no usable LAN interface.
- **Gotcha (not an MFR — no product bug, only a test-harness trap):** the E2E
  test originally passed one shared `obs.Ctx` to both `sender.Run` and
  `receiver.Run`; their `Summary` calls read the same live `obs.Counters`, so
  each side's tallies summed with the other's ("transferred 10 files" for 5
  actual files). Fixed by giving sender and receiver **separate** `obs.Init`
  instances (`sctx`/`rctx`). Any future test driving both roles in one process
  must do the same.
- Coverage gate: `go test ./...` (per-package coverage) put `main` at 0% and
  `sender`/`channel`/`digest`/`handshake` under 80% because the transfer engine
  is mainly exercised through the root E2E test, not per-package unit tests.
  Fixed two ways: (1) `Makefile` `cover`/`cover-check` now pass
  `-coverpkg=$(COVERPKG)` (`COVERPKG ?= ./...`) so the E2E test's coverage
  attributes to every package it exercises, not just `package main`; (2) added
  `cli_test.go` (flag parsing, env precedence, usage-error paths, `hasLinkFlag`,
  `atLeast`, `once`) since `-coverpkg` alone doesn't cover cli.go's parse-error
  branches the E2E test never takes. Result: 84.3% total (min 80%).
- Full gate run green: `gofmt -l .` clean, `go vet ./...` clean,
  `go test -race ./...` all packages pass, `make check-goroutines` clean,
  `go mod tidy -diff` clean, `make cover-check` 84.3%, `make build` produces
  `bin/esync`. `golangci-lint` not installed in this environment (soft-fail
  per Makefile, not a gate failure).
- Follow-ups carried forward unchanged from the receiver session (adaptive
  tuner, BLAKE3, `.part` checkpoint resume, SESSION_RESUME, etc. — see prior
  entry) plus: `--owner` is parsed but not wired into fetch's materialisation
  path; `--shutdown-grace` / `--spill-threshold` are parsed and validated but
  not yet consumed by sender/receiver.

### 2026-09-04 — internal/receiver package

- Built the RECEIVER half against the existing wire protocol / `internal/channel`
  / `internal/sender` (all committed, untouched). New package `internal/receiver`:
  `config.go` `queue.go` `space.go` `decide.go` `fetch.go` `run.go` `tune.go`.
- `Run(ctx obs.Ctx, cfg Config) (Summary, int)` drives §4.2: decode pairing code
  before IO (E2001) → `channel.Dial` racing candidates + `ClientHandshake` →
  `channel.Control` → recv SESSION_PARAMS (adopt sender's `hash_alg`) → resolve
  dest `./<RootName>` or `--dest`, `fsx.OpenDest` (E1006) → send SESSION_READY
  (advertises `MaxChunk = 1<<20-8192`, `GroupCredit`, N = min(--channels,
  sp.MaxChannels)) → dial N data channels + `JoinChannel` → control-reader goroutine
  (SCAN_COMPLETE/GROUP_MANIFEST/SESSION_SUMMARY) → `fsx.Open` journal+resume →
  decide-worker pool + one fetcher per data channel → drain (INV-7) → recv
  SESSION_SUMMARY, compare completion digest (E5005 exit 2), send
  SESSION_SUMMARY_ACK → journal.Clear on success / retain + print resume hint.
- decide (§11.2): `fsx.ValidatePath` → reject w/ numeric code; `fsx.Collisions`;
  journal `prior` skip; dir/symlink/hardlink materialisation; §11.2 table incl
  `--quick` (size+mtime); largest-first enqueue; ONE GROUP_DECISION per group
  (INV-1) then CREDIT{+1} (T-PROTO-08); free-space guard §12.7 (WARN then E7003
  before writing).
- fetch (§12.2): FILE_REQUEST (always offset 0 — see follow-ups) → FILE_HEADER →
  `.part` truncate+stream, offset-contiguity E5006 fatal, ENOSPC E7003 fatal, hash
  → FILE_COMPLETE: byte count E8002, streamed digest E8001, manifest digest E8003,
  `fsx.PublishPart` (E7006 = content-ok/meta-warn), `journal.MarkComplete`,
  hardlink secondaries. Retry policy §14.3 via `fault.Backoff` + `--max-retries`.
- completion digest = SHA-256 over ascending u64-BE file_ids that reached
  FILE_COMPLETE this session (matches `sender/state.go`).
- Goroutines all via `obs.Go` returning nil, routed through `session.fail` +
  root-ctx cancel (obs.Go escalates Fatal returns to the global OnFatal, which
  Run must not trigger). `make check-goroutines` clean.
- Fixed while integrating: MFR-0001 (read-only manifest dir mode blocked child
  publish). `decideDir` now creates 0o755, `applyDirMeta` stamps real mode+mtime
  deepest-first at session end.
- Tests (`-race`, coverage 81.8%): P-QUEUE-01 (order/bound/no-drop/no-double-yield),
  T-DEC-01/02/03 + collision/unsafe-path/free-space/dir/symlink/hardlink,
  T-RES-01 (journal skip), T-XFER-02 (digest-mismatch retry), T-XFER-03 (atomic
  publish), T-PROTO-08 (credit), fetch component test vs in-memory sender stub,
  T-PROTO-06 end-to-end `Run` vs a loopback fake sender (+ resume/no-op run).
- Out of scope (follow-ups): adaptive tuner (`tune.go` stub, `--channels` fixed,
  min/max parsed only); BLAKE3 (E1007); effective pipeline depth 1/channel;
  `.part` checkpoint resume §11.4 (offset always 0); E1008 self-copy not
  enforceable receiver-side; `--dry-run` has no clean protocol shutdown;
  SESSION_RESUME (0x70) unimplemented; E2005 indistinguishable from E4001.

### 2026-09-04 — Initial vertical-slice implementation

- Repo was a skeleton (`src/main.go` stub only); full design in `doc/`.
- Moved entrypoint to module root per ARCHITECTURE §17; `Makefile` `MAIN_PKG ?= .`.
- Target for this session: a **working vertical slice** (agreed with user) — a real
  encrypted LAN transfer through the whole pipeline, race-tested, with golden/property
  tests and one E2E test. Out of session scope (documented follow-ups): BLAKE3 digest,
  adaptive channel tuner (fixed `--channels` only), fault-injection seams (`F-*`),
  benchmarks (`B-*`), and full `§18.1` `T-`/`D-` matrix coverage.
- Implementation run as a phased multi-agent workflow over the `internal/` packages
  from `ARCHITECTURE §17`, integrated by the orchestrator.
- Digest: MD5 + SHA-256 only (stdlib). `hash_alg` wire field keeps BLAKE3 (id 1)
  reserved. Handshake crypto is pure stdlib (`crypto/ecdh`, `crypto/hkdf`).
- `internal/fault` + `internal/obs` landed (base packages). fault: full §14.2
  catalogue in one policy table (`catalogue.go`), `*Fault` with code-driven
  Class/Retryable, `New/Newf/Wrap/WithCtx`, `ExitCode`, `Backoff`, `IsRetryable`,
  `IsFatal`. `Signal`/`ErrSignal` for exit 5. obs: leveled structured logger
  (`Level` starts at 1 so the zero value is "unset"), text+json formats, `Ctx`
  trace context + `With`, spans, trace ring, non-blocking sink (drop+count),
  `Counters`, `Summary`, `Secret`/`RedactedString` redaction, `Progress`,
  `obs.Go`/`OnFatal`/`DumpRing`. obs imports fault (no cycle; fault imports
  neither). Deferred here: adaptive tuner, F-* seams, BLAKE3, benchmarks,
  full T-/D- matrix.
