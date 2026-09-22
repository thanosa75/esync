# esync — Remaining Work, ROI-Ordered

**Date:** 2026-09-22 · **Base:** HEAD `d9177b3` + working tree · **Scope:** everything *not* fixed in the 2026-09-22 bug-hunt and fix passes.

Ordering is **return on investment**: correctness and trust-in-the-process first, then value per unit of effort. Effort is S (< ½ day), M (1–2 days), L (3–5 days), XL (> 1 week).

Two things changed how this list is ordered relative to `STATUS_SUMMARY.md`:

1. **Nine findings are now closed** (see §0) — including every item the owner named. Their dependents move up.
2. **Six items buried in the Tier-4 "doc-drift backlog" are real bugs, not drift.** They were filed there because each is a small edit; that is a statement about *cost*, not about *impact*. They are promoted into §2 below with their true severity. This is the single biggest change in this document versus the previous status file.

---

## 0. Closed — do not re-open

| # | Finding | Closed by |
|---|---|---|
| R-03 | Excluded directories not pruned | `75fbff4` (MFR-0009) |
| R-05 | `make ci` red from `.claude/` false positive | 2026-09-08 |
| R-09 | Second-signal force-exit / `--shutdown-grace` no-op | `ae19178` |
| R-24 | Clean data-channel close treated as fatal; no §7.3 rejoin | `a6d5c6f` (MFR-0010) |
| R-17 | CHANNEL_JOIN has no read deadline | 2026-09-22 (MFR-0012) |
| R-38 | Cancelled worker silently drops its in-flight file | 2026-09-22 (MFR-0011) |
| R-39 | Mid-stream abort desyncs the data channel | 2026-09-22 (MFR-0014) |
| R-40 | `serve()` TOCTOU: swapped source streamed in full | 2026-09-22 (MFR-0013) |
| R-41 | Digest-cache `ctime` second-granularity stale hit | 2026-09-22 (MFR-0015) |

Plus this pass: **R-02, R-12, R-13, R-16, R-19, R-22, R-23** — see the session log in `doc/MEMORY.md`.

---

## 1. Tier A — highest ROI, do next

### A1 · R-01 — CI enforces none of the §19.8 gates · `HIGH` · S
`.github/workflows/build.yml` builds but never runs `make ci`. Every gate this project relies on — `-race`, the 80 % coverage floor, `check-goroutines`, `tidy-check`, `fmt-check` — is enforced only by whoever remembers to run it locally.

**Why first:** it is the cheapest item on this list and it is what makes every other fix *stay* fixed. Four of the nine findings closed above were regressions of invariants that a green CI would have caught at the commit that introduced them. Everything below is worth less without it.

**Do:** add a `test` job running `make ci` on push and PR. Pin the Go toolchain to the `go.mod` version.

### A2 · R-21 — derived keys survive every session · `MED` · S
AGENTS.md states the rule plainly: *"Session keys and the root secret live in process memory only and are **zeroed at every exit path**."* Redaction is implemented (the unloggable types hold), but zeroing is partial — the derived `K_*` material is not wiped on the normal exit path or on any fatal path.

**Why here:** it is an explicit, written project invariant that the code does not honour. Cost is a handful of `defer`s plus a wipe helper; the alternative is a documented security property that is false.

**Do:** wipe on every return from the session owner, not at the call sites. Add a test that asserts the buffers are zero after a completed run.

### A3 · statfs failure → guaranteed abort · `MED` · S · *(promoted from Tier 4)*
`receiver/run.go:400-402`: a failed `statfs` yields free = 0, which makes the very first needed group abort with E7003 "disk full" on a destination that has plenty of room. Any filesystem that does not answer `statfs` — several FUSE mounts, some network filesystems — cannot receive at all, and the reported cause is a lie.

**Do:** fail **open** with a WARN. A space check that cannot run must not become a space check that fails.

### A4 · Drain watchdog false-positives on slow-but-live tails · `MED` · S · *(promoted from Tier 4)*
`tracker.progress` bumps once per chunk, so a transfer moving below roughly one chunk per watchdog window (~17 KiB/s) trips E9002 and kills a session that is working correctly. The catalogue's own wording is the test: **E9002 must mean stalled, never merely slow.**

**Do:** cross-check byte counters against wall-clock instead of counting chunk events. Pairs naturally with A3 — same file, same review.

### A5 · R-08 — receiver summary never lists failed items · `MED` · S
Failures are counted but never named. An operator sees `3 failed` and has no way to learn *which three* without re-reading the logs — and with `--redact-paths` on, not even then.

**Do:** carry the failed set through to the summary and print path + E-code per item, capped with an "and N more" tail.

### A6 · R-10 — dry-run writes to disk · `MED` · S
`--dry-run` creates the destination directory and writes a probe file. A mode whose entire contract is "change nothing" changes something. It is also the reason dry-run cannot be pointed at a read-only path to answer "would this work?".

**Do:** gate the creation and the probe behind `!cfg.DryRun`; degrade the space check to a WARN when it cannot run (shares the A3 fix).

### A7 · R-14 — FILE_CHUNK ceiling arithmetic is self-inconsistent · `MED` · S
The documented ceiling, the constant, and the true maximum (an undocumented 1 MiB − 8192) disagree. Nothing is broken today because the negotiated chunk size sits far below all three, but this is exactly the class of latent inconsistency that R-13 turned into a session-killing bug once real data reached the boundary.

**Do:** derive the ceiling from the record limit in one place, assert the relationship in a test, and fix §8.1/§9.3 to match. **Sequence this immediately after the R-13 work lands** — same arithmetic, same file, and R-13 has already moved the control ceiling to 512 KiB.

### A8 · Exit-code contract is incoherent · `MED` · S (once decided) · **blocked on an owner decision**
Three disagreements at once: E2006 pair-timeout exits 4 while §5.6 says 2; a skips-only run never yields exit 1 although §14.6 says it covers skipped; E2003 (version mismatch) is neither pairing nor auth yet maps to 4.

**Why it matters more than it looks:** exit codes are the only thing automation can see. R-02 and R-23 (both fixed this pass) were the same disease — the process reporting an outcome that did not happen.

**Owner decision needed:** pick the contract (§14.6 or the code), then align the other. The edit is small; the decision is not mine to make.

### A9 · R-11 — fatal-path observability asymmetry · `MED` · S–M
The ring-buffer dump and the summary fire only for goroutine-escalated faults. A fatal returned on the main path exits with no ring dump and no summary — so the failures hardest to reproduce are the ones that log least.

**Do:** move the dump and summary into a single deferred exit handler that every fatal path passes through.

### A10 · R-15 — self-copy guard is dead code · `MED` · S
FS-045/E1008 is implemented and never reached. Sending a tree into itself is a genuine foot-gun (the destination grows inside the source as the walk proceeds).

**Do:** call the guard, or delete it and the E-code. Either is acceptable; a guard that exists and does nothing is worse than both.

---

## 2. Tier B — real bugs, larger or narrower

### B1 · `.esync` rejection is byte-exact · `MED` · S · *(promoted from Tier 4)*
`validate.go:44` compares exactly. On a case-insensitive destination (macOS, Windows) a source directory named `.Esync/` collides with the receiver's own journal directory — **and is deleted with it at cleanup.** This is the only remaining path in the tree that can destroy destination data, which is why it outranks its size. Platform-case-fold the check.

### B2 · Symlink replacement is remove-then-create · `MED` · S · *(promoted from Tier 4)*
§12.3 specifies create-temp + rename. The code removes first, leaving a window where the path does not exist; a crash there leaves the destination missing an entry that the journal believes is resolved. Use the atomic form the spec already describes.

### B3 · `--follow-symlinks` silently crosses mounts · `MED` · S · *(promoted from Tier 4)*
The followed-directory descent lacks the `st_dev` check that `walk.go:94-98` applies everywhere else, so `--follow-symlinks` quietly defeats the default `--one-file-system`. Followed special-file targets are also dropped with no WARN. Add the check; warn on the drop.

### B4 · R-18 — cross-group ordering not enforced under the concurrent decide pool · `MED` · M
§13.6 promises monotonic group progress; the concurrent pool does not guarantee it, which makes E7012 collision winners non-deterministic. Either enforce group-ordered draining or soften the prose — **owner decision**, and the cheap half is the prose.

### B5 · Protocol version lives in four independent constants · `MED` · S · *(promoted from Tier 4)*
`wire.go:27` (orphan, read nowhere), `handshake.go:36` (the enforced one), `plan.go:68`, `journal.go:90`. Four places, one of them dead, and a bump that misses one is a silent cross-version accept. Single source + a test that pins it. R-22's `--version` fix now reads one of these, so the consolidation has a consumer.

### B6 · Journal `protocol_version` is written but never validated · `MED` · S · *(promoted from Tier 4)*
`fsx/journal.go:90`. Invalidation works today only as a side effect of the plan digest folding the version — remove that coupling and stale journals are silently accepted. Validate on read. `source_root_name` is also always written empty although §12.6 lists it as recorded state.

### B7 · Manifest digest folds the literal `"md5"` · `MED` · S · *(promoted from Tier 4)*
§10.5's recipe hardcodes the algorithm name regardless of what `--hash` negotiated, so changing the hash between runs does not change plan identity — a resume can match a plan built with a different algorithm. Fold the negotiated algorithm.

### B8 · R-36 — receiver never emits FILE_CANCEL · `MED` · M *(downgraded)*
Message `0x35` and the sender's handler both exist; no receiver path sends it, so §9.4's INV-3 is vacuous. **This dropped in priority this pass:** the R-39 desync fix removed the correctness need for it, and `serve()` runs synchronously inside `run()`, so the sender cannot read a cancel mid-stream anyway. It is now tidiness plus a small bandwidth win on abandoned transfers — and the MEMORY.md note calling it "WONTFIX, needs a wire message" is stale and should be corrected whichever way this goes.

### B9 · Numeric flag validation does not exist · `LOW–MED` · S
§16 claims every numeric flag is range-validated to E1007. False for all duration flags and two ints; `withDefaults` (`receiver/config.go:69-110`) silently re-defaults negatives, so `--stall-timeout=-5` is accepted and ignored. Validate, or reword the invariant. Validating is barely more work and is the better answer.

### B10 · Counter bugs · `LOW` · S
`Counters.FilesExcluded` is never incremented, so exclusions are double-counted into "skipped" on the wire; the sender `drain` span still bills the whole transfer; the span vocabulary advertises `sort` / `file.verify` / `file.publish`, which no code opens. Small, but they make the summary — the one artefact operators actually read — wrong.

### B11 · SCAN_PROGRESS (0x10) is never emitted · `LOW` · S
Codec-only (`wire/msgtype.go:29`, `frame.go:178`); no sender emit site, and a receiver would E5001 it if one appeared. Emit it during long walks, or scrub §9.3's row. Emitting is the better half — a long scan currently looks like a hang.

### B12 · PING/PONG RTT never reaches the tuner · `LOW` · M
Keepalive is liveness-only (`channel/channel.go:192-243`); `tune.go` uses goodput plus error counters. §9.2/§13.4's "RTT measurement feeding the tuner" over-promises. Either wire RTT in or scrub the claim; the tuner works without it, so this is honesty-first.

---

## 3. Tier C — documentation integrity

### C1 · R-04 — §18.1 verification matrix is largely fabricated · `HIGH` · M + backlog
104 requirement ids claim to be "mechanically checked" against tests that do not exist. **This is the largest integrity gap in the repo** — every other doc claim is read through it, and a reader who spot-checks one row and finds it false has no reason to trust any row.

Ranked below Tier A only because it is blocked on **owner decision** (restore to truth with a real checker and explicit deferred markers, or re-scope the matrix). It outranks everything else in this section and would sit in Tier A the moment that decision lands.

### C2 · R-06 — doc header contradicts the shipped tree · `MED` · S
"No code exists yet", "Protocol version 1", `v0.2.0` dated 2026-09-04 — in a repo of ~16.8 kLOC on protocol version 2. Pure edit, no decision needed. Cheapest credibility win available.

### C3 · R-07 — docs present four absent features as working · `MED` · S docs / L–XL features
Blocked on **owner decision #2**. Until it lands, the docs over-promise; the S-effort half (scrub the prose, mark deferred in §20.3) is available immediately and independently of whether the features are ever built.

### C4 · Remaining doc-drift batch · `LOW` · S total
Best done as one pass, not as individual commits: E8003-vs-E6003 naming; §12.2 pseudocode marking E8003 retryable when catalogue and code agree it is not (the code is right); §12.3's "dir metadata at group completion" versus the intentional end-of-session `applyDirMeta` (MFR-0001); the single-use-code path where §5.6/E2E-07 promise E2005 but a second handshake is mis-read as CHANNEL_JOIN → E4001/E3004 (the *property* holds, the documented code and test do not exist); pair-code version byte 1 versus protocol 2; REQ-PAIR-003 excluding digits 0/1 that §5.2's canonical Crockford alphabet includes; `--redact-paths` emitting a literal `[redacted]` where §15.9 promises `file=<id>`, which makes logs unjoinable across hosts and is the one item here with real operational cost; cross-version mismatch raising E2003 where REQ-PROTO-006 names E5002; phantom `REQ-SEC-070/075`; `--log-sync` missing from §16.1 and usage; usage text omitting `--shutdown-grace`, `--group-bytes`, `--checkpoint-interval`, `--resume-window`, `--stall-timeout`; stale MEMORY.md notes (sender heartbeat, `.part`/`digest.Marshal`, FILE_CANCEL, and the coverage note, which should name `Run`/`pair`/`acceptData`/`sessionParams` as the genuinely untested sender code — `walk.go` has five unit tests); AGENTS.md's "no CI workflow file yet" (it exists — R-01) and its stale "Go is not installed" claim.

---

## 4. Tier D — deferred features

Ordered by **value per unit of effort**, not by size. Nothing here should start before Tier A closes.

| # | Feature | Effort | Note |
|---|---|---|---|
| R-28 | Fault-injection suite (18 scenarios) | XL | **Highest-value item in this tier.** REQ-VER-005 is P0 and has 0 tests and 0 seams. Every bug fixed in the two passes this month was found by hand-built one-off harnesses; this is that work, made permanent. The seams are the hard part — build them compiled-in-but-inert first, and the rest becomes incremental. |
| R-29 | E2E suite beyond E2E-01 (13 missing) | L | Current E2E is in-process, not the "real processes" §19.3 claims. The second-run-zero-bytes and no-delete scenarios directly protect data-safety invariants. |
| R-35 | Z-PROTO-01 state-machine fuzzer + corpus | M | 5/6 targets already present — cheapest item in this tier by a wide margin. |
| R-30 | Benchmarks + §19.8 regression gate | M | Zero `func Benchmark` today, so the adaptive tuner (§13.4) is tuned against nothing measurable. Pairs with A1. |
| R-25 | `.part` checkpoint resume | L | `digest.Marshal` already exists and is unused — the groundwork is half-built. Highest user-visible payoff in this tier: large transfers currently restart whole files. |
| R-26 | SESSION_RESUME suspend/re-dial | XL | **Design already delivered** (`doc/SESSION_RESUME_DESIGN.md`); code awaits sign-off on D1–D4. Unblocking is an owner decision, not engineering. |
| R-31 | Real per-channel pipelining | L | `--pipeline-depth` is advertised in SESSION_READY but depth is always 1 — a placebo flag. If this will not be built soon, remove the flag rather than continue advertising it. |
| R-33 | Summary richness (goodput, channels, time breakdown) | L | Needs `Counters` plus per-stage sampling. Partially overlaps A5 and B10 — do those first and reassess what is left. |
| R-37 | `--log-file` full-fidelity, independent of console level | M | Today a level-filtered MultiWriter mirror, so raising console verbosity is the only way to get a detailed file — exactly backwards for unattended runs. |
| R-34 | TRACE protocol-message logging | L | 6 TRACE sites, none message-level. Largely subsumed by R-28 if that lands first. |
| R-27 | External merge sort above the spill threshold | XL | In-memory always. NFR-010's ceiling is theoretical until someone runs a tree big enough to hit it; defer until R-30 can measure where that is. |
| R-32 | `--owner` materialisation | L | Currently a silent no-op. Needs privilege handling; the honest interim is to reject the flag rather than ignore it. |
| R-20 | `--owner` is a silent no-op *(the reject-it half)* | S | The S-effort fix for R-32's dishonesty, available now and independent of ever implementing chown. |

---

## 5. Owner decisions still blocking work

These gate items above; none can be resolved from the code.

1. **§18.1 matrix (C1):** restore to truth with a real checker, or re-scope? *Blocks the largest integrity gap in the repo.*
2. **Absent features (C3 / Tier D):** implement, or formally defer in §20.3 and scrub the prose?
3. **Exit-code contract (A8):** E2006 → 2 or 4; skips-only → exit 1 or 0?
4. **Cross-group ordering (B4):** enforce deterministic E7012 winners, or soften §13.6?
5. **`--redact-paths` / `--log-file` semantics (C4, R-37):** implement to spec, or reword the spec?
6. **SESSION_RESUME D1–D4 (R-26):** sign off the delivered design, or shelve it?

`--compact-code`, formerly the seventh item here, was resolved by implementing it this pass.

---

## 6. Suggested sequence

1. **A1** alone, first, and merge it before anything else — every subsequent fix should land against enforced gates.
2. **A3 + A4 + A6** as one commit (same file, same review, all three are "a check that cannot run must not fail the run").
3. **A2, A5, A7, A10, B1, B2, B3** — independent small fixes, parallelisable across reviewers. B1 first among these; it is the only remaining data-destruction path.
4. **C2 + C4** as one documentation pass.
5. Put **A8, B4, C1, C3** in front of the owner as a single decision memo; they are four decisions, not four investigations.
6. **B5–B7, B9–B12** as a consistency pass once the decisions land.
7. **Tier D** in table order, starting with **R-35** (cheapest) and **R-28** (highest value).
