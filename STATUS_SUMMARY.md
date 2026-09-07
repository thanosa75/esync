# esync — Code vs Architecture Audit: Status Summary

**Date:** 2026-09-07 (tree: branch `grouping-and-reliability`, 32 files modified/uncommitted, HEAD `2671cde`)
**Scope:** every non-test package (root, `internal/{channel,crypto,digest,fault,fsx,obs,paircode,plan,receiver,sender,wire}` ≈ 20 kLOC) cross-checked against `doc/ARCHITECTURE.md` (§1–21, incl. the 156-row §18.1 matrix), `doc/INITIAL_REQS.md` (156 REQ-ids), and `doc/MEMORY.md` (MFRs + known follow-ups).
**Method:** nine parallel subsystem audits (each read its full doc sections + full package source, cited file:line), plus independent re-verification here of the top ~12 load-bearing claims, plus ground-truth gate runs. Effort scale: S <½ d · M 1–2 d · L 3–5 d · XL >5 d. Findings marked **KNOWN** were already listed as MEMORY.md follow-ups; **NEW** were not. No re-reported MFR (fixed bug) appears.

---

## 1. Verdict

The implementation is in good shape at the **core**: record layer, message catalogue, E-code catalogue, key schedule, golden vectors, and the recent reliability fixes (MFR-0001…0007) all verify byte-for-byte / line-for-line against the docs. The full `-race` suite is green and coverage passes.

The problems are **not in the security/wire core** — they are (a) a small set of **silent-wrong-output behaviors** (pruning, hardlinks, peer notification, dry-run), (b) a **verification story that is substantially fabricated** (§18.1 matrix, fault-injection/E2E/bench suites, CI), and (c) **docs that describe absent features as working** (SESSION_RESUME, `.part` checkpoint, external sort, `--compact-code`, `--owner`, `--shutdown-grace`) — plus a stale header that still says *"No code exists yet. Protocol version 1."*

No CRITICAL security vulnerability was found: single-use pairing, the attempt cap, transcript binding, constant-time confirmation, per-channel sequencing, and decode-side bounds all hold. The E-code catalogue and wire message table are fully consistent (71/71 codes, 19/19 messages).

---

## 2. Ground truth (run today, all in the current tree)

| Gate | Result |
|---|---|
| `gofmt -l .` | clean |
| `go vet ./...` | clean |
| `go build ./...` | clean |
| `go test -race -count=1 ./...` | **13/13 packages pass** |
| Coverage (`-coverpkg=./...`, `make cover-check` equiv.) | **85.0 %** (min 80 %) — passes |
| `make check-goroutines` on the real tree | clean (17 runtime goroutines all via `obs.Go`; zero bare `go`) |
| Bare-`go` grep (repo-wide, non-test) | zero violations |
| E-code catalogue vs §14.2 | 71/71 present, 0 semantic mismatches, retry classes match §14.3 |
| Message catalogue vs §9.2 | 19/19 present, directions + bodies match; goldens re-synced to v2 `group_bytes` |
| Key schedule vs §6.3 | all 7 derived values independently recomputed = golden vectors |
| System-file exclusion **sets** vs §10.1.1 | macOS 18/18, Windows 22/22, Linux/Unix 6/6 byte-exact |
| `make ci` / `make verify` | **currently RED — false positive** (see R-04) |

---

## 3. ROI-ordered findings

ROI = impact ÷ effort. Tier 1 items each either prevent silent wrong output, make the two hosts agree on outcome, or re-arm the verification process that is currently fiction; all are cheap relative to their value.

### Tier 1 — do next (S–M effort, protects correctness/process trust)

**R-01 · CI enforces none of the §19.8 gates — add a test job to the workflow** · `HIGH` · S · NEW
- Docs: §19.8 (gates list), §18.2. Code: `.github/workflows/build.yml` (only `make dist`; no test/race/coverage/fuzz/goroutine/matrix check ever runs).
- Impact: every gate is green only when a developer happens to run `make verify` locally; a PR can merge with broken tests or stale golden vectors. This is the single highest-ROI change — it turns R-02…R-04 into enforced invariants.
- Evidence: workflow steps = checkout → setup-go → `make dist` → upload → release. AGENTS.md ("no CI workflow file yet") is itself stale.

**R-02 · Receiver never notifies the sender of a fatal; sender exits `0` "ok" on receiver-side failure** · `HIGH` · S–M · NEW
- Docs: §14.4, REQ-ERR-005 (P0: "both sides log the same cause"). Code: sender sends `wire.Error{fatal:1}` on fatal (`sender/run.go:57-64`); the receiver's `finish()` (`receiver/run.go:286-318`) has no equivalent send, and its control reader treats a bare EOF as a clean end.
- Impact: receiver dies on E7003 disk-full / E3005 mid-transfer → sender sees EOF → exits 0, prints "ok". The two machines disagree on the outcome — exactly the cross-host inconsistency §14.4 exists to prevent; automation and resume decisions trust a success that was a failure.
- Evidence: `grep '&wire.Error' internal/` → only `sender/run.go:60` (production). No `peer=true` field is ever emitted in a log (grep confirms).

**R-03 · Excluded directories are not pruned — their contents are transferred** · `HIGH` · M · NEW
- Docs: §10.1 ("directory entries **pruned**, not descended into"), REQ-SCAN-026, REQ-CLI-011. Code: `sender/walk.go` descends into every directory (only `.esync` and — never-set — `skipAbs` are skipped); exclusion runs later in `plan.Build` (`plan.go:115-140`), testing each entry's **final component alone**, and the `Prune` flag it computes is recorded on an `Exclusion` record that nothing ever consumes.
- Impact: children of `.Trash-1000`, `$RECYCLE.BIN`, `lost+found`, `__MACOSX`, and of any `--exclude dir` survive, get file ids, fold into the manifest digest, and are transferred. Silent wrong output of a P0 scan requirement; the digest is polluted too, so a later fix changes resume identity. (Found independently by two audits.)
- Evidence: `filter.go:71` comment ("the walker prunes … so its children never reach Build") describes behavior that does not exist; `Exclusion.Prune` (`plan.go:127`) has no consumer; `MatchSystemFile` is called nowhere in `internal/sender`.
- Fix shapes: prune in the walker (give `walkConfig` the rule set) or ancestor-state tracking in `Build`. A test seeding a file inside a pruned dir currently passes — add it first (sysfiles_test seeds only dir names).

**R-04 · Verification matrix §18.1 is largely fabricated and "mechanically checked" is false** · `HIGH` · M (checker) + backlog · NEW
- Docs: §18.1 ("This table is mechanically checked … fails the documentation check in CI"), §18.2 ("154/154 requirements have ≥1 artefact"). Code: no check exists anywhere.
- Evidence (reproduced): 156 matrix rows cite **174 unique artefact ids; ≥104 never appear in any `.go` file** (53 T-, 15 F-, 13 E2E-, 7 B-, 7 D-, 21 I-, 1 Z- in the full audit; 104/174 by direct grep). The "present" remainder are `// T-RES-01` comment annotations on tests, not test names — so even the letter of "test names cover ids" holds for zero ids.
- Impact: a requirement "verified" by `F-NET-01` / `E2E-09` / `B-THRU-03` has no proof; the doc's coverage claim is fiction. Fix: add a `make traceability` checker (matrix ids ↔ test names, run in CI) and make it pass one of two ways per row — real test, or an honest "deferred" marker. Do **not** silently renumber tests to match ids.

**R-05 · `make ci` is RED from a false positive: `.claude/` worktree is scanned by `check-goroutines`** · `MED` · S · NEW
- Code: `Makefile:117-124` — `find` does not exclude the untracked nested git worktree `.claude/worktrees/e9002-drain-progress/`, whose stale copy of `internal/obs/goroutine.go` trips the rule; `make fuzz` has the same globbing issue.
- Impact: the pre-commit gate fails for a reason unrelated to the real tree; `make ci`/`make verify` cannot currently be the trusted entry point R-01 should run.
- Evidence: reproduced exit 1; also the grep pattern misses `go x()` spawned inline after `{` (false-negative window worth closing while there).

**R-06 · Doc header contradicts the shipped tree — "No code exists yet", "Protocol version 1", v0.2.0 dated 2026-09-04** · `MED` · S · NEW
- Evidence: `ARCHITECTURE.md:1-16` banner; body itself documents the "protocol_version bump (1 → 2)" (§10.4); code is v2 (`handshake.go:36`, `plan.go:68`); §21 revision history's newest entry is 09-04 — the 09-07 sessions in MEMORY.md are unrecorded. Header status/version fields are the first thing a reader checks; they misstate the wire format.

**R-07 · Docs present four absent features as working — decide implement-vs-defer, then scrub prose** · `MED` · S (docs) / L–XL (features) · KNOWN
- (a) **SESSION_RESUME / §7.3 suspend–re-dial** — wire `0x70` exists (golden-tested) but nothing sends or handles it; control loss is always fatal; `--resume-window` parsed-only; E3007 never raised. §7.3/§9.2/§14.2/E3007/REQ-NET-009 read as working; §20.3 doesn't even list it.
- (b) **`.part` checkpoint resume** — §11.4/§12.6/F-SIG-02/REQ-XFER-041 promise `parts/<id>.state` + truncate-to-verified-offset; `fetch.go:116` truncates to 0 and re-fetches; `digest.Marshal/Unmarshal` have zero callers; `--checkpoint-interval` help admits "(parsed)".
- (c) **External sort** — §10.3/REQ-SCAN-033/NFR-010 promise a `$TMPDIR` external merge above 500 k entries; `plan.go:148-150` sorts in memory (`TODO(B-MEM-01)`); `--spill-threshold` is discarded at `cli.go:234` (`_ = fs.Int(...)`).
- (d) **BLAKE3** — selectable per §16.1 table, §11.5, G-HASH-01, RISK-02, OP-02; code rejects it with E1007 ("reserved, not implemented") at `digest/algo.go:66-68`.
- Recommendation: implement (a)/(b) if resume economics matter (see Tier 3), otherwise add all four to §20.3 Deferred and scrub the "as-implemented" prose. Until then an operator following the docs gets a hard E1007 / a silent re-transfer / a dead session at control loss.

---

### Tier 2 — high-value behavior fixes & cheap doc-truth (S–M)

**R-08 · Receiver summary never lists failed items** · `MED` · S · NEW
- Docs: §14.3 ("named in the summary"), §15.7 `failed items:` block. Code: `obs.Summary` accepts a `failedItems` variadic but `receiver/run.go:308` calls it empty; the fetch loop tallies `FilesFailed` but never accumulates id/path/code. The documented "which files failed and why, at a glance" is absent on the receiver.

**R-09 · Second-signal force-exit unimplemented; `--shutdown-grace` parsed-only no-op** · `MED` · M · KNOWN-half
- Docs: §14.7 / REQ-CLI-010 ("a second signal within the grace period exits immediately with 5 and no summary"). Code: both `signal.watch` goroutines (`sender/run.go:147`, `receiver/run.go:456`) handle exactly one signal; a second SIGINT is dropped; `shutdownGrace` (`cli.go:78`) is never plumbed to either run path. First-signal behavior (cancel → journal fsync → resume command → exit 5) is correct.

**R-10 · Dry-run writes to disk: destination directory is created and probed** · `MED` · S · NEW
- Docs: REQ-CLI-009 ("write nothing to disk"), §16.3. Code: `receiver/run.go:388` `os.MkdirAll(destPath)` + `checkWritable` (:389) run unconditionally; only later stages sit behind `!cfg.DryRun`. A dry-run leaves an empty directory behind and fails on a read-only destination it would never write to.

**R-11 · Fatal-path observability asymmetry: ring dump + summary fire only for goroutine-escalated faults** · `MED` · S–M · NEW
- Docs: §14.4 step 2 / §15.6 (ring dump on any Fatal), REQ-ERR-031 (summary on fatal exit). Code: `DumpRing` is reachable only via `obs.Go`'s panic/return path (`obs/goroutine.go:38-75`); main-flow fatals (E2006/E3001/E7003/E9002 watchdogs, all `s.fail()`/`setFatal` routes) skip it — and an E9001 panic reaches `OnFatal`, which exits without printing the summary (`cli.go:199-204`). Unify on one fatal funnel.

**R-12 · Sender's `--drain-timeout` is not configurable** · `MED` · S · NEW
- Docs: §13.7 names `--drain-timeout` for both sides. Code: flag registered only on the receiver FlagSet; sender hardcodes `DrainTimeout: 60s` (`cli.go:290`); `sender.Config.DrainTimeout` is unreachable from the CLI. An operator cannot lengthen the sender watchdog for a slow-but-legit tail.

**R-13 · GROUP_MANIFEST can exceed the control-record ceiling → sender E5003 kills a legal session** · `MED` · M · NEW
- Docs: §8.1 `MaxCTControl` (256 KiB+16), §10.4 (group ≤1024 entries), §9.3 sizes are "typical-path" only. Code: `sender/manifest.go:117` writes the whole group as one record; record writer rejects padded ct_len > 256 KiB+16 with E5003 (`record/record.go:110-111`). Grouping caps entries by count + **file bytes**, never by encoded manifest size; paths are u16 (up to 64 KiB) with ~100+ B overhead each → worst case megabytes, and ~200 B avg paths already overflow 256 KiB.
- Impact: deep/vendored trees whose 1024-entry groups average ≳200 B path fail deterministically, and the resume journal replays the same failing group forever.

**R-14 · FILE_CHUNK ceiling arithmetic is self-inconsistent; true max chunk is an undocumented 1 MiB−8192** · `MED` · S · NEW
- Docs: §7.1/§16.1 (chunk default 1 MiB, data record ≤ 1 MiB). Code: codec `maxChunkData = 1<<20` (`wire.go:42`) cannot fit `MaxCTData = 1<<20+16` (`record.go:43`) once header/padding are added → the sender rejects its own maximum. The failure is masked only because esync's receiver advertises the undocumented `safeMaxChunk = 1<<20−8192` (`receiver/config.go:14-19`); any peer advertising ≥ ~1 MiB−9 kills the sender on its first full-size chunk. Fix: clamp on the sender to the record limit and derive both constants from one source; update doc text.

**R-15 · Self-copy guard (FS-045/E1008) is dead code** · `MED` · S · NEW
- Docs: §10.1 ("destination inside source → skipped WARN", E1008 fatal). Code: `walkConfig.skipAbs` is never set (`sender/run.go:212-217`); `fsx.IsInside` + E1008 have zero production call sites. A same-machine copy into a subtree of the source walks files mid-write → nondeterministic plans/digest mismatches instead of the documented guard.

**R-16 · Hard-link secondary can be silently dropped with exit 0** · `MED` · M · NEW
- Docs: §12.4 / REQ-FS-003 ("correctness preserved" via content fallback). Code: a secondary decided while its primary is still in flight is parked by `claimSecondary` (`receiver/decide.go:364-365`) and is only materialised when the primary publishes (`fetch.go:208-212` `registerMaterialised`); if the primary is **skipped** (prior-resume or identical-content skip) it never publishes and the secondary is never linked *or* fetched; a deferred-link failure at `fetch.go:209` is warn-only with no content fallback and no `FilesFailed`.
- Impact: hardlinked trees onto link-less destinations (FAT/exFAT/FUSE) or after a resume can end with files absent, journal clean, exit 0.

**R-17 · CHANNEL_JOIN/ACCEPT has no read deadline — silent on-LAN connector parks the sender** · `MED` · S · NEW
- Docs: REQ-NET-010 ("no code path may wait forever"). Code: `handshake/channel.go:65` (`io.ReadFull`) and `channel.go:48` have no deadline; keepalive is enabled only after join. A peer that connects and sends nothing parks the accept loop indefinitely; no E-code fires. Add a deadline mirroring `--handshake-timeout`.

**R-18 · Receiver cross-group ordering is not enforced under the concurrent decide pool** · `MED` · M · NEW
- Docs: §13.2/§13.6 ("need queue … ordered", "Group order itself remains ascending, so progress is monotonic"), queue.go's own invariant comment. Code: up to `GroupCredit`=4 manifests are decided by parallel workers and appended in lock-acquisition order (`queue.go:53-80`), with no per-group barrier.
- Impact: request streams interleave groups arbitrarily (heartbeat/§13.6 claims overstated) and — worse — FS-044 name-collision winners (E7012) become scheduling-dependent: an identical manifest can yield a different destination tree across runs and after a resume. Decide: enforce group-ordered draining, or soften the doc's monotonicity claim.

**R-19 · `--compact-code` is documented (§16.2, §5.2, §18.1) but the flag does not exist** · `MED` · S · NEW
- Code: zero parses of `compact-code`; `eps = eps[:4]` (`sender/run.go:130`) prints up to 4 endpoints with no WARN, so codes can exceed the REQ-PAIR-002 ≤64-char target silently and §14.1's "code with >2 endpoints" WARN never fires. Implement the two-endpoint truncation + WARN, or scrub the rows.

**R-20 · `--owner` is a silent no-op** · `MED` · M (feature) · KNOWN
- Docs: §16.3 `--owner | off | FS-010`; help text "restore ownership". Code: parsed → `Config.Owner` → never read; `fsx.Dest.Chown` (`fsx/dest.go:173`) has zero callers. Either implement (chown on materialise + privilege handling) or remove the flag; a flag that appears to work but changes nothing is worse than absent.

**R-21 · Redaction/zeroing is partial: derived keys survive every session** · `MED` · S · NEW
- Docs: §15.9 (code + K_* held in redacted types), REQ-SEC-012 (zeroed at session end). Code: only the 128-bit root secret is zeroed (`sender/run.go:114`, `receiver/run.go:337`); K_pair, PRK, K_conf, K_chan and all per-channel K_enc/K_mac are plain buffers never wiped (`handshake/schedule.go:29-61`); the pairing code itself never lives in a redacted type. **No log leak was found** (nothing logs the code or keys) — this is mechanism-vs-claim drift, not an active leak.

**R-22 · `--version` omits the protocol version** · `LOW` · S · NEW
- Docs: REQ-CLI-008 requires semantic version, commit, build date **and protocol version** (relevant post 1→2 bump). Code: `main.go:35-37` prints three of the four. Add `wire.ProtocolVersion` to the string.

**R-23 · Walk-skip counts never reach the summary or exit code** · `LOW` · S · NEW
- Docs: REQ-SCAN-032 (count in final summary, affects exit), §14.6 (exit 1 covers skipped). Code: E6005/6/7 skips go only into `SCAN_COMPLETE.skipped_entries` (`sender/manifest.go:63`); `SESSION_SUMMARY.FilesSkipped` (`run.go:392-396`) omits them, and the exit switch keys on `FilesFailed` only (`run.go:60-70`) → an unreadable subtree prints nothing and exits 0.

**R-24 · Channel-loss handling: a clean close of one data channel is fatal E3005; §7.3 rejoin is absent** · `MED` · M · NEW
- Docs: §7.3 (recoverable row: requeue + CHANNEL_JOIN retry ×3 + retire), REQ-NET-008. Code: no rejoin/retire machinery — `addChannel` is tuner-driven only; a dead channel's fetcher burns per-item `--max-retries` on a wedged conn and a *clean* FIN maps to fatal non-retryable E3005 (`fetch.go:244-254`), the opposite of the doc's recoverable row. The narrow-but-real "one data flow dies, control survives" case degrades to zombie retries or aborts the whole transfer.

---

### Tier 3 — deferred features with honest state (larger; do after Tier 1–2)

| # | Feature | Doc / REQ | Effort | Status |
|---|---|---|---|---|
| R-25 | `.part` checkpoint resume (verified-offset truncation, `parts/<id>.state`) | §11.4, §12.6, XFER-041, F-SIG-02 | L | KNOWN — stubbed (`fetch.go:116`, digest.Marshal unused) |
| R-26 | SESSION_RESUME suspend/re-dial within `--resume-window` | §7.3, REQ-NET-009, E3007 | XL | KNOWN — wire type only |
| R-27 | External merge sort above spill threshold | §10.3, REQ-SCAN-033, NFR-010 | XL | KNOWN — in-memory always |
| R-28 | Fault-injection suite (18 scenarios; **needs inject seams** compiled-in-but-inert) | §19.4, REQ-VER-005 (P0) | XL | NEW — 0 tests, 0 seams |
| R-29 | E2E suite beyond E2E-01 (50 k-file, second-run-zero-bytes, NFD↔NFC, dry-run, no-delete, code-race E2005, sys-v1 exclusion …) | §19.3 E2E-02..14 | L | NEW — 1/14; current E2E is in-process, not "real processes" |
| R-30 | Benchmarks (B- ids, `REQ-VER-007`, §19.8 regression gate; zero `func Benchmark` today) | §19.6 | M | NEW |
| R-31 | Real per-channel pipelining (`--pipeline-depth`; currently advertised in SESSION_READY but depth is always 1) | §13.3, REQ-PAR-003 | L | NEW — flag is a placebo |
| R-32 | `--owner` materialisation (chown + privilege handling) | FS-010 | L | KNOWN |
| R-33 | Summary richness: goodput/channels/time-breakdown/planned bytes (§15.7 example); needs `Counters` + per-stage sampling | REQ-OBS-013 | L | NEW |
| R-34 | TRACE protocol-message logging (msg type/size/correlation at SendMsg/RecvMsg) | §15.2/§15.5, REQ-OBS-017 | L | NEW — 6 TRACE sites, none message-level |
| R-35 | Z-PROTO-01 state-machine fuzzer + checked-in corpus | §19.5, REQ-VER-004 | M | NEW — 5/6 targets present |
| R-36 | Receiver emits FILE_CANCEL after retry exhaustion (message `0x35` + sender handler **already exist**; MEMORY's "WONTFIX — needs a wire message" is stale) | §9.2/§9.4 INV-3 | M | KNOWN — INV-3 currently vacuous |
| R-37 | `--log-file` full-fidelity independent of console level (currently a level-filtered MultiWriter mirror) | REQ-OBS-015, §16.1 | M | NEW |

### Tier 4 — doc-drift backlog (each a small doc edit or a tiny code alignment; batch in one pass)

- **Exit-code contract (§14.6 vs `fault.go:196`):** E2006 pair-timeout exits 4, docs say 2 (§5.6/REQ-PAIR-008); "skips-only" never yields exit 1 (code keys on failures only); E2003 (version) is neither pairing nor auth yet maps to 4. Decide the contract, then align doc+code.
- **E8003 vs E6003:** §12.1/REQ-XFER-012 name E6003 for streamed-digest mismatch; code sends E8003 ("equivalent to E6003" per catalogue). Align the doc to E8003.
- **§12.2 pseudocode** marks E8003 retry; catalogue+code are non-retryable (code is right — fix the pseudocode).
- **§12.3** says dir metadata applied "at group completion"; code intentionally defers to end-of-session `applyDirMeta` (MFR-0001). Update §12.3.
- **Single-use code path:** §5.6/E2E-07 promise "second receiver gets E2005"; in practice a second full control handshake is mis-read as CHANNEL_JOIN → E4001/E3004 (E2005 unreachable). The property holds; the documented code + test do not exist.
- **Pair-code version byte** is 1 while protocol version is 2 (§5.1 labels it "the protocol version"); REQ-PAIR-005's "E2003 before connecting" is inaccurate (E2002/E2003 fire later).
- **REQ-PAIR-003** demands excluding digits 0/1; §5.2 alphabet + code include them (canonical Crockford). Req text contradicts its own architecture.
- **`--redact-paths`** substitutes a literal `[redacted]` where §15.9 promises `file=<id>` (logs become unjoinable across hosts).
- **Manifest digest folds the literal `"md5"`** regardless of the negotiated hash (§10.5 recipe) — a `--hash` change between runs is not detected by the plan identity.
- **`.esync` rejection is byte-exact** (validate.go:44); on case-insensitive destinations (macOS/Windows) a source `.Esync/` would collide with the receiver's journal dir and be deleted with it at cleanup. Platform-case-fold the check.
- **journal `protocol_version` is stored but never validated on read** (`fsx/journal.go:90`); invalidation today works only because the digest folds the version. And `source_root_name` is always written "" (§12.6 lists it as recorded state).
- **Cross-version mismatch raises E2003; REQ-PROTO-006 names E5002** — catalogue, code, and requirement text disagree with each other.
- **Protocol version lives in 4 independent constants** (`wire.go:27` — orphan, read nowhere; `handshake.go:36` — the enforced one; `plan.go:68`; `journal.go:90`). Single source + test.
- **Phantom REQ ids** `REQ-SEC-070/075` are cited in INITIAL_REQS but never defined — would trip the doc's own "row for a nonexistent requirement" check.
- **Numeric flag validation:** §16's "every numeric flag is range-validated → E1007" is false for all duration flags and two ints; `withDefaults` silently re-defaults negatives (`receiver/config.go:69-110`). Either validate or reword the invariant.
- **Counter bugs:** `Counters.FilesExcluded` never incremented (exclusions double-counted into "skipped" on the wire); sender `drain` span still bills the whole transfer (KNOWN); span vocabulary advertises `sort`/`file.verify`/`file.publish` that no code opens (§15.5).
- **`--log-sync`** parses + works but is missing from §16.1 and usage text; usage text also omits `--shutdown-grace/--group-bytes/--checkpoint-interval/--resume-window/--stall-timeout`.
- **`--follow-symlinks` + default `--one-file-system`** can cross a mount (followed-dir descent lacks the `st_dev` check `walk.go:94-98` applies elsewhere); followed special-file targets are dropped with no WARN. Add the check or document the edge.
- **Sender drain watchdog false-positives below ~1 chunk/window** (~<17 KiB/s): `tracker.progress` bumps per chunk, so a slow-but-live tail <1 chunk/60 s trips E9002 ("E9002 must mean stalled, never merely slow"). Cross-check counter movement against wall-clock.
- **statfs failure** → free=0 → guaranteed E7003 abort on the first needed group (`receiver/run.go:400-402`). Fail-open with a WARN instead.
- **Symlink replacement is remove-then-create**, not create-temp+rename (§12.3) — non-atomic window.
- **MEMORY.md** inaccuracies to fix while editing: sender heartbeat described as "file ids ÷ 1024" (code uses real wire group ids — correct already); ".part checkpoint" described as deferred without noting `digest.Marshal` exists; FILE_CANCEL "WONTFIX" reasoning stale (R-36). Coverage notes should name `Run/pair/acceptData/sessionParams` as the truly untested sender code (walk.go has 5 unit tests).
- **AGENTS.md**: "no CI workflow file yet" (exists — R-01); check-goroutines / coverage descriptions updated for the above.

---

## 4. What verified clean (do not "fix")

- **Record layer §8.1** byte-exact: EtM, const-time MAC before decrypt, fresh IV/record, per-channel per-direction seq in MAC, decode bounds pre-allocation, E5002/4003/4004/4005/5004 mapping.
- **Message catalogue:** 19/19 ids, directions, field order/type match §9.2/9.3; goldens G-WIRE-01/G-REC-01 current post-bump.
- **Key schedule & handshake:** all K_* match §6.3 (recomputed); transcript + const-time confirmation; single-use pairing; 5-attempt cap (E2007); expiry (E2006); discovery ranking per §5.4.
- **E-code catalogue:** 71/71, classes + retry table + backoff match §14.2/14.3; `TestCatalogueRoundTrip` pins it.
- **System-file exclusion sets** (patterns) byte-exact per OS; `.esync` self-exclusion and rule ordering hold.
- **Manifest digest recipe** matches §10.5, grouping-independent (MFR-0006 honored), single producer.
- **Recent reliability fixes present and wired:** stall timeouts on both data-channel receives (MFR-0005); E9002 armed only at reachability with inactivity sampling both sides (MFR-0003/0004); tuner gated on queue-drained (MFR-0002); dirs 0o755 + end-of-session deepest-first applyDirMeta (MFR-0001); progress + heartbeat wired in both runs inside `!cfg.DryRun` (MFR-0007).
- **Path safety ordering:** manifest entries validated + collision-checked before any fs call; skip-check Lstat confined through `os.Root`.
- **Goroutine rule:** zero bare goroutines; all 17 through `obs.Go`.
- **Queue/pipeline bounds:** bounded manifest channel (2·credit+8), queue cap 4096, backpressure end-to-end; no unbounded goroutine pile-up.
- **Coverage 85.0 %** total (`-coverpkg`); **all 13 packages pass `-race`**.

---

## 5. Decisions the owner should make (blockers for the doc-drift items)

1. **CI:** add a `make verify`-equivalent job to `.github/workflows/build.yml`? (Recommended: yes, immediately — R-01.)
2. **Absent features (§10.3 sort, §7.3 resume, §11.4 checkpoint, BLAKE3, `--owner`, `--pipeline-depth`):** implement (Tier 3) or formally defer in §20.3 and scrub "as-implemented" prose? Until decided, docs over-promise.
3. **§18.1 matrix:** restore it to truth (checker + real artefacts + explicit deferred markers) or re-scope it? The current 104-id fabrication is the largest integrity gap in the repo.
4. **Exit-code contract** (E2006→2 or 4; skips→1 or 0): pick and align §14.6 + `fault.go`.
5. **`--compact-code` / `--redact-paths` / `--log-file` semantics:** implement to spec or reword spec.
6. **Cross-group ordering:** enforce group-ordered request draining (deterministic E7012 winners) or soften §13.6 monotonicity prose (R-18).

---

## 6. How to re-run this audit

- Gate ground truth: `make verify` (after R-05) / raw `go test -race ./...` + `go test -coverpkg=./... ./...`.
- Matrix check (R-04): extract ids from `ARCHITECTURE.md` §18.1, `grep -r` over `*.go`, diff.
- E-codes: compare `fault/catalogue.go` against §14.2 (currently identical).
- The nine subsystem briefs used for this audit map 1:1 to the doc sections cited per finding; MEMORY.md MFRs were treated as ground truth (fixed bugs were not re-reported).
