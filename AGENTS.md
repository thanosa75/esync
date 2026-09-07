# AGENTS.md

## Project Overview

`esync` is a one-shot, zero-configuration, single-direction file-tree transfer for two machines on
the same LAN: the sender runs `esync <path>`, prints a short pairing code, the user pastes it into
`esync --link <code>` on the receiver, and the directory is copied once — encrypted, authenticated,
integrity-checked, differential (identical files are not re-sent), and parallelised to saturate the
link. No accounts, no daemon, no persistent config or state. It is written in Go (`module esync`,
`go 1.27.1`) and ships as a single self-contained static binary requiring no runtime dependencies
and no elevated privileges, targeting Linux (x86-64, arm64) and macOS (arm64, x86-64) with
cross-platform transfers in both directions. The package layout follows `doc/ARCHITECTURE.md` §17:
`main.go`/`cli.go` at the module root (flag parsing, mode dispatch, exit-code mapping) with the
sender/receiver state machines and supporting code under `internal/` (`sender`, `receiver`, `plan`,
`wire`, `channel`, `crypto`, `digest`, `paircode`, `fsx`, `obs`, `fault`).

## Build and Test Commands

A `Makefile` wraps the common tasks (`make help` lists them); there is no `package.json` or CI
workflow file yet — the `make ci` target mirrors the gates recorded in `doc/ARCHITECTURE.md` §19.8.
Go is not installed in this environment (`go 1.27.1` required).

```sh
make build                     # static binary into ./bin (CGO_ENABLED=0, version-stamped)
make run ARGS="./somedir"      # go run . with arguments
make dist                      # cross-compile linux/macos amd64+arm64 into ./dist + SHA256SUMS
make test                      # full suite with the race detector
make cover-check               # tests + fail under 80% total coverage
make diagrams                  # render doc/ARCHITECTURE.md mermaid blocks to doc/images/ (needs mmdc)
make verify                    # tidy/fmt/vet/lint + goroutine check + tests (pre-commit)
```

Equivalent raw commands: `go build -o bin/esync .`, `go test -race ./...`,
`go test -cover ./...`, `go vet ./...`, `gofmt -l -w .`. `MAIN_PKG` defaults to `.` (the module
root, per `doc/ARCHITECTURE.md` §17).

## Code Style Guidelines

No `.editorconfig`, `.eslintrc`, `ruff.toml`, or `golangci-lint` config exists in the repo.
Formatting is therefore standard `gofmt` (tabs, Go defaults); run `make fmt vet` (or
`make verify`) before committing. Project-specific rules, all from `doc/ARCHITECTURE.md`:

- **No bare `go` statement.** Every goroutine is started via `obs.Go(ctx, name, fn)`, which installs
  `recover`, converts a panic to `E9001`, and triggers the fatal path (§14.5). CI enforces this with
  a lint rule; `make check-goroutines` is a grep-level pre-check.
- **Catalogued errors only.** Every error returned across a package boundary maps to a code in the
  `E<NNNN>` catalogue (§14.2); no bare wrapped errors escape a package (`I-ERR-01`).
- **stdout is for the pairing code only** — on its own line, undecorated; all logs, progress, and
  human guidance go to stderr (`REQ-CLI-005`).
- **Secrets are unloggable types.** The code, root secret, `K_*` keys, and file content are held in
  types whose `String()`/`LogValue()` render `[redacted]` (§15.9).
- Dependency rule (§17): `obs` and `fault` may be imported anywhere; nothing imports `sender` or
  `receiver` except `main`; `plan` has no network or filesystem-write dependency.

## Testing Instructions

- **Full suite:** `make test` (`go test -race ./...`) — must pass with `-race` (CI gate,
  `doc/ARCHITECTURE.md` §19.8).
- **Single test:** `go test -race -run '^TestName$' ./internal/<pkg>` (use `-v` for per-test
  output).
- **Coverage:** `make cover` prints the total; `make cover-check` fails below the ≥ 80 % convention
  (`COVER_MIN`). Raw: `go test -race -coverprofile=build/coverage.out ./... && go tool cover
  -func=build/coverage.out`.
- **Fuzzing:** `make fuzz` runs every `FuzzXxx` briefly (`FUZZTIME=30s`); or
  `go test -run '^$' -fuzz '^FuzzName$' ./internal/<pkg>` — targets `Z-PAIR-*`, `Z-REC-*`,
  `Z-WIRE-*`, `Z-PATH-*`, `Z-PROTO-*` (§19.5); criterion is zero-crash, zero-hang,
  bounded-allocation.
- **Benchmarks:** `make bench` (`go test -bench . -benchmem ./...`).
- **Suites:** golden vectors `G-` (checked-in input/output pairs, §19.1), property tests `P-`
  (§19.2), end-to-end `E2E-` (real processes over real sockets, byte-for-byte tree equality,
  §19.3), fault injection `F-` (§19.4), benchmarks `B-` (measured and recorded per run, §19.6).
- **Output:** test and benchmark results go to stdout/stderr and the CI log; the `§18.1`
  traceability matrix lists every `T-`/`D-` id whose test must exist, and CI checks that repository
  test names cover it.

## Security Considerations

Derived from `doc/ARCHITECTURE.md` §6 / §12.5 and `doc/INITIAL_REQS.md` §5.3, §6.

- **No secrets at rest, no config file.** Session keys and the root secret live in process memory
  only and are zeroed at every exit path; there is deliberately no configuration file (`P8` /
  `REQ-NFR-003`). Nothing persists after a successful run except the transferred files and any
  explicitly enabled `--log-file` / digest cache. Config precedence is CLI flag > `ESYNC_*` env var
  > default.
- **Pairing code is a bearer secret.** 128-bit secret in the code; single-use; 10-minute expiry
  (`--pair-timeout`); 5-attempt cap (`--max-pair-attempts`). It grants full read of the source
  tree — treat it like a password; `esync <path> | pbcopy` keeps it off the terminal.
- **Transport:** AES-256-CBC with HMAC-SHA-256 encrypt-then-MAC, MAC verified in constant time
  before decryption; fresh random IV per record; per-channel per-direction monotonic sequence
  numbers bound into the MAC (replay/reorder/truncation → `E4004`/`E4005`).
- **Auth / forward secrecy:** X25519 + HKDF-SHA-256 handshake with a SHA-256 transcript hash and
  constant-time confirmation; both sides prove possession of the pairing secret before any tree
  metadata flows; a leaked code cannot decrypt previously captured traffic. Each extra data channel
  re-proves session membership (`CHANNEL_JOIN`/`ACCEPT`).
- **Path safety (receiver):** every manifest path is validated before any filesystem call (reject
  absolute, `..`, NUL, drive/UNC, `.esync`, over-length, Windows-reserved); containment is verified
  structurally with `openat`-style resolution and `O_NOFOLLOW` per component (`E7013`); unsafe
  symlinks rejected (`E7011`) unless `--allow-unsafe-links`; name collisions rejected (`E7012`),
  never silently overwritten. Destination files absent at the source are never deleted.
- **Resource safety:** every wire length is checked against a declared maximum before allocation
  (`REQ-SEC-014`); panics are contained at worker boundaries (`E9001`).
- **Out of scope:** a compromised endpoint host, a user who pastes the code publicly, and traffic
  analysis (an observer learns byte volume and timing, not content).

<ALWAYS_INCLUDE_SECTION>

# RULES

## 1. Think Before Coding : **Don't assume. Don't hide confusion. Surface tradeoffs.**
Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First **Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes **Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution **Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## Pre-Commit Checklist
- [ ] All MFR rules in `doc/MEMORY.md` reviewed
- [ ] No code violates an existing MFR
- [ ] New MFR created/amended if a bug was fixed
- [ ] `doc/MEMORY.md` updated with session log entry
- [ ] `doc/MEMORY.md` under 400 lines (prune if over)
- [ ] `make test` passes with `-race`
- [ ] Coverage ≥ 80% on changed packages
</ALWAYS_INCLUDE_SECTION>

## Further Reading

- [INITIAL_REQS.md](doc/INITIAL_REQS.md) — the baselined requirement set (`ESYNC-REQ`): every
  requirement identified, attributed, prioritised, and given a verification method, plus the §13
  decision log and the §12 request-to-requirement traceability table.
- [ARCHITECTURE.md](doc/ARCHITECTURE.md) — the approved architecture (`ESYNC-ARCH`): design
  principles, component and package model, session state machines, pairing-code and wire formats,
  security architecture, transport/record layers, enumeration and grouping, transfer engine,
  concurrency and flow control, error model, observability, configuration surface, the §18
  requirement→architecture→verification matrix, and the §19 verification strategy.
