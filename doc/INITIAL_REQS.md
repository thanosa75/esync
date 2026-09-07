# esync — Initial Requirements Specification

| Field | Value |
|---|---|
| Document ID | ESYNC-REQ |
| Version | 0.2.0 (baselined) |
| Status | **Approved** — defaults confirmed by the user 2026-09-04 |
| Date | 2026-09-04 |
| Owner | agelatos@gmail.com |
| Supersedes | — |
| Companion | [ARCHITECTURE.md](ARCHITECTURE.md) (ESYNC-ARCH) |

---

## 1. Document Control

### 1.1 Purpose

This document captures the complete, reviewable requirement set for `esync`, derived from the
originating request. It exists so that the analysis can be **verified before any code is written**.
Every requirement is individually identified, attributed to an origin, assigned a priority, and
assigned a verification method. Nothing in the architecture document may exist without tracing
back to a requirement here; nothing here may be left untraced in the architecture.

### 1.2 Keyword Conventions

Keywords are used per RFC 2119 / RFC 8174:

- **MUST** / **MUST NOT** — absolute requirement; a violation is a defect that blocks release.
- **SHOULD** / **SHOULD NOT** — strong recommendation; deviation requires a recorded rationale.
- **MAY** — optional; its absence is not a defect.

### 1.3 Requirement Identifier Format

`REQ-<AREA>-<NNN>`, where `<AREA>` is one of:

| Area | Meaning |
|---|---|
| `CLI` | Command-line surface and user interaction |
| `PAIR` | Pairing code generation, encoding, and consumption |
| `SEC` | Cryptography, authentication, and threat mitigation |
| `NET` | Transport, connectivity, channels |
| `SCAN` | Filesystem enumeration and group formation |
| `HASH` | Content digests and change detection |
| `PROTO` | Wire protocol, message set, state machines |
| `XFER` | File transfer mechanics, integrity, resume |
| `PAR` | Parallelism, flow control, throughput |
| `FS` | Filesystem semantics, metadata, path safety |
| `ERR` | Error taxonomy, handling, recovery |
| `OBS` | Logging, tracing, metrics, progress |
| `NFR` | Non-functional: performance, portability, limits |
| `VER` | Verifiability and testability of the product itself |

Identifiers are **immutable**. A requirement that is withdrawn is marked `WITHDRAWN` in place and
never reused.

### 1.4 Origin Tags

Each requirement records where it came from. This distinguishes what the user asked for from what
the analysis inferred, so that inferences can be challenged.

| Tag | Meaning |
|---|---|
| **S** — Stated | Explicitly present in the originating request. |
| **D** — Derived | A necessary logical consequence of a Stated requirement. |
| **A** — Assumption | Filled a gap the request did not address. **Requires user confirmation.** |
| **C** — Concern | Raised by analysis as a correctness/security issue with a Stated requirement. |

> All **A** and **C** requirements are consolidated in [§13 Decisions](#13-decisions).
> They were the review surface, and all were confirmed on 2026-09-04; the specification is complete.

### 1.5 Verification Methods

| Code | Method | Meaning |
|---|---|---|
| `T` | Test | Automated test (unit, property, integration, fuzz, fault-injection). |
| `D` | Demonstration | Observable behaviour of the running system under a scripted scenario. |
| `I` | Inspection | Code/document review against a checklist. |
| `A` | Analysis | Reasoned argument, model, or measurement over collected data. |

### 1.6 Priority

| Code | Meaning |
|---|---|
| `P0` | Ship-blocking. The product is not the product without it. |
| `P1` | Required for a trustworthy first release. |
| `P2` | Valuable; may be deferred with a recorded decision. |

---

## 2. Problem Statement

A user has two machines on the same local network and wants the contents of a directory on machine
A to appear on machine B. Existing options (rsync over SSH, Syncthing, cloud drives, USB sticks)
each demand setup: account creation, key exchange, daemon installation, firewall configuration, or
persistent configuration state.

`esync` is a **one-shot, zero-configuration, single-direction** transfer. The user runs one command
on the sending machine, copy-pastes a short code to the receiving machine, runs one command there,
and the transfer happens. No accounts, no persistent daemon, no pre-shared configuration, no
lingering state after completion.

## 3. Scope

### 3.1 In Scope

- One sender, one receiver, one directory tree, one run.
- Both hosts on the same LAN segment or otherwise directly IP-reachable.
- Encrypted, authenticated, integrity-checked transfer.
- Differential transfer: files already present and identical at the destination are not re-sent.
- Parallel transfer sized to saturate LAN bandwidth and both disks.
- Structured, level-controlled logging and end-to-end tracing.

### 3.2 Out of Scope (v1 non-goals)

| ID | Non-goal | Rationale |
|---|---|---|
| `NG-01` | Bidirectional / two-way sync | Request specifies one-directional push. |
| `NG-02` | Continuous / daemon-mode watching | Request specifies one-shot. |
| `NG-03` | Deletion of destination files absent at source (mirroring) | Destructive; not requested. See `REQ-FS-060`. |
| `NG-04` | WAN / NAT traversal, relays, hole punching | Request specifies LAN. |
| `NG-05` | Delta-within-file transfer (rsync rolling checksum) | Request specifies whole-file decisions by MD5. |
| `NG-06` | Compression | Not requested; on LAN it is usually a throughput loss. Revisit per `REQ-NFR-050`. |
| `NG-07` | Multi-receiver fan-out from one code | Not requested; a single-use code is safer. See `REQ-SEC-070`. |
| `NG-08` | Windows-specific ACLs, alternate data streams, extended attributes | Not requested. See `REQ-FS-055`. |

### 3.3 Environment Assumptions

| ID | Assumption |
|---|---|
| `ENV-01` | Both machines can open a direct TCP connection to each other on an ephemeral port. |
| `ENV-02` | The user can move ~60 characters of text between the two machines by any means. |
| `ENV-03` | Local clocks may disagree; the protocol MUST NOT depend on clock agreement (`REQ-SEC-075`). |
| `ENV-04` | The LAN is not trusted: other hosts may sniff, spoof, and inject. |
| `ENV-05` | The source tree may be modified while the transfer runs; this must be detected, not ignored. |

---

## 4. Actors and Primary Flow

### 4.1 Actors

| Actor | Description |
|---|---|
| **Sender** | Process started with a source path. Listens, serves manifests and file content. |
| **Receiver** | Process started with a pairing code. Connects, decides what it needs, pulls it, writes it. |
| **User** | One human, operating both machines, who transports the pairing code between them. |
| **LAN adversary** | Untrusted party with full network read/write on the segment. |

### 4.2 Primary Flow (normative narrative)

1. User runs `esync /path/to/dir` on the sender.
2. Sender binds a TCP listener, generates session key material, prints a pairing code, and waits.
3. User copy-pastes the code to the receiver and runs `esync --link <code>`.
4. Receiver decodes the code, connects, and both sides run an authenticated key exchange.
5. Sender enumerates the tree deterministically and partitions it into ordered groups of 1024 files.
6. For each group, the sender computes a digest per file and sends the group manifest.
7. The receiver compares each manifest entry against the destination and decides *needed* vs *skip*.
8. The receiver enqueues needed files and pulls them over several parallel channels.
9. Steps 6–8 pipeline until every group has been decided and every needed file transferred.
10. Both sides exchange a signed session summary, report results, and exit.

---

## 5. Functional Requirements

### 5.1 Command-Line Surface (`CLI`)

**REQ-CLI-001** · S · P0 · V:`D`
Invoking `esync <path>` MUST start a sending session for `<path>`.
*Acceptance:* `esync ./x` on a readable directory prints a pairing code and blocks.

**REQ-CLI-002** · S · P0 · V:`D`
Invoking `esync --link <code>` MUST start a receiving session using `<code>`.
*Acceptance:* a valid code connects to the corresponding sender; an invalid one exits non-zero
with `E2001` before any network I/O.

**REQ-CLI-003** · D · P0 · V:`T`
Exactly one mode MUST be selected. Supplying both a path and `--link`, or neither, MUST fail with
exit code `3` and a usage message naming the conflict.

**REQ-CLI-004** · A · P0 · V:`D`
The receiver MUST write into the current working directory by default, into a subdirectory named
after the source tree's basename. `--dest <dir>` MUST override the destination root.
*Acceptance:* `esync --link C` with a source of `/Users/t/Documents` creates `./Documents/`.

**REQ-CLI-005** · D · P0 · V:`D`
The pairing code MUST be printed to **stdout** on its own line with no decoration, and all human
guidance, progress, and logs MUST go to **stderr**, so that `esync ./x | pbcopy` works.

**REQ-CLI-006** · S · P0 · V:`D`
Both sides MUST render live progress: files done / total, bytes done / total, current aggregate
throughput, and estimated time remaining.

**REQ-CLI-007** · D · P1 · V:`D`
Progress rendering MUST detect a non-TTY stderr and degrade to periodic single-line log records
(default every 5 s) rather than emitting terminal control sequences.

**REQ-CLI-008** · D · P0 · V:`T`
`--help` and `--version` MUST exit `0` and MUST NOT touch the network or filesystem.
`--version` MUST print the semantic version, git commit, build date, and protocol version.

**REQ-CLI-009** · A · P1 · V:`D`
`--dry-run` (receiver) MUST perform pairing, manifest exchange, and the full needed/skip decision,
report exactly what would be transferred, and write nothing to disk.

**REQ-CLI-010** · D · P0 · V:`D`
`SIGINT`/`SIGTERM` MUST trigger graceful shutdown on either side: stop issuing new work, flush and
close in-flight writes to a resumable state, emit a summary, and exit `5`. A second signal MUST
force immediate exit.

**REQ-CLI-011** · A · P2 · V:`D`
`--include` / `--exclude` glob patterns (repeatable, applied to the source-relative path,
`.gitignore`-style precedence, last match wins) MUST filter the source tree if supplied. Filtering
is applied **before** grouping, so groups remain reproducible for a given pattern set
(see `REQ-SCAN-025`).

### 5.2 Pairing Code (`PAIR`)

**REQ-PAIR-001** · S · P0 · V:`T`
The sender MUST produce a single code that carries everything the receiver needs to connect and
authenticate: protocol version, candidate network endpoints, a session identifier, and secret key
material.

**REQ-PAIR-002** · S · P0 · V:`A`
The code MUST be short enough to copy-paste by hand and MUST be transcribable by voice or by
retyping. Target: **≤ 64 significant characters** for the single-endpoint case.

**REQ-PAIR-003** · D · P0 · V:`T`
The code alphabet MUST be case-insensitive and MUST exclude visually ambiguous characters
(`I`, `L`, `O`, `U` and digits `0`,`1`). Decoding MUST accept any case and MUST ignore separators
(`-`, space) so that line-wrapped or hyphenated pastes work.

**REQ-PAIR-004** · D · P0 · V:`T`
The code MUST embed an integrity check that detects any single-character substitution, any
transposition of adjacent characters, and any truncation, and MUST report `E2001` (malformed code)
rather than attempting to connect.

**REQ-PAIR-005** · D · P0 · V:`T`
The code MUST carry a protocol version. A receiver that does not implement that version MUST fail
with `E2003` naming both versions, before connecting.

**REQ-PAIR-006** · D · P0 · V:`T`
The code MUST carry **all** plausible LAN endpoints of the sender (multi-homed hosts, Wi-Fi + wired,
IPv4 and IPv6), not just one, up to a bounded count of 4. Loopback and link-local addresses MUST be
excluded unless no other address exists.

**REQ-PAIR-007** · C · P0 · V:`T`
The code is a bearer secret. It MUST be single-use: once one receiver completes the authenticated
handshake, the sender MUST refuse all further handshake attempts with `E2005`.

**REQ-PAIR-008** · A · P1 · V:`T`
An unclaimed code MUST expire. The sender MUST stop listening and exit `2` with `E2006` after
`--pair-timeout` (default **10 minutes**) with no successful handshake.

**REQ-PAIR-009** · D · P1 · V:`I`
The full pairing code MUST NOT appear in any log record at any level, in any error message, or in
any crash dump. Logs MUST reference the session by its non-secret session ID only.

### 5.3 Security (`SEC`)

**REQ-SEC-001** · S · P0 · V:`T`
All application data on the wire MUST be encrypted with **AES in CBC mode**, using key material
derived from the pairing code.

**REQ-SEC-002** · C · P0 · V:`T`
CBC provides confidentiality only. Every encrypted record MUST additionally carry a
**MAC computed over the ciphertext (encrypt-then-MAC)**, verified in constant time before any
decryption or parsing. This closes padding-oracle, bit-flipping, and injection attacks that raw
CBC leaves open. *This is an analysis-raised addition to `REQ-SEC-001`; see [§13](#13-decisions).*

**REQ-SEC-003** · D · P0 · V:`T`
Key strength MUST be 256-bit for encryption and 256-bit for authentication.

**REQ-SEC-004** · D · P0 · V:`T`
Each record MUST use a fresh, unpredictable IV from a cryptographically secure RNG. IVs MUST NOT be
chained across records (no TLS-1.0-style predictable-IV construction).

**REQ-SEC-005** · D · P0 · V:`T`
Each direction MUST use independent encryption and authentication keys. A record produced by one
side MUST NOT be verifiable as a record from the other side (reflection resistance).

**REQ-SEC-006** · D · P0 · V:`T`
Every record MUST carry a monotonically increasing sequence number, bound into the MAC, so that
replay, reordering, and dropping of records is detected. Detection MUST be fatal (`E4004`).

**REQ-SEC-007** · D · P0 · V:`T`
The session MUST be terminated by an authenticated close message. A connection that ends without it
MUST be reported as truncated (`E4005`), never as success.

**REQ-SEC-008** · D · P0 · V:`T`
The handshake MUST authenticate **both** parties by proof of possession of the pairing secret,
before any file metadata is exchanged. An unauthenticated peer MUST learn nothing about the source
tree — not path names, not file count, not total size.

**REQ-SEC-009** · C · P1 · V:`T`
The handshake SHOULD provide forward secrecy, so that a pairing code captured later (from shell
history, a chat log, or a screenshot) does not decrypt a previously captured session.

**REQ-SEC-010** · D · P0 · V:`T`
Handshake failures MUST be rate-limited and capped. After `N` (default 5) failed attempts the sender
MUST stop listening and exit with `E2007`. This bounds online guessing against the code.

**REQ-SEC-011** · D · P0 · V:`T`
All secret-comparison operations (MACs, confirmation tags, checksums over secret material) MUST be
constant-time.

**REQ-SEC-012** · D · P0 · V:`I`
Key material MUST be zeroed in memory at session end and MUST never be written to disk, to logs, or
to any resume journal.

**REQ-SEC-013** · D · P0 · V:`T`
Every auxiliary connection (data channels) MUST be independently authenticated against the same
session, and MUST be bound to the session such that it cannot be replayed against a different
session.

**REQ-SEC-014** · D · P1 · V:`T`
Every length field parsed from the wire MUST be bounds-checked against a declared maximum **before**
allocation. A hostile or corrupt length MUST NOT cause an unbounded allocation.

**REQ-SEC-015** · D · P0 · V:`T`
The receiver MUST NOT trust any path supplied by the sender. Path traversal, absolute paths, and
symlink-mediated escapes from the destination root MUST be rejected — see `REQ-FS-040`..`REQ-FS-045`.

**REQ-SEC-016** · A · P1 · V:`D`
`--bind <addr>` MUST restrict the sender's listener to one interface, and the emitted code MUST then
advertise only that endpoint.

### 5.4 Networking and Channels (`NET`)

**REQ-NET-001** · D · P0 · V:`T`
Transport MUST be TCP. The sender MUST listen; the receiver MUST connect. This keeps the receiver
behind an unmodified host firewall in the common case.

**REQ-NET-002** · D · P0 · V:`T`
The receiver MUST attempt the code's candidate endpoints concurrently (staggered start) and MUST
proceed with the first that completes the handshake, cancelling the rest. Total connect budget:
`--connect-timeout`, default **10 s**.

**REQ-NET-003** · D · P0 · V:`T`
If every candidate endpoint fails, the receiver MUST report `E3001` and MUST list each endpoint with
its individual failure cause (refused / unreachable / timeout / handshake rejected).

**REQ-NET-004** · S · P0 · V:`T`
The session MUST support multiple concurrent TCP connections: one **control channel** and *N*
**data channels**.

**REQ-NET-005** · D · P0 · V:`T`
Control-plane messages MUST NOT be blocked behind bulk file data. Manifests, decisions, credit
grants, and errors MUST make progress while large files are streaming.

**REQ-NET-006** · D · P1 · V:`D`
Sockets MUST be configured for throughput: `TCP_NODELAY` on the control channel, large socket
buffers on data channels, and platform zero-copy paths used where available.

**REQ-NET-007** · D · P0 · V:`T`
Both sides MUST run a keepalive over the control channel (default: probe at 15 s idle, declare dead
at 45 s). A dead peer MUST produce `E3004`, not an indefinite hang.

**REQ-NET-008** · D · P1 · V:`T`
Loss of a **data** channel MUST NOT abort the session: the channel's in-flight work MUST be requeued
and the channel re-established, up to a retry budget.

**REQ-NET-009** · D · P0 · V:`T`
Loss of the **control** channel MUST suspend the session and trigger the resume path (`REQ-XFER-040`),
not a silent partial success.

**REQ-NET-010** · D · P1 · V:`T`
Every blocking network operation MUST have a timeout. No code path may wait forever.

### 5.5 Enumeration and Grouping (`SCAN`)

**REQ-SCAN-001** · S · P0 · V:`T`
The sender MUST enumerate the source path **recursively**.

**REQ-SCAN-010** · S · P0 · V:`T`
Files MUST be partitioned into **groups of exactly 1024 files**, except the final group, which holds
the remainder.

**REQ-SCAN-020** · S · P0 · V:`T`
Grouping MUST be **reproducible**: two enumerations of an unchanged tree, on any supported platform,
MUST produce identical groups with identical membership and identical ordering.
*Acceptance:* property test — for any generated tree, two independent scans produce byte-identical
manifest digests; and the digest is stable across Linux and macOS runs on the same tree.

**REQ-SCAN-021** · D · P0 · V:`T`
The ordering key MUST be the byte sequence of the source-relative path, compared with unsigned byte
ordering. Locale, case-folding, and filesystem readdir order MUST NOT influence it.

**REQ-SCAN-022** · D · P0 · V:`T`
Path separators MUST be normalised to `/` on the wire and in the ordering key, regardless of host
platform.

**REQ-SCAN-023** · C · P0 · V:`T`
Unicode normalisation differences (macOS stores NFD, Linux stores whatever was written) MUST be
handled explicitly: the ordering key MUST be computed over a defined normalisation form, and the
original bytes MUST also be transmitted so the receiver can reproduce the exact name where the
platform allows it.

**REQ-SCAN-024** · D · P0 · V:`T`
Each file MUST receive a stable **file ID** equal to its zero-based index in the global sorted order.
Group ID MUST be `file_id / 1024`. Both sides MUST be able to derive one from the other without extra
state.

**REQ-SCAN-025** · D · P0 · V:`T`
Filters (`REQ-CLI-011`) MUST be applied before ID assignment, and the active filter set MUST be
included in the manifest digest, so that a resumed session with different filters is detected as a
different session rather than silently mis-mapping IDs.

**REQ-SCAN-026** · S · P0 · V:`T`
OS-generated metadata and system files MUST NOT be transferred. The sender MUST exclude the
**default system-file exclusion set** (ESYNC-ARCH §10.1.1), which covers macOS, Windows, and
Linux/Unix, from the plan entirely: excluded entries MUST NOT receive a file ID, MUST NOT appear in
any manifest, and MUST NOT contribute to the totals. Directory-type entries in the set MUST be
**pruned** — the walk MUST NOT descend into them. Exclusion MUST be by name and the **union of all
three platform tables MUST apply on every platform**, so that a tree is cleaned regardless of which
OS created the debris or which OS is sending it. Patterns marked *root only* MUST match solely at
the top level of the source tree.
*Acceptance:* a source tree seeded with `.DS_Store`, `._Foo.txt`, `.Spotlight-V100/`, `__MACOSX/`,
`Thumbs.db`, `desktop.ini`, `$RECYCLE.BIN/`, `System Volume Information/`, `.Trash-1000/`,
`lost+found/`, and `.directory` transfers with none of them present at the destination, and the
summary reports the excluded count. A nested `lost+found/` is **kept**, and `THUMBS.DB` is excluded.

**REQ-SCAN-027** · D · P1 · V:`T`
The default exclusion set MUST be overridable by `--keep-system-files`, and the identity of the
active set (its version tag, plus whether the override is in effect) MUST be part of the manifest
digest input alongside the user filter set (`REQ-SCAN-025`). A session resumed with a different
exclusion policy MUST therefore be detected as a different plan rather than silently mis-mapping
file IDs.

**REQ-SCAN-030** · D · P0 · V:`T`
Enumeration MUST NOT follow symbolic links by default (loop safety). `--follow-symlinks` MUST enable
following, and when enabled, MUST detect and break cycles via visited (device, inode) tracking.

**REQ-SCAN-031** · D · P0 · V:`T`
Enumeration MUST NOT cross filesystem boundaries by default. `--one-file-system=false` MUST allow it.

**REQ-SCAN-032** · D · P0 · V:`T`
Per-entry enumeration failures (permission denied, vanished during walk, I/O error) MUST be recorded
as skipped-with-reason and MUST NOT abort the scan. The count MUST appear in the final summary and
MUST influence the exit code (`REQ-ERR-030`).

**REQ-SCAN-033** · D · P1 · V:`T`
Enumeration MUST operate in bounded memory. Above a threshold entry count, the entry list MUST spill
to a temporary file and be externally sorted. Memory MUST NOT scale linearly with tree size without
bound.
*Acceptance:* a 5,000,000-entry synthetic tree completes within a declared RSS ceiling.

**REQ-SCAN-034** · D · P1 · V:`D`
While scanning, the sender MUST emit progress (entries seen, bytes seen, elapsed) so that a long scan
on a large tree does not look like a hang.

**REQ-SCAN-035** · D · P0 · V:`T`
The sender MUST publish the totals (file count, byte count, group count, manifest digest) to the
receiver once the scan completes, before or alongside the first group manifest.

**REQ-SCAN-036** · D · P0 · V:`T`
An empty source tree MUST be a successful no-op session, not an error.

**REQ-SCAN-037** · D · P0 · V:`T`
A source path that is a single regular file (not a directory) MUST be accepted and transferred as a
one-file tree.

### 5.6 Hashing and Change Detection (`HASH`)

**REQ-HASH-001** · S · P0 · V:`T`
The sender MUST compute an **MD5** digest for each file in a group and include it in the group
manifest.

**REQ-HASH-002** · C · P1 · V:`I`
MD5 is collision-broken. Within this design the peer is authenticated and the digest is used only for
change detection, so the practical exposure is limited to a source tree that *already* contains
adversarially crafted colliding files. The digest algorithm MUST nevertheless be a negotiated field
in the protocol, defaulting to MD5, so a stronger digest can be selected without a protocol break.
*See [§13](#13-decisions).*

**REQ-HASH-003** · S · P0 · V:`T`
The receiver MUST compare each manifest entry against the corresponding destination path and decide
per file whether it must be transferred.

**REQ-HASH-004** · D · P0 · V:`T`
The decision rule MUST be: transfer if the destination entry is absent, is of a different type, has a
different size, or has a different digest. Otherwise skip.

**REQ-HASH-005** · D · P1 · V:`T`
Both sides MUST maintain a digest cache keyed by (device, inode, size, mtime, ctime) so that
unchanged files are not re-read on subsequent runs. A cache entry MUST be invalidated if any key
component changes.
*Acceptance:* second run over an unchanged tree performs no content reads on either side.

**REQ-HASH-006** · D · P1 · V:`T`
The digest cache MUST be advisory. Corruption, absence, or a version mismatch of the cache MUST
degrade to full hashing, never to an incorrect skip and never to a crash.

**REQ-HASH-007** · A · P2 · V:`T`
`--quick` MUST offer a size+mtime-only comparison that skips content hashing entirely on both sides,
with the reduced guarantee stated in `--help` and logged at `INFO` at session start.

**REQ-HASH-008** · D · P0 · V:`T`
Digests MUST be computed over file content only. Metadata MUST NOT contribute to the digest.

**REQ-HASH-009** · D · P0 · V:`T`
Hashing MUST be parallel across files, bounded by a configurable worker count, and MUST NOT starve
the network path.

### 5.7 Protocol and Session (`PROTO`)

**REQ-PROTO-001** · D · P0 · V:`T`
The protocol MUST be fully specified as a byte-level wire format with an explicit message set, and
both endpoints MUST be implementable independently from the specification alone.

**REQ-PROTO-002** · D · P0 · V:`T`
Both endpoints MUST be modelled as explicit finite state machines. Every message MUST have a defined
handling in every state, including "not valid here", which MUST be a protocol violation (`E5001`) and
MUST terminate the session.

**REQ-PROTO-003** · S · P0 · V:`T`
The sender MUST push group manifests (group ID + per-file identity and digest); the receiver MUST
respond with a per-group decision listing the files it needs.

**REQ-PROTO-004** · S · P0 · V:`T`
The receiver MUST request needed files from a queue, and MUST be able to have multiple requests
outstanding concurrently across channels.

**REQ-PROTO-005** · S · P0 · V:`T`
The session MUST run until every group has been manifested and every group has been decided
(requested or skipped), then terminate.

**REQ-PROTO-006** · D · P0 · V:`T`
The protocol MUST carry an explicit version, negotiated at handshake. Version mismatch MUST be a
clean, named failure (`E5002`), never undefined behaviour.

**REQ-PROTO-007** · D · P1 · V:`T`
Unknown-but-well-formed optional fields MUST be ignorable by an older peer without breaking framing,
so that additive protocol evolution is possible.

**REQ-PROTO-008** · D · P0 · V:`T`
Every request MUST carry a correlation ID, echoed in its response, so that concurrent in-flight work
is unambiguously attributable in logs and in traces.

**REQ-PROTO-009** · D · P0 · V:`T`
The session MUST end with a summary exchange: counts of files transferred, skipped, failed, bytes
moved, and a digest over the set of completed file IDs, so both sides independently agree on the
outcome. Disagreement MUST be reported as `E5005`.

### 5.8 Transfer and Integrity (`XFER`)

**REQ-XFER-001** · S · P0 · V:`T`
The sender MUST transmit the content of each requested file.

**REQ-XFER-002** · D · P0 · V:`T`
File content MUST be chunked into bounded records so that a single large file cannot monopolise
memory and so that progress is observable at sub-file granularity.

**REQ-XFER-010** · D · P0 · V:`T`
The receiver MUST verify the digest of every received file against the manifest before publishing it.
A mismatch MUST discard the data, log `E8001`, and retry up to the retry budget.

**REQ-XFER-011** · D · P0 · V:`T`
Files MUST be written to a temporary path and made visible at the final path by an **atomic rename**
only after full receipt and successful verification. A destination file MUST never be observable in a
partially written state.

**REQ-XFER-012** · D · P0 · V:`T`
The sender MUST detect that a file changed while being read (size drift or digest mismatch against the
manifest) and MUST report it as a per-file error (`E6003`) rather than shipping inconsistent bytes.

**REQ-XFER-013** · D · P0 · V:`T`
A file that vanishes between manifest and request MUST produce a per-file error (`E6002`), MUST NOT
abort the session, and MUST be reported in the summary.

**REQ-XFER-014** · D · P1 · V:`T`
The receiver MUST pre-check available free space against the total needed byte count and MUST warn
before starting when it is insufficient. Running out mid-transfer MUST produce `E7003` and a clean,
resumable stop.

**REQ-XFER-020** · D · P0 · V:`T`
Per-file failures MUST be retried with exponential backoff up to `--max-retries` (default 3). Only
after exhaustion is a file marked failed.

**REQ-XFER-021** · D · P0 · V:`T`
A retried file MAY be served by a different data channel than the one that failed.

**REQ-XFER-030** · D · P1 · V:`D`
`fsync` policy MUST be explicit and configurable: default is to fsync each file before rename and to
fsync the containing directory at group completion. `--no-fsync` MUST document the durability
trade-off it accepts.

**REQ-XFER-040** · A · P1 · V:`T`
The receiver MUST journal completed file IDs so that an interrupted session can be resumed rather
than restarted. Resume MUST verify that the source manifest digest is unchanged; if it changed, the
session MUST restart cleanly (already-correct files are then skipped by digest anyway, so no data is
re-sent unnecessarily).

**REQ-XFER-041** · D · P1 · V:`T`
Partial files from an interrupted session MUST be either resumed from a verified offset or discarded
— never silently completed from mixed content.

**REQ-XFER-042** · D · P1 · V:`D`
On successful completion the receiver MUST remove its temporary/journal working state. On failure it
MUST retain it and MUST tell the user exactly how to resume.

### 5.9 Parallelism and Flow Control (`PAR`)

**REQ-PAR-001** · S · P0 · V:`D`
The receiver MUST parallelise file requests to maximise LAN bandwidth utilisation.
*Acceptance:* on a 1 GbE link with many medium files, sustained aggregate throughput ≥ 85 % of the
single-stream `iperf3` baseline for the same link.

**REQ-PAR-002** · S · P0 · V:`D`
Parallelism MUST also target disk throughput on both ends: concurrent reads on the sender and
concurrent writes on the receiver, with independently bounded pools.

**REQ-PAR-003** · D · P0 · V:`T`
Concurrency MUST be bounded at every stage — connections, in-flight requests, hash workers, disk
readers, disk writers, and queued groups — with documented defaults and CLI overrides. No unbounded
goroutine/thread/queue growth is permitted anywhere.

**REQ-PAR-004** · D · P1 · V:`D`
The receiver SHOULD adapt its channel count at runtime based on measured aggregate throughput,
within `[--min-channels, --max-channels]`, and MUST log every adaptation decision at `DEBUG`.

**REQ-PAR-005** · D · P0 · V:`T`
The sender MUST NOT be able to flood the receiver with manifests. The receiver MUST grant explicit
credit for the number of un-decided groups it will accept (default 4).

**REQ-PAR-006** · D · P0 · V:`T`
Backpressure MUST propagate end to end: a slow receiver disk MUST slow the network reads, which MUST
slow the sender's disk reads, without unbounded buffering at any hop.

**REQ-PAR-007** · D · P1 · V:`A`
Work distribution MUST avoid a long tail: a single huge file MUST NOT leave other channels idle while
the queue still holds work. Ordering within a group SHOULD be largest-first to shorten the tail.

**REQ-PAR-008** · D · P0 · V:`T`
Manifest production (scan + hash) MUST pipeline with transfer. The sender MUST NOT hash the entire
tree before the first byte moves.

### 5.10 Filesystem Semantics (`FS`)

**REQ-FS-001** · D · P0 · V:`T`
Regular files MUST be transferred. Directories MUST be created, including empty ones.

**REQ-FS-002** · D · P0 · V:`T`
Symbolic links MUST be transferred as links (target recorded verbatim), not dereferenced, when
`--follow-symlinks` is off.

**REQ-FS-003** · D · P1 · V:`T`
Hard links within the source tree SHOULD be detected and reproduced at the destination; when not
reproducible, the content MUST be materialised as separate files and the degradation logged at `WARN`.

**REQ-FS-004** · D · P0 · V:`T`
Non-regular, non-directory, non-symlink entries (sockets, FIFOs, devices) MUST be skipped with a
`WARN` and counted in the summary. They MUST NOT abort the session.

**REQ-FS-005** · D · P1 · V:`T`
Sparse files MUST NOT be expanded into their full allocated size at the destination where the platform
supports sparse writes.

**REQ-FS-010** · D · P0 · V:`T`
POSIX permission bits and modification time MUST be preserved. Ownership MUST NOT be preserved by
default (it usually requires privilege and rarely maps across machines); `--owner` MAY request it.

**REQ-FS-011** · D · P2 · V:`T`
Extended attributes and ACLs MUST NOT be transferred in v1, and their omission MUST be stated in
`--help` rather than silently assumed.

**REQ-FS-040** · D · P0 · V:`T`
The receiver MUST reject any manifest path that is absolute, contains a `..` component, contains a
NUL byte, is empty, or is not valid for the destination platform. Rejection is `E7010`, per-file, and
counted.

**REQ-FS-041** · D · P0 · V:`T`
Every write MUST be confined to the destination root. The receiver MUST verify containment after
resolving the parent directory, not merely by string inspection.

**REQ-FS-042** · D · P0 · V:`T`
The receiver MUST NOT write through an existing symlink at any component of the destination path.

**REQ-FS-043** · D · P0 · V:`T`
A received symlink whose target escapes the destination root MUST be rejected by default
(`E7011`); `--allow-unsafe-links` MUST be required to create it.

**REQ-FS-044** · D · P1 · V:`T`
Destination-platform name collisions (e.g. two source names differing only by case or by Unicode
normalisation landing on one destination name) MUST be detected and reported as `E7012`, not silently
overwritten.

**REQ-FS-045** · D · P1 · V:`T`
The receiver MUST refuse to write into a destination root that is inside the source tree when both
are on the same machine (self-copy recursion guard).

**REQ-FS-050** · D · P1 · V:`T`
Long paths, deep nesting, and names near platform limits MUST fail per-file with a clear code, never
with a truncated or corrupted destination name.

**REQ-FS-055** · A · P2 · V:`I`
Windows support is not in v1 scope. Path handling MUST nevertheless be written so that the
platform-specific validation layer is a single, replaceable component.

**REQ-FS-060** · D · P0 · V:`I`
The receiver MUST NOT delete or truncate any destination file that is not part of the transfer set.
`esync` is additive by definition in v1 (`NG-03`).

### 5.11 Errors and Recovery (`ERR`)

**REQ-ERR-001** · S · P0 · V:`I`
Every failure mode MUST be enumerated with a stable identifier, a severity, a defined retry policy,
and a defined operator action. There MUST be no unclassified error path.

**REQ-ERR-002** · D · P0 · V:`I`
Errors MUST be classified as **fatal** (session ends) or **per-item** (session continues, item
recorded). Classification MUST be a property of the error code, not of the call site.

**REQ-ERR-003** · D · P0 · V:`T`
Every user-visible error MUST state: what was attempted, what failed, the identifier, and the next
action available to the user. Bare wrapped system errors are not acceptable.

**REQ-ERR-004** · D · P0 · V:`T`
Transient network failures MUST be retried with exponential backoff and jitter, with a bounded budget.
Permanent failures MUST NOT be retried.

**REQ-ERR-005** · D · P0 · V:`T`
Fatal errors MUST be reported to the peer over the authenticated channel before disconnecting, when
the channel still works, so both sides log the same cause.

**REQ-ERR-006** · D · P0 · V:`I`
Panics/uncaught exceptions MUST be trapped at every worker boundary, converted to `E9001`, logged with
a stack trace, and MUST fail the session cleanly rather than leaving a hung process or a half-written
file.

**REQ-ERR-030** · D · P0 · V:`T`
Exit codes MUST be: `0` complete success; `1` completed with per-item failures or skips; `2` fatal
error; `3` usage error; `4` authentication/pairing failure; `5` interrupted by signal.

**REQ-ERR-031** · D · P0 · V:`D`
The final summary MUST always be emitted, on every exit path except a forced second signal, and MUST
list per-item failures with their reasons.

### 5.12 Observability (`OBS`)

**REQ-OBS-001** · S · P0 · V:`D`
The system MUST have built-in logging with levels `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`.

**REQ-OBS-002** · S · P0 · V:`D`
The default level MUST be `INFO`, showing meaningful error, warning, and informational output and
nothing else.

**REQ-OBS-003** · S · P0 · V:`D`
Verbosity MUST increase progressively: `-v` → `DEBUG`, `-vv` → `TRACE`; `-q` → `WARN`, `-qq` → `ERROR`.
`--log-level <name>` MUST set it explicitly and MUST win over the shorthand flags.

**REQ-OBS-004** · D · P0 · V:`I`
Each level MUST have a documented contract stating what belongs in it, so that level choice is
reviewable rather than a matter of taste. See ARCH §15.2.

**REQ-OBS-005** · D · P0 · V:`D`
Logs MUST be structured (typed key/value fields). `--log-format json` MUST emit one JSON object per
record; the default `text` format MUST stay human-readable.

**REQ-OBS-010** · S · P0 · V:`T`
The system MUST be **traceable**: every log record MUST carry the session ID, and where applicable the
group ID, file ID, request ID, and channel ID, so that any single file's journey can be reconstructed
from either side's logs.

**REQ-OBS-011** · D · P0 · V:`T`
The session ID MUST be identical on both sides, so that the two logs can be joined into one timeline.

**REQ-OBS-012** · D · P1 · V:`D`
Long operations MUST be represented as spans with a start, an end, a duration, and an outcome, so that
time is attributable to scan, hash, wait, network, and disk.

**REQ-OBS-013** · D · P1 · V:`D`
The system MUST report end-of-session counters and timings: bytes read/written, files by outcome,
network vs disk time, retry counts, and per-stage stall time.

**REQ-OBS-014** · D · P0 · V:`I`
Logs MUST NOT contain key material, the pairing code, or file content. Path names MAY be logged; a
`--redact-paths` flag MUST replace them with their file ID.

**REQ-OBS-015** · D · P1 · V:`D`
`--log-file <path>` MUST write the full-fidelity log to a file independently of the console level, so
a user can keep `INFO` on screen and `TRACE` on disk in the same run.

**REQ-OBS-016** · D · P1 · V:`T`
Logging MUST be non-blocking and MUST never deadlock or measurably throttle the transfer. Under
saturation the logger MUST drop records and MUST record the drop count.

**REQ-OBS-017** · D · P1 · V:`D`
At `TRACE`, every protocol message MUST be logged with its type, size, and correlation ID (never its
payload bytes), sufficient to reconstruct the message exchange.

### 5.13 Non-Functional (`NFR`)

**REQ-NFR-001** · D · P0 · V:`I`
`esync` MUST be a single self-contained binary with no runtime dependencies and no installation step.

**REQ-NFR-002** · D · P0 · V:`I`
`esync` MUST NOT require elevated privileges.

**REQ-NFR-003** · D · P0 · V:`I`
`esync` MUST leave no persistent state on either machine after a successful run, other than the
transferred files and any explicitly enabled cache/log file.

**REQ-NFR-010** · D · P1 · V:`D`
Memory use MUST be bounded and independent of tree size beyond the documented spill threshold, and
independent of individual file size.

**REQ-NFR-011** · D · P1 · V:`D`
Time-to-first-byte after pairing SHOULD be under 2 s for a tree of 10,000 files: the first group must
not wait for the whole tree.

**REQ-NFR-020** · D · P1 · V:`D`
Supported platforms for v1: Linux (x86-64, arm64) and macOS (arm64, x86-64). Cross-platform transfers
between them MUST work in both directions.

**REQ-NFR-050** · A · P2 · V:`A`
Compression is out of scope for v1 (`NG-06`). If added, it MUST be negotiated and MUST be off by
default on links faster than a documented threshold.

### 5.14 Verifiability of the Product (`VER`)

**REQ-VER-001** · S · P0 · V:`I`
Every requirement in this document MUST map to at least one architecture section and at least one
verification artefact. The traceability matrix in ARCH §18 is the record.

**REQ-VER-002** · D · P0 · V:`T`
The pairing-code codec, the key schedule, and the record layer MUST each have fixed golden test
vectors, so that an independent implementation can be validated against the specification.

**REQ-VER-003** · D · P0 · V:`T`
Group formation MUST have a property test asserting determinism over randomly generated trees
(`REQ-SCAN-020`).

**REQ-VER-004** · D · P0 · V:`T`
All wire-format parsers MUST be fuzz-tested against malformed, truncated, and hostile input, with a
zero-crash, zero-hang, bounded-allocation acceptance criterion.

**REQ-VER-005** · D · P0 · V:`T`
Fault injection MUST be a first-class test capability: connection drops, corrupted records, slow
peers, disk-full, permission-denied, and mid-transfer source mutation MUST each have a test that
asserts the specified behaviour.

**REQ-VER-006** · D · P1 · V:`T`
An end-to-end integration test MUST run both sides as real processes over real sockets against a
generated tree, and MUST assert byte-for-byte tree equality plus preserved metadata.

**REQ-VER-007** · D · P1 · V:`D`
A repeatable throughput benchmark MUST exist so that `REQ-PAR-001` and `REQ-PAR-002` are measured
rather than asserted.

---

## 6. Data the System Handles

| Datum | Where it lives | Sensitivity | Lifetime |
|---|---|---|---|
| Pairing code | Sender stdout, user's clipboard | **Secret** — grants full access to the source tree | Until claimed or expired |
| Root secret / session keys | Process memory only | **Secret** | Session; zeroed at end |
| Session ID | Both sides' logs | Non-secret correlation handle | Session + log retention |
| Path names | Manifests, logs | Potentially sensitive | Session + log retention |
| File content | Wire (encrypted), destination disk | User's data | Permanent at destination |
| Digest cache | Optional local cache file | Low; reveals path + digest | Until invalidated |
| Resume journal | Receiver's working dir | Low | Until session completes |

## 7. Constraints

| ID | Constraint | Source |
|---|---|---|
| `CON-01` | Implementation language is Go (`go.mod` declares `module esync`, `go 1.27.1`). | Repository |
| `CON-02` | Encryption must be AES-CBC. | Stated |
| `CON-03` | Content digest must be MD5. | Stated |
| `CON-04` | Group size must be 1024 files. | Stated |
| `CON-05` | Pairing is code-based; no accounts, no certificates, no key files. | Stated |

## 8. Success Criteria

The specification is accepted when:

1. Every open point in [§13](#13-decisions) has a user decision. — **met 2026-09-04**
2. Every requirement traces to an architecture section and a verification artefact (`REQ-VER-001`).
3. No requirement is contradicted by another; the consistency review in ARCH §18.3 is clean.

The **product** is accepted when every `P0` and `P1` requirement passes its stated verification.

## 9. Glossary

| Term | Definition |
|---|---|
| **Pairing code** | The short string the sender prints and the receiver consumes; carries endpoints + secret. |
| **Session** | One complete sender↔receiver interaction, from handshake to summary. |
| **Control channel** | The single connection carrying manifests, decisions, credit, and lifecycle messages. |
| **Data channel** | One of *N* connections carrying file requests and file content. |
| **Group** | An ordered set of exactly 1024 files (final group: the remainder). |
| **Manifest** | The sender's description of a group: per-file identity, metadata, and digest. |
| **Decision** | The receiver's reply to a manifest: which file IDs it needs. |
| **File ID** | Zero-based index of a file in the global sorted enumeration; stable and shared. |
| **Manifest digest** | A digest over the whole enumeration, used to detect that the source changed shape. |
| **Credit** | Receiver-granted permission for the sender to have *k* manifests outstanding. |

## 10. Requirement Count by Area

| Area | Count | P0 | P1 | P2 |
|---|---|---|---|---|
| CLI | 11 | 8 | 2 | 1 |
| PAIR | 9 | 7 | 2 | 0 |
| SEC | 16 | 13 | 3 | 0 |
| NET | 10 | 7 | 3 | 0 |
| SCAN | 18 | 15 | 3 | 0 |
| HASH | 9 | 5 | 3 | 1 |
| PROTO | 9 | 8 | 1 | 0 |
| XFER | 13 | 8 | 5 | 0 |
| PAR | 8 | 6 | 2 | 0 |
| FS | 16 | 9 | 5 | 2 |
| ERR | 8 | 8 | 0 | 0 |
| OBS | 13 | 8 | 5 | 0 |
| NFR | 7 | 3 | 3 | 1 |
| VER | 7 | 5 | 2 | 0 |
| **Total** | **154** | **110** | **39** | **5** |

---

## 11. Explicitly Rejected Alternatives

| Alternative | Why rejected |
|---|---|
| UDP / QUIC transport | Adds congestion-control and reliability work with no LAN benefit; TCP with multiple streams meets `REQ-PAR-001`. Revisit only if measurement says otherwise. |
| Receiver listens, sender connects | The code would have to travel the wrong way; the sender is the party with the data and the party that generates the code. |
| Sender pushes files unsolicited | Contradicts `REQ-PROTO-004`; the receiver is the only party that knows what it already has and how fast it can write. |
| One connection with application-level multiplexing | Simpler wire format, but a single TCP stream is one congestion-control instance and one kernel-buffer path; multiple connections give better disk and NIC parallelism and avoid head-of-line blocking (`REQ-NET-005`). |
| Trust-on-first-use with no pre-shared secret | Leaves the LAN adversary a working MITM window (`ENV-04`, `REQ-SEC-008`). |
| Streaming enumeration without a global sort | Cheaper, but breaks reproducible grouping (`REQ-SCAN-020`). |

---

## 12. Traceability of Stated Requirements

Every sentence of the originating request, mapped to requirement IDs. This is the primary check that
nothing was dropped.

| # | Original statement | Requirements |
|---|---|---|
| 1 | "easy, oneshot, sync between systems within a local lan" | `NFR-001`,`NFR-002`,`NFR-003`, `NG-02`, `NG-04` |
| 2 | "sending side receives a path to sync (like `esync /Users/tangelatos/Documents`)" | `CLI-001`, `SCAN-037` |
| 3 | "produces a small code that can be copy-pasted" | `PAIR-001`,`PAIR-002`,`PAIR-003`, `CLI-005` |
| 4 | "…that includes connectivity and a hash key" | `PAIR-001`,`PAIR-006`, `SEC-001` |
| 5 | "receiving side is executed with `esync --link <code>`" | `CLI-002` |
| 6 | "when the two sides link, they use the provided info … to generate AES CBC between them" | `SEC-001`..`SEC-008` |
| 7 | "sending side proceeds to send recursively all files from the specified folder" | `SCAN-001`, `XFER-001` |
| 8 | "breaks the operation in 'file groups' — groups of 1024 files" | `SCAN-010` |
| 9 | "this should be a reproducible grouping (like always sorting alphabetically)" | `SCAN-020`,`SCAN-021`,`SCAN-022`,`SCAN-023`,`SCAN-024`, `VER-003` |
| 10 | "computes md5 for each file in the group" | `HASH-001`, `HASH-009` |
| 11 | "sends the group id, the file ids+hashes to the other side" | `PROTO-003`, `SCAN-024` |
| 12 | "the other side receives the group, tracks files on disk vs md5" | `HASH-003`,`HASH-004` |
| 13 | "decides which should be actually transferred" | `HASH-003`,`HASH-004`, `PROTO-003` |
| 14 | "requests the files from the sender, one after the other (via a queue system)" | `PROTO-004`, `PAR-003` |
| 15 | "until all groups have been requested/skipped" | `PROTO-005`, `PAR-005` |
| 16 | "receiving side should be able to parallelize requests" | `PAR-001`, `NET-004` |
| 17 | "maximum bandwidth (lan)" | `PAR-001`, `NET-006` |
| 18 | "maximum throughput (sending disk / receiving disk)" | `PAR-002`,`PAR-006`,`PAR-007` |
| 19 | "all aspects of the design are bulletproof" | `SEC-*`, `ERR-*`, `FS-040`..`FS-045` |
| 20 | "traceable and verifiable" | `VER-001`..`VER-007`, ARCH §18 |
| 21 | "there are no gaps" | §12 (this table), ARCH §18.2 coverage check |
| 22 | "all error handling has been addressed" | `ERR-001`..`ERR-031`, ARCH §14 |
| 23 | "logging and tracing system built in" | `OBS-001`, `OBS-010` |
| 24 | "by default showing meaningful error/warn/info" | `OBS-002`,`OBS-004` |
| 25 | "progressively more logging (debug/trace)" | `OBS-003`,`OBS-017` |
| 26 | "special macos files (attribute files, .DSync etc) are not to be copied" | `SCAN-026`,`SCAN-027` |
| 27 | "OP-15 extend this to linux/windows" | `SCAN-026` (set broadened to all three platforms) |

**Coverage: 27 / 27 statements mapped. No statement is unassigned.**

> Statement 26 was added after the initial draft. `.DSync` is read as **`.DS_Store`** (Finder folder
> metadata) and "attribute files" as the **AppleDouble `._*` sidecars** that carry resource forks and
> extended attributes onto non-native filesystems. The full set is enumerated in ESYNC-ARCH §10.1.1;
> the two judgment calls in it are flagged there and raised as `OP-14`/`OP-15` below.

---

## 13. Decisions

These are the places where the analysis either filled a gap (**A**) or pushed back on a stated
requirement (**C**). Each was raised as an open point with a recommended default.

> **All defaults below were reviewed and confirmed by the user on 2026-09-04.** They are now
> baselined decisions, not open questions. Changing any of them is a change to this specification
> and requires a version bump and a review of the affected requirements. `OP-15` was answered by
> extending the exclusion set rather than by accepting its original default.

| # | Ref | Question | **Decision** |
|---|---|---|---|
| `OP-01` | `REQ-SEC-002` | Raw AES-CBC is unauthenticated: an attacker can flip bits and mount padding-oracle attacks. Add HMAC-SHA-256 over the ciphertext (encrypt-then-MAC)? | **Yes — add it.** This keeps AES-CBC as specified and closes the hole. Default: implemented. |
| `OP-02` | `REQ-HASH-002` | MD5 is collision-broken. Keep MD5 as default with a negotiated algorithm field allowing BLAKE3? | **Yes.** MD5 default (as stated), field present, `--hash blake3` available. Default: implemented. |
| `OP-03` | `REQ-SEC-009` | Add ephemeral key agreement so a later-leaked code cannot decrypt a captured session? | **Yes.** Cost is one round trip. Default: implemented. |
| `OP-04` | `REQ-CLI-004` | Where does the receiver write? | Default: `./<source-basename>/`, overridable with `--dest`. |
| `OP-05` | `REQ-PAIR-008` | How long is an unclaimed code valid? | Default: 10 minutes, `--pair-timeout`. |
| `OP-06` | `REQ-PAIR-007` | Single-use code, or may several receivers pull from one sender? | Default: **single-use** (`NG-07`). |
| `OP-07` | `REQ-FS-060` | Additive only, or should the destination mirror the source (deleting extras)? | Default: **additive, never deletes**. Mirroring deferred. |
| `OP-08` | `REQ-XFER-040` | Should an interrupted session be resumable, at the cost of a small journal in the destination? | Default: **yes**, journal under `<dest>/.esync/`, removed on success. |
| `OP-09` | `REQ-HASH-007` | Offer a `--quick` (size+mtime) mode that skips hashing? | Default: **offered, off**. |
| `OP-10` | `REQ-CLI-011` | Include/exclude filters in v1? | Default: **yes**, P2 — implement if it does not delay v1. |
| `OP-11` | `REQ-FS-010` | Preserve ownership? | Default: **no** (needs privilege, rarely maps across hosts); `--owner` opt-in. |
| `OP-12` | `REQ-NFR-020` | Windows in v1? | Default: **no**; path layer kept replaceable (`REQ-FS-055`). |
| `OP-13` | `REQ-PAR-004` | Fixed channel count or adaptive? | Default: **adaptive** between 4 and 32, `--channels` pins it. |
| `OP-14` | `REQ-SCAN-026` | The `._*` rule excludes AppleDouble sidecars by prefix, but a legitimate user file could be named `._notes`. Exclude by prefix anyway? | **Yes.** The prefix is reserved by macOS convention and the collision is rare; `--keep-system-files` is the escape hatch. Excluded entries are listed at `DEBUG` and counted at `INFO`, so the loss is never silent. Default: implemented. |
| `OP-15` | `REQ-SCAN-026` | Extend the same treatment to Windows (`Thumbs.db`, `desktop.ini`, `$RECYCLE.BIN`) and Linux (`.Trash-*`) junk? | **Yes — extended.** The set now covers all three platforms as one union, tagged `sys-v1`, applied regardless of sending OS. ESYNC-ARCH §10.1.1. |

---

## 14. Revision History

| Version | Date | Change |
|---|---|---|
| 0.1.0 | 2026-09-04 | Initial extraction from the originating request. Unapproved. |
| 0.1.1 | 2026-09-04 | Added `REQ-SCAN-026`/`REQ-SCAN-027` (macOS system-file exclusion) from originating statement 26; added `OP-14`, `OP-15`. Requirement count 152 → 154. |
| 0.2.0 | 2026-09-04 | `OP-15` decided: exclusion set extended to Windows and Linux/Unix, retagged `sys-v1`. All 15 open-point defaults reviewed and confirmed; §13 converted from open questions to baselined decisions. Status → Approved. |
