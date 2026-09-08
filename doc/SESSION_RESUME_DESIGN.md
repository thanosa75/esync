# SESSION_RESUME — suspend / re-dial design (gap 1, R-26 / R-07a)

Status: **design — awaiting sign-off on D1–D4 before implementation.** Companion to
`STATUS_SUMMARY.md` R-26. The doc (ARCHITECTURE §7.3, §4.1, §4.2) describes control-loss
suspend+resume as working; today `SESSION_RESUME` (0x70) is codec-only, `--resume-window` is
parsed-only, E3007 is never raised, and any control-channel loss is fatal on both ends. This
document fixes the doc's internal contradictions and pins one concrete wire/state contract so the
implementation is a mechanical follow-through rather than a guess.

## 1. Goal and non-goals

- Goal: when the **control channel** dies mid-session (both data channels survive), both ends enter
  `Suspended`; the receiver re-dials the sender within `--resume-window` (default 60 s) and the
  session continues from where it stopped — no journal re-run, no new pairing code. On window expiry:
  receiver and sender both exit `2`, retaining the journal, with `E3007`.
- Non-goal: data-channel-loss recovery (that is shipped — see commit a6d5c6f, §7.3 row 1). Non-goal:
  resume across a receiver *process* restart (that stays journal-based). Non-goal: forward secrecy
  for the resumed portion — the doc already records this as RISK-04, accepted.

## 2. Doc contradictions to resolve (all in ARCHITECTURE today)

| # | Where | Says | Conflict |
|---|---|---|---|
| C1 | §4.1 sender diagram, §7.3 table | resume-window expiry is `E3004` | §14.2 catalogue + F-NET-02 (§19) say `E3007` "control channel resume window expired"; STATUS R-26 says E3007 |
| C2 | §7.3 | receiver "performs a fresh handshake" on re-dial | pairing code is single-use; RISK-04 says existing `K_chan` is reused (no fresh X25519 possible without the secret) |
| C3 | §9.3 | `SESSION_RESUME` body "see §7.3" | §7.3 never defines the message body fields or how the re-dialed TCP conn becomes an encrypted control channel 0 before 0x70 can be sent inside it |
| C4 | wire codec | `SessionResume{SessionID, Nonce, Tag, LastSeqSeen}` | `LastSeqSeen` has no specified meaning |

Proposed resolutions (see D1–D4): C1 → E3007 on expiry, E3004 becomes dormant (it keeps its
catalogue row and Fatal class but loses its raise site — precedent: E2005, R-07b). C2/C3 → no pair
handshake on re-dial; the new TCP conn is wrapped directly as control channel 0 with the **existing**
ch-0 record keys and 0x70 is its first encrypted frame. C4 → `LastSeqSeen` = the last control
record the receiver **decrypted** before loss; the sender uses it to replay its buffered-but-unread
S2R control stream (see §5). Text edits to ARCHITECTURE are part of the implementation commit, not
this one.

## 3. Threat model for the resumed connection (why the shape is sound)

The re-dialed TCP conn is wrapped with the *existing* `K_enc[dir][0]`/`K_mac[dir][0]`; there is no
fresh X25519, so no forward secrecy (RISK-04: window ≤ 60 s, keys already resident both ends,
accepted). An impostor who connects first to the sender's listener cannot do anything: it cannot
produce a validly MAC'd `SESSION_RESUME` (no `K_chan`), and the sender reads one frame, fails the
MAC (E4003), closes. The sender's own ch-0 record stream restarts at seq 0 on the new conn, so
records captured from the dead conn replay if replayed in order within the window — the exact
bounded exposure RISK-04 accepts; no additional exposure beyond it. Data channels are untouched by
resume (their keys/seq never reset).

## 4. State machines

Sender `Serving --(control read error | keepalive E3004)--> Suspended --(window expires)--> Failed E3007 exit 2`; `Suspended --(resume conn accepted+verified)--> Serving` (unchanged diagram states, C1-fixed code).

Receiver `Receiving --(control read error | keepalive E3004)--> Suspended`; `Suspended --(re-dial + SESSION_RESUME accepted)--> Receiving`; `Suspended --(window expires)--> Failed E3007 exit 2`, journal retained (existing `finish()` non-clean path already does this).

Both ends already run their control reader in a goroutine; the change is what that goroutine does on
error (suspend instead of `s.fail`/`setFatal`) and what runs during Suspended.

## 5. Resume protocol (one concrete contract)

1. **Loss detection.** Control `RecvMsg` errors (io.EOF, reset) or the keepalive watchdog trips.
   Receiver: `session.suspendControl(cause)` — record `lastCtrlSeqSeen` (its ch-0 *reader*'s next
   expected seq, exposed via the record layer), stop pumping the old control conn, leave all data
   channels and the decide/fetch pipeline running for whatever still works, and start the re-dial
   loop. Sender: same, minus the re-dial (it waits on its listener).
2. **Re-dial (receiver).** Loop until `--resume-window`: dial the code's candidate endpoints
   (still in memory, same ports — the sender's listener stays bound), schedule 1 s, 2 s, 4 s …
   capped at 10 s (D4). On connect: wrap as `channel.Control(conn, sess, Receiver)` and send
   `SESSION_RESUME{SessionID, Nonce, Tag=HMAC(K_chan,"resume"||sid||nonce), LastSeqSeen}` as the
   first frame. Wait (with the keepalive-equivalent read bound) for the sender to replay the S2R
   control stream from `LastSeqSeen` (§6). A MAC failure / close → next attempt.
3. **Accept (sender).** While Suspended the sender's `join.accept` loop must still run; a resume
   conn is *not* a `CHANNEL_JOIN`, so the accept loop peeks the first 14 record-header bytes: a
   resume conn is wrapped in the record layer immediately (version/channel 0 header), a join conn
   sends the raw 63-byte join blob. Peek buffer distinguishes them (one `bufio`-style read that
   `record.Reader` must be able to continue from — a `*bufio.Reader` wrapper around the socket
   before both layers, or a `peekedConn`).
4. **Verification.** Sender reads one frame; it must decode to `SESSION_RESUME` with a valid tag for
   this session and a sane `LastSeqSeen`; anything else → E4002, close, keep waiting. On success the
   conn becomes the new control channel 0; the old one is discarded. E3007 arming starts at loss,
   stops on success.
5. **Reconciliation.** See §6. Both ends then resume their normal control-plane roles. Nothing about
   the *data* plane changes: fetchers on surviving data channels were never stopped.

## 6. Control-stream reconciliation (the real design crux)

State each side holds in memory across a suspend:

- Sender: decided groups (`tracker.decidedSet`), manifests published (0..s), credit outstanding,
  pending requests. Receiver: manifests received (0..k), groups decided (0..j ≤ k), need queue,
  journal.

Because GROUP_DECISIONs flow R→S on control, the sender learns decisions only over control. Options:

- **O1 (recommended): sender replay buffer + `LastSeqSeen`.** The sender's ch-0 writer records every
  S2R control record (header+seq) in a bounded ring (configurable, default the whole control
  stream — manifests dominate; a 100 k-file tree is ~100 × ≤256 KiB records ≈ tens of MiB worst
  case; acceptable for a one-shot tool, and trimmable by receiver-acked watermark from
  GROUP_DECISION/CREDIT receipt). On resume the sender replays records with seq > `LastSeqSeen` on
  the new control conn, then continues live. The receiver sees a gapless S2R stream: it never needs
  to know the transfer "resumed". Idempotence falls out of the existing duplicate-decision/
  E5009 handling.
- **O2: receiver-driven re-request.** No replay; after resume the receiver asks for manifests ≥ its
  first undecided group. Requires a new R→S message → wire/version change; rejected (the whole
  point of 0x70 was to avoid one).
- **O3: journal-diff.** Teardown + journal-based resume. That is today's *fatal* path; using it for
  suspend loses the in-memory state for zero wire change — rejected (this is what the gap calls
  "control loss is always fatal").

O1 keeps the wire unchanged (0x70 only), is receiver-transparent, and bounds work to the sender. The
record writer must expose per-record seq + a hook to tee plaintext+seq into the ring; the replay
writer must start its seq counter at `LastSeqSeen+1` on the new conn (fresh record `Writer`
constructed with that initial seq), so receiver MACs verify.

## 7. Code change list (implementation order)

1. `internal/crypto/record` — `Writer` init seq param; expose last-written seq; a `RecordSink`
   hook (or build the ring in `sender` by wrapping `Writer`). Add the reader's next-expected-seq
   accessor on the receiver side.
2. `internal/sender/run.go` — control-loss → `Suspended` (control.reader must stop fatalling on
   transport loss; only coded faults remain fatal); `--resume-window` plumbed into `sender.Config`
   (new flag on the sender FlagSet, default 60 s, matching §16.1); suspended-state accept loop with
   peek discrimination; replay ring + replay-on-resume; expiry → `E3007` exit 2 (journal is
   receiver-side; sender just exits).
3. `internal/receiver/run.go` — control-loss → `Suspended`; re-dial loop; SESSION_RESUME send +
   wait for replay; expiry → `E3007` exit 2 retaining journal (reuse existing non-clean `finish()`);
   `--resume-window` consumed (currently parsed-only).
4. `internal/receiver/decide.go` / decide pool — GROUP_DECISION/CREDIT sends must tolerate a
   suspended control and retry after resume (route sends through a small control-session wrapper
   that blocks while suspended).
5. Wire goldens: add an S→R replay + 0x70 frame vector (extend `wire` golden tests).
6. Tests (F-NET-02): in-process sender + receiver over loopback; kill only the control TCP mid-
   transfer (both data channels survive); assert: session suspends, re-dials within the window,
   remaining groups transfer, exit 0, destination byte-exact. Negative: sender stops answering the
   re-dial → receiver exits 2 with E3007 and a retained journal + resume command. Keepalive-shortened
   variant reuses the `channel` test seams.
7. Doc edits (same commit): §4.1/§4.2 diagrams unchanged (states exist); §7.3 table "On expiry:
   E3004" → E3007; §14.2 note that E3004 is the suspend *trigger* and is dormant while resume is
   armed; §9.3 0x70 body paragraph (fields + LastSeqSeen meaning); RISK-04 row already accurate.

## 8. Decisions requested (each with a recommended default)

- **D1 — expiry code:** E3007 (catalogue + F-NET-02) and make E3004 dormant. *Recommended.*
- **D2 — resumed conn crypto:** reuse existing ch-0 record keys, no fresh handshake, seq restarts,
  RISK-04 accepted. *Recommended* (the only option consistent with a single-use pairing code).
- **D3 — reconciliation:** O1 sender replay ring keyed by `LastSeqSeen`. *Recommended.* Rejected
  alternatives: O2 (new wire message), O3 (journal-diff teardown = today's fatal path).
- **D4 — re-dial schedule:** 1 s, 2 s, 4 s, … cap 10 s for `--resume-window` (default 60 s).
  *Recommended;* 0.5/2/8 in §7.3 refers to *data-channel* rejoin, not control re-dial.

With D1–D4 confirmed this document is the spec; implementation is items 1–7 above.
