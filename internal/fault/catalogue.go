package fault

// This file is the error policy (ARCHITECTURE §14.1, §14.2). Every code in the
// §14.2 catalogue has exactly one row here: its Class, its Retryable flag, and a
// short human "condition / operator action" pair. Classification is a property of
// the code and is decided here, once, never at a throw site (REQ-ERR-002).

// Codes from the §14.2 catalogue.
const (
	// E1xxx — command line and environment.
	E1001 Code = "E1001"
	E1002 Code = "E1002"
	E1003 Code = "E1003"
	E1004 Code = "E1004"
	E1005 Code = "E1005"
	E1006 Code = "E1006"
	E1007 Code = "E1007"
	E1008 Code = "E1008"

	// E2xxx — pairing code and session setup.
	E2001 Code = "E2001"
	E2002 Code = "E2002"
	E2003 Code = "E2003"
	E2004 Code = "E2004"
	E2005 Code = "E2005"
	E2006 Code = "E2006"
	E2007 Code = "E2007"

	// E3xxx — transport and channels.
	E3001 Code = "E3001"
	E3002 Code = "E3002"
	E3003 Code = "E3003"
	E3004 Code = "E3004"
	E3005 Code = "E3005"
	E3006 Code = "E3006"
	E3007 Code = "E3007"

	// E4xxx — cryptography and authentication.
	E4001 Code = "E4001"
	E4002 Code = "E4002"
	E4003 Code = "E4003"
	E4004 Code = "E4004"
	E4005 Code = "E4005"
	E4006 Code = "E4006"
	E4007 Code = "E4007"

	// E5xxx — protocol.
	E5001 Code = "E5001"
	E5002 Code = "E5002"
	E5003 Code = "E5003"
	E5004 Code = "E5004"
	E5005 Code = "E5005"
	E5006 Code = "E5006"
	E5007 Code = "E5007"
	E5008 Code = "E5008"
	E5009 Code = "E5009"
	E5010 Code = "E5010"
	E5011 Code = "E5011"

	// E6xxx — source side.
	E6001 Code = "E6001"
	E6002 Code = "E6002"
	E6003 Code = "E6003"
	E6004 Code = "E6004"
	E6005 Code = "E6005"
	E6006 Code = "E6006"
	E6007 Code = "E6007"
	E6008 Code = "E6008"

	// E7xxx — destination side.
	E7001 Code = "E7001"
	E7002 Code = "E7002"
	E7003 Code = "E7003"
	E7004 Code = "E7004"
	E7005 Code = "E7005"
	E7006 Code = "E7006"
	E7007 Code = "E7007"
	E7008 Code = "E7008"
	E7009 Code = "E7009"
	E7010 Code = "E7010"
	E7011 Code = "E7011"
	E7012 Code = "E7012"
	E7013 Code = "E7013"
	E7014 Code = "E7014"

	// E8xxx — integrity and journalling.
	E8001 Code = "E8001"
	E8002 Code = "E8002"
	E8003 Code = "E8003"
	E8004 Code = "E8004"
	E8005 Code = "E8005"

	// E9xxx — internal defects.
	E9001 Code = "E9001"
	E9002 Code = "E9002"
	E9003 Code = "E9003"
	E9004 Code = "E9004"

	// Signal is the synthetic code for "interrupted by a signal" (§14.6 exit 5).
	Signal Code = "ESIGNAL"
)

// policy is one row of the §14.2 catalogue.
type policy struct {
	class     Class
	retryable bool
	condition string
	action    string
}

// catalogue maps every Code to its policy. This map IS the classification policy
// (§14.1): nothing else in the codebase decides a Class.
var catalogue = map[Code]policy{
	E1001: {Fatal, false, "unknown flag or malformed argument", "fix the command line"},
	E1002: {Fatal, false, "both a path and --link given, or neither", "choose one mode"},
	E1003: {Fatal, false, "source path does not exist", "check the path"},
	E1004: {Fatal, false, "source path not readable", "check permissions"},
	E1005: {Fatal, false, "no usable network interface for the code", "connect to a network, or use --bind"},
	E1006: {Fatal, false, "destination not writable", "choose another --dest"},
	E1007: {Fatal, false, "flag value outside its documented range", "see --help for the range"},
	E1008: {Fatal, false, "destination is inside the source tree on the same host", "choose a destination outside the source"},

	E2001: {Fatal, false, "pairing code fails length/alphabet/CRC validation", "re-copy the code; it was mistyped or truncated"},
	E2002: {Fatal, false, "code carries an unknown code-format version", "upgrade the older side"},
	E2003: {Fatal, false, "protocol version unsupported by this build", "use matching versions on both machines"},
	E2004: {Fatal, false, "every endpoint in the code is syntactically unusable", "regenerate the code"},
	E2005: {Fatal, false, "code already claimed by another receiver", "generate a new code on the sender"},
	E2006: {Fatal, false, "no receiver linked before --pair-timeout", "re-run the sender"},
	E2007: {Fatal, false, "handshake attempt budget exhausted", "someone is guessing, or the code is wrong; re-run the sender"},

	E3001: {Fatal, false, "no candidate endpoint reachable", "check LAN connectivity and firewall; see the per-endpoint table"},
	E3002: {Fatal, false, "connect timeout on all candidates", "check LAN connectivity and firewall"},
	E3003: {Item, true, "connection reset mid-session", "automatic: requeue and rejoin; fatal if the control channel and resume fails"},
	E3004: {Fatal, false, "peer unresponsive past the keepalive deadline", "check the other machine; resume with the same command"},
	E3005: {Fatal, false, "all data channels lost and unrecoverable", "re-run; the receiver resumes from its journal"},
	E3006: {Item, true, "CHANNEL_JOIN rejected", "automatic retry; channel retired on exhaustion"},
	E3007: {Fatal, false, "control channel resume window expired", "re-run; the receiver resumes from its journal"},

	E4001: {Fatal, false, "handshake confirmation MAC failed", "the code is wrong or a MITM is present"},
	E4002: {Fatal, false, "data-channel authentication failed", "the code is wrong or a MITM is present"},
	E4003: {Fatal, false, "record MAC verification failed", "tampering or corruption; session is aborted"},
	E4004: {Fatal, false, "record sequence violation", "tampering, replay, or a bug; session is aborted"},
	E4005: {Fatal, false, "stream ended without an authenticated close", "treat the transfer as incomplete; re-run to resume"},
	E4006: {Fatal, false, "sequence space exhausted", "unreachable in practice; re-run"},
	E4007: {Fatal, false, "system CSPRNG failure", "environment fault; do not retry"},

	E5001: {Fatal, false, "message not valid in the current state", "version mismatch or a peer bug; report with both versions"},
	E5002: {Fatal, false, "unsupported record/protocol version", "match versions"},
	E5003: {Fatal, false, "record length outside the declared bounds", "corrupt or hostile peer"},
	E5004: {Fatal, false, "malformed padding after a valid MAC", "internal bug; reachable only from a defect"},
	E5005: {Fatal, false, "session summaries disagree", "do not trust the destination; investigate with both logs"},
	E5006: {Fatal, false, "FILE_CHUNK offsets not contiguous", "peer bug"},
	E5007: {Fatal, false, "request for an unknown file_id", "peer bug or desynchronised plan"},
	E5008: {Fatal, false, "sender exceeded the granted credit", "peer bug"},
	E5009: {Fatal, false, "duplicate GROUP_DECISION for one group", "peer bug"},
	E5010: {Fatal, false, "frame body_len disagrees with the record plaintext", "corrupt peer"},
	E5011: {Fatal, false, "unknown message type", "version mismatch"},

	E6001: {Item, false, "permission denied reading a source file", "fix source permissions and re-run"},
	E6002: {Item, false, "source file vanished between manifest and read", "expected on a live tree; re-run to pick it up"},
	E6003: {Item, false, "source file changed during transfer (size or digest drift)", "re-run; the file will be re-planned with its new content"},
	E6004: {Item, true, "source read I/O error", "retried; persistent failure suggests failing media"},
	E6005: {Warn, false, "source directory unreadable during enumeration", "fix permissions; contents were not planned"},
	E6006: {Warn, false, "unsupported entry type (socket, FIFO, device)", "expected; these are never transferred"},
	E6007: {Warn, false, "symlink loop detected under --follow-symlinks", "informational"},
	E6008: {Item, true, "too many open files on the sender", "lower --read-concurrency or raise ulimit -n"},

	E7001: {Item, true, "destination file create failed", "check permissions and inode availability"},
	E7002: {Item, false, "destination permission denied", "fix destination permissions"},
	E7003: {Fatal, false, "destination out of space", "free space and re-run; the journal makes it resumable"},
	E7004: {Item, true, "destination write I/O error", "retried; persistent failure suggests failing media"},
	E7005: {Item, true, "atomic publish (rename) failed", "retried; the partial file remains under .esync/parts"},
	E7006: {Warn, false, "could not apply mode or mtime", "content is correct; metadata is not"},
	E7007: {Fatal, false, "journal write failed", "resume safety cannot be guaranteed; stop rather than continue blind"},
	E7008: {Item, true, "directory create failed", "check permissions"},
	E7009: {Warn, false, "hard link creation failed", "content is materialised separately; storage is larger"},
	E7010: {Item, false, "unsafe path rejected (traversal, absolute, NUL, reserved)", "reported in the summary; investigate the source tree"},
	E7011: {Item, false, "symlink target escapes the destination root", "use --allow-unsafe-links only if intended"},
	E7012: {Item, false, "destination name collision after platform normalisation", "two source names map to one destination name; rename at the source"},
	E7013: {Item, false, "symlink encountered on the destination path", "remove the symlink or choose another destination"},
	E7014: {Item, false, "path too long for the destination platform", "shorten the destination root"},

	E8001: {Item, true, "received content digest does not match the manifest digest", "retried; exhaustion means the file could not be moved intact"},
	E8002: {Item, true, "byte count does not match the declared size", "retried; exhaustion means the file could not be moved intact"},
	E8003: {Item, false, "sender's streamed digest does not match the manifest digest", "the source changed; equivalent to E6003"},
	E8004: {Warn, false, "journal record corrupt (torn tail)", "automatic: the record is discarded, its file is re-fetched"},
	E8005: {Warn, false, "digest cache corrupt or version-mismatched", "automatic: full hashing is used"},

	E9001: {Fatal, false, "panic recovered at a worker boundary", "a defect; the stack trace and the trace-ring dump are in the log"},
	E9002: {Fatal, false, "drain watchdog expired", "an accounting defect; report with the log"},
	E9003: {Fatal, false, "runtime invariant violated (§9.4)", "a defect; report with the log"},
	E9004: {Fatal, false, "a bound was exceeded that should have been unreachable", "a defect; report with the log"},

	Signal: {Fatal, false, "interrupted by a signal", "re-run to resume; the journal is durable"},
}

// lookup returns the policy for a code, falling back to a Fatal policy for an
// unknown code so an un-catalogued code can never be silently treated as benign.
func lookup(code Code) policy {
	if p, ok := catalogue[code]; ok {
		return p
	}
	return policy{class: Fatal, retryable: false, condition: "uncatalogued error code", action: "report this as a bug"}
}

// Condition returns the short human description of what the code means.
func (c Code) Condition() string { return lookup(c).condition }

// Action returns the short operator action for the code.
func (c Code) Action() string { return lookup(c).action }
