package wire

// The §9.3 bodies, one struct per message, fields in wire order. Slice-backed
// count fields (entry_count, needed_count, rejected_count) are derived from the
// slice length on encode and not stored separately.

// SessionParams is 0x01 SESSION_PARAMS (S→R).
type SessionParams struct {
	RootName        string
	SourceKind      uint8
	HashAlg         uint8
	GroupSize       uint32
	Flags           uint32
	SourcePlatform  uint8
	PathNorm        uint8
	SenderVersion   string
	MaxChannels     uint32
	FilterSignature string
	SysexcludeTag   string
}

func (*SessionParams) Type() MsgType { return MsgSessionParams }

// SessionReady is 0x02 SESSION_READY (R→S).
type SessionReady struct {
	Channels        uint16
	GroupCredit     uint16
	PipelineDepth   uint8
	MaxChunk        uint32
	DestPlatform    uint8
	ReceiverVersion string
	DestFreeBytes   uint64
}

func (*SessionReady) Type() MsgType { return MsgSessionReady }

// ScanProgress is 0x10 SCAN_PROGRESS (S→R).
type ScanProgress struct {
	EntriesSeen uint64
	BytesSeen   uint64
	ErrorsSeen  uint32
}

func (*ScanProgress) Type() MsgType { return MsgScanProgress }

// ScanComplete is 0x11 SCAN_COMPLETE (S→R).
type ScanComplete struct {
	TotalFiles     uint64
	TotalBytes     uint64
	TotalGroups    uint32
	SkippedEntries uint32
	ManifestDigest []byte
}

func (*ScanComplete) Type() MsgType { return MsgScanComplete }

// ManifestEntry is one entry inside a GROUP_MANIFEST (§9.3 0x12).
type ManifestEntry struct {
	EntryType   uint8
	Flags       uint16
	Path        []byte
	Size        uint64
	Mode        uint32
	MtimeSec    int64
	MtimeNsec   uint32
	Digest      []byte // length is digest_len on the wire; 0 for dir/symlink/quick
	LinkTarget  []byte // present iff EntryType == 2
	HardlinkKey uint64 // present iff Flags bit0
}

// ManifestFlagHasHardlinkKey is Flags bit0.
const ManifestFlagHasHardlinkKey = 1 << 0

// GroupManifest is 0x12 GROUP_MANIFEST (S→R).
type GroupManifest struct {
	GroupID     uint32
	FirstFileID uint64
	Entries     []ManifestEntry
}

func (*GroupManifest) Type() MsgType { return MsgGroupManifest }

// RejectedEntry is one { index, error_code } pair in a GROUP_DECISION.
type RejectedEntry struct {
	Index     uint16
	ErrorCode uint16
}

// GroupDecision is 0x20 GROUP_DECISION (R→S).
type GroupDecision struct {
	GroupID      uint32
	Needed       []uint16 // ascending indices within the group
	SkippedCount uint16
	Rejected     []RejectedEntry
}

func (*GroupDecision) Type() MsgType { return MsgGroupDecision }

// Credit is 0x21 CREDIT (R→S).
type Credit struct {
	AdditionalGroups uint16
}

func (*Credit) Type() MsgType { return MsgCredit }

// FileRequest is 0x30 FILE_REQUEST (R→S).
type FileRequest struct {
	RequestID uint64
	FileID    uint64
	Offset    uint64
	MaxChunk  uint32
}

func (*FileRequest) Type() MsgType { return MsgFileRequest }

// FileHeader is 0x31 FILE_HEADER (S→R).
type FileHeader struct {
	RequestID uint64
	FileID    uint64
	Size      uint64
	Mode      uint32
	MtimeSec  int64
	MtimeNsec uint32
	Digest    []byte
	ChunkSize uint32
}

func (*FileHeader) Type() MsgType { return MsgFileHeader }

// FileChunk is 0x32 FILE_CHUNK (S→R).
type FileChunk struct {
	RequestID uint64
	Offset    uint64
	Data      []byte
}

func (*FileChunk) Type() MsgType { return MsgFileChunk }

// FileComplete is 0x33 FILE_COMPLETE (S→R).
type FileComplete struct {
	RequestID  uint64
	BytesSent  uint64
	DigestFull []byte
}

func (*FileComplete) Type() MsgType { return MsgFileComplete }

// FileError is 0x34 FILE_ERROR (S→R).
type FileError struct {
	RequestID uint64
	Code      uint16
	Retryable uint8
	Message   string
}

func (*FileError) Type() MsgType { return MsgFileError }

// FileCancel is 0x35 FILE_CANCEL (R→S).
type FileCancel struct {
	RequestID uint64
	Reason    uint16
}

func (*FileCancel) Type() MsgType { return MsgFileCancel }

// SessionSummary is 0x40 SESSION_SUMMARY (S→R).
type SessionSummary struct {
	FilesTotal       uint64
	FilesTransferred uint64
	FilesSkipped     uint64
	FilesFailed      uint64
	BytesTransferred uint64
	GroupsTotal      uint32
	CompletionDigest []byte
}

func (*SessionSummary) Type() MsgType { return MsgSessionSummary }

// SessionSummaryAck is 0x41 SESSION_SUMMARY_ACK (R→S); same body as SESSION_SUMMARY.
type SessionSummaryAck struct {
	FilesTotal       uint64
	FilesTransferred uint64
	FilesSkipped     uint64
	FilesFailed      uint64
	BytesTransferred uint64
	GroupsTotal      uint32
	CompletionDigest []byte
}

func (*SessionSummaryAck) Type() MsgType { return MsgSessionSummaryAck }

// Error is 0x50 ERROR (both directions).
type Error struct {
	Code    uint16
	Fatal   uint8
	Message string
	Detail  string // opt: absent when body_len ends before it (REQ-PROTO-007)
}

func (*Error) Type() MsgType { return MsgError }

// Ping is 0x60 PING (both directions).
type Ping struct {
	Token uint64
}

func (*Ping) Type() MsgType { return MsgPing }

// Pong is 0x61 PONG (both directions).
type Pong struct {
	Token uint64
}

func (*Pong) Type() MsgType { return MsgPong }

// SessionResume is 0x70 SESSION_RESUME (R→S).
type SessionResume struct {
	SessionID   uint64
	Nonce       []byte // 16 bytes
	Tag         []byte // HMAC(K_chan, "resume" || session_id || nonce)
	LastSeqSeen uint64
}

func (*SessionResume) Type() MsgType { return MsgSessionResume }
