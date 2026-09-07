// Package receiver is the esync receiving half (ARCHITECTURE §3.2, §4.2): it
// drives the transfer, decides per-file what is needed against the destination
// tree, pulls content over the data channels, verifies and atomically publishes
// every file, and keeps a resume journal.
package receiver

import (
	"runtime"
	"time"

	"esync/internal/digest"
)

// safeMaxChunk is the largest chunk payload the receiver advertises in
// SESSION_READY. A FILE_CHUNK frame plaintext is 25 + len(data) bytes and is
// pkcs7-padded to a multiple of 16 before the record MAC; the padded ciphertext
// must stay within record.MaxCTData (1<<20 + 16). 1<<20 - 8192 leaves ample
// head-room for the frame header, the pad block, and the tag.
const safeMaxChunk = 1<<20 - 8192

// needQueueDepth is the hard ceiling on the ordered need queue (ARCHITECTURE
// §13.3: 4 x 1024 entries).
const needQueueDepth = 4 * 1024

// spaceMarginBytes is the free-space safety margin held back by the guard
// (ARCHITECTURE §12.7).
const spaceMarginBytes int64 = 64 << 20

// Config is the fully resolved receiver configuration. Run does not read flags
// or the environment; the caller (main) fills every field.
type Config struct {
	Link    string // --link <code>, the pairing code (required)
	Dest    string // --dest <dir>; "" means ./<source-basename>
	Version string // receiver_version string for SESSION_READY

	DryRun bool // --dry-run: pair, manifest, decide, report; write nothing

	Channels       int  // --channels K: initial count; also the pinned count when ChannelsPinned
	ChannelsPinned bool // true when --channels (or ESYNC_CHANNELS) was set explicitly: disables the adaptive tuner (§13.4)
	MinChannels    int  // --min-channels: adaptive floor
	MaxChannels    int  // --max-channels: adaptive ceiling
	PipelineDepth  int  // --pipeline-depth (advertised; effective depth is 1 per channel here)
	GroupCredit    int  // --group-credit
	DecideWorkers  int  // --decide-workers

	NoFsync            bool          // --no-fsync
	CheckpointInterval int64         // --checkpoint-interval (parsed; part-checkpoint resume is stubbed)
	ResumeWindow       time.Duration // --resume-window (parsed only)
	DrainTimeout       time.Duration // --drain-timeout (E9002)
	StallTimeout       time.Duration // --stall-timeout: max silence on a data channel before E3005
	ProgressInterval   time.Duration // --progress-interval: stderr progress cadence
	AllowUnsafeLinks   bool          // --allow-unsafe-links
	Owner              bool          // --owner

	Quick   bool // --quick (receiver-side decision shortcut)
	NoCache bool // --no-cache

	ConnectTimeout   time.Duration // per-candidate TCP connect budget
	HandshakeTimeout time.Duration // per-connection handshake deadline
	Stagger          time.Duration // candidate-racing stagger
	SocketBuffer     int           // data-channel socket buffer
	MaxRetries       int           // --max-retries (§14.3)

	// HashAlg is a fallback only. The authoritative algorithm is whatever the
	// sender declares in SESSION_PARAMS; the receiver adopts it.
	HashAlg digest.Algo
}

func (c Config) withDefaults() Config {
	if c.Channels <= 0 {
		c.Channels = 4
	}
	if c.MinChannels <= 0 {
		c.MinChannels = 4
	}
	if c.MaxChannels <= 0 {
		c.MaxChannels = 32
	}
	if c.PipelineDepth <= 0 {
		c.PipelineDepth = 2
	}
	if c.GroupCredit <= 0 {
		c.GroupCredit = 4
	}
	if c.DecideWorkers <= 0 {
		c.DecideWorkers = min(8, runtime.NumCPU())
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 64 << 20
	}
	if c.ResumeWindow <= 0 {
		c.ResumeWindow = 60 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 60 * time.Second
	}
	if c.StallTimeout <= 0 {
		c.StallTimeout = 60 * time.Second
	}
	if c.ProgressInterval <= 0 {
		c.ProgressInterval = 5 * time.Second
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.Stagger <= 0 {
		c.Stagger = 250 * time.Millisecond
	}
	if c.SocketBuffer <= 0 {
		c.SocketBuffer = 4 << 20
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	return c
}

// Summary is the receiver's own tally of the finished session, returned by Run.
type Summary struct {
	Outcome          string
	FilesTotal       uint64
	FilesTransferred uint64
	FilesSkipped     uint64
	FilesFailed      uint64
	FilesRejected    uint64
	BytesTransferred uint64
	GroupsTotal      uint32
	Elapsed          time.Duration
	// ResumeCommand is set on a non-clean exit: the exact command to resume.
	ResumeCommand string
}

func platformID() uint8 {
	if runtime.GOOS == "darwin" {
		return 1
	}
	return 0
}
