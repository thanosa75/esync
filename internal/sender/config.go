// Package sender is the esync sending half (ARCHITECTURE §3.2, §4.1): it owns
// the source tree, publishes the manifest under receiver credit, and streams
// file content on demand across the data channels.
package sender

import (
	"runtime"
	"time"

	"esync/internal/digest"
	"esync/internal/plan"
)

// Config is the fully resolved sender configuration. Run does not read flags or
// the environment; the caller (main) fills every field.
type Config struct {
	SourcePath string // the source root, as given on the command line
	Bind       string // "" for all interfaces, else a literal IP
	Port       int    // 0 picks a free port
	Version    string // sender_version string for SESSION_PARAMS

	MaxChannels int // sender-side ceiling advertised in SESSION_PARAMS

	PairTimeout      time.Duration // --pair-timeout (E2006)
	MaxPairAttempts  int           // --max-pair-attempts (E2007)
	HandshakeTimeout time.Duration // per-connection handshake deadline
	DrainTimeout     time.Duration // --drain-timeout (E9002)
	ProgressInterval time.Duration // --progress-interval: stderr progress cadence

	GroupBytes int64 // --group-bytes: target group size; <= 0 uses the plan default

	HashAlg         digest.Algo
	HashWorkers     int
	ReadConcurrency int
	ChunkSize       int
	SocketBuffer    int

	FollowSymlinks  bool
	OneFileSystem   bool
	Hardlinks       bool
	KeepSystemFiles bool
	Quick           bool
	NoCache         bool

	Filters []plan.FilterRule
}

func (c Config) withDefaults() Config {
	if c.MaxChannels <= 0 {
		c.MaxChannels = 8
	}
	if c.PairTimeout <= 0 {
		c.PairTimeout = 10 * time.Minute
	}
	if c.MaxPairAttempts <= 0 {
		c.MaxPairAttempts = 5
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 60 * time.Second
	}
	if c.ProgressInterval <= 0 {
		c.ProgressInterval = 5 * time.Second
	}
	if c.GroupBytes <= 0 {
		c.GroupBytes = 512 << 20 // keep in step with plan.defaultGroupBytes
	}
	if c.HashWorkers <= 0 {
		c.HashWorkers = min(8, runtime.NumCPU())
	}
	if c.ReadConcurrency <= 0 {
		c.ReadConcurrency = 4
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = 1 << 20
	}
	if c.SocketBuffer <= 0 {
		c.SocketBuffer = 4 << 20
	}
	return c
}

func (c Config) planOptions() plan.Options {
	return plan.Options{KeepSystemFiles: c.KeepSystemFiles, Filters: c.Filters, GroupBytes: c.GroupBytes}
}

func (c Config) sessionFlags() uint32 {
	var f uint32
	if c.FollowSymlinks {
		f |= 1 << 0
	}
	if c.OneFileSystem {
		f |= 1 << 1
	}
	if c.Quick {
		f |= 1 << 2
	}
	if c.Hardlinks {
		f |= 1 << 4
	}
	if c.KeepSystemFiles {
		f |= 1 << 5
	}
	return f
}

func platformID() uint8 {
	if runtime.GOOS == "darwin" {
		return 1
	}
	return 0
}

// Summary is the sender's own tally of the finished session.
type Summary struct {
	Outcome          string
	FilesTotal       uint64
	FilesTransferred uint64
	FilesSkipped     uint64
	FilesFailed      uint64
	BytesTransferred uint64
	GroupsTotal      uint32
	Elapsed          time.Duration
}
