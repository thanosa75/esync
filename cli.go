package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"esync/internal/digest"
	"esync/internal/fault"
	"esync/internal/obs"
	"esync/internal/plan"
	"esync/internal/receiver"
	"esync/internal/sender"
)

// hasLinkFlag reports whether --link / -link (with or without "=value") appears
// before a "--" terminator. Its presence selects receiver mode.
func hasLinkFlag(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "--link" || a == "-link" ||
			strings.HasPrefix(a, "--link=") || strings.HasPrefix(a, "-link=") {
			return true
		}
	}
	return false
}

// commonFlags holds the flags shared by both modes (ARCHITECTURE §16.1).
type commonFlags struct {
	v, vv, q, qq     *bool
	logLevel         *string
	logFormat        *string
	logFile          *string
	logRing          *int
	logSync          *bool
	redactPaths      *bool
	progressInterval *time.Duration
	hash             *string
	quick            *bool
	noCache          *bool
	chunkSize        *int
	socketBuffer     *int
	maxRetries       *int
	connectTimeout   *time.Duration
	handshakeTimeout *time.Duration
	shutdownGrace    *time.Duration
}

func registerCommon(fs *flag.FlagSet) *commonFlags {
	return &commonFlags{
		v:                fs.Bool("v", false, "raise log level to DEBUG"),
		vv:               fs.Bool("vv", false, "raise log level to TRACE"),
		q:                fs.Bool("q", false, "lower log level to WARN"),
		qq:               fs.Bool("qq", false, "lower log level to ERROR"),
		logLevel:         fs.String("log-level", "", "error|warn|info|debug|trace"),
		logFormat:        fs.String("log-format", "text", "text|json"),
		logFile:          fs.String("log-file", "", "mirror full-fidelity logs to a file"),
		logRing:          fs.Int("log-ring", 2048, "trace ring buffer size"),
		logSync:          fs.Bool("log-sync", false, "synchronous log writes"),
		redactPaths:      fs.Bool("redact-paths", false, "redact paths in logs"),
		progressInterval: fs.Duration("progress-interval", 5*time.Second, "progress cadence"),
		hash:             fs.String("hash", "md5", "md5|sha256 (blake3 reserved)"),
		quick:            fs.Bool("quick", false, "trust size+mtime"),
		noCache:          fs.Bool("no-cache", false, "ignore the digest cache"),
		chunkSize:        fs.Int("chunk-size", 1<<20, "file chunk size in bytes"),
		socketBuffer:     fs.Int("socket-buffer", 4<<20, "data-channel socket buffer in bytes"),
		maxRetries:       fs.Int("max-retries", fault.MaxRetriesDefault, "per-item retry budget"),
		connectTimeout:   fs.Duration("connect-timeout", 10*time.Second, "per-candidate TCP connect budget"),
		handshakeTimeout: fs.Duration("handshake-timeout", 5*time.Second, "per-connection handshake deadline"),
		shutdownGrace:    fs.Duration("shutdown-grace", 5*time.Second, "second-signal grace period"),
	}
}

// resolveLevel applies the -v/-vv/-q/-qq shorthands, with --log-level winning
// outright (ARCHITECTURE §16.1 / REQ-OBS-003).
func (c *commonFlags) resolveLevel() (obs.Level, error) {
	if *c.logLevel != "" {
		return obs.ParseLevel(*c.logLevel)
	}
	switch {
	case *c.vv:
		return obs.LevelTrace, nil
	case *c.v:
		return obs.LevelDebug, nil
	case *c.qq:
		return obs.LevelError, nil
	case *c.q:
		return obs.LevelWarn, nil
	default:
		return obs.LevelInfo, nil
	}
}

func (c *commonFlags) obsConfig(role string, lvl obs.Level) obs.Config {
	return obs.Config{
		Level:            lvl,
		Format:           *c.logFormat,
		File:             *c.logFile,
		RingSize:         *c.logRing,
		RedactPaths:      *c.redactPaths,
		Sync:             *c.logSync,
		ProgressInterval: *c.progressInterval,
		Role:             role,
	}
}

// filterFlag appends an --include / --exclude glob to a shared ordered slice so
// that "last match wins" reflects command-line order (ARCHITECTURE §10.1).
type filterFlag struct {
	list    *[]plan.FilterRule
	include bool
}

func (f filterFlag) String() string { return "" }

func (f filterFlag) Set(s string) error {
	if s == "" {
		return fmt.Errorf("empty glob")
	}
	*f.list = append(*f.list, plan.FilterRule{Include: f.include, Pattern: s})
	return nil
}

// applyEnv fills any flag the user did not set from ESYNC_<NAME>, so precedence
// is flag > env > default (ARCHITECTURE §16). Values go through flag.Set, so the
// same parsing and validation apply.
func applyEnv(fs *flag.FlagSet) error {
	set := map[string]bool{}
	fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })

	var firstErr error
	fs.VisitAll(func(fl *flag.Flag) {
		if firstErr != nil || set[fl.Name] {
			return
		}
		env := "ESYNC_" + strings.ToUpper(strings.ReplaceAll(fl.Name, "-", "_"))
		if val, ok := os.LookupEnv(env); ok {
			if err := fs.Set(fl.Name, val); err != nil {
				firstErr = fmt.Errorf("%s=%q: %w", env, val, err)
			}
		}
	})
	return firstErr
}

// explicitlySet reports whether name was set by the user, either on the command
// line or via its ESYNC_<NAME> environment variable — call after applyEnv, which
// routes env values through fs.Set and so also marks them visited. Used to tell
// "--channels 4" (an explicit pin) apart from the same value arriving as the
// unset default (ARCHITECTURE §13.4).
func explicitlySet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(fl *flag.Flag) {
		if fl.Name == name {
			found = true
		}
	})
	return found
}

// atLeast range-checks a numeric flag; a violation is E1007 with the valid range
// (ARCHITECTURE §16 final paragraph / REQ-ERR-003).
func atLeast(name string, v, min int) error {
	if v < min {
		return fault.Newf(fault.E1007, "parse flags", name, nil,
			"--%s must be >= %d, got %d", name, min, v)
	}
	return nil
}

// usageErr prints a parse/usage error to stderr and returns exit code 3.
func usageErr(err error) int {
	fmt.Fprintf(os.Stderr, "esync: %v\ntry 'esync --help'\n", err)
	return 3
}

func parseHash(s string) (digest.Algo, error) {
	a, err := digest.Parse(s)
	if err != nil {
		return 0, err
	}
	if a == digest.BLAKE3 {
		return 0, fault.Newf(fault.E1007, "parse flags", "hash", nil,
			"--hash blake3 is reserved but not implemented; use md5 or sha256")
	}
	return a, nil
}

// installFatalHandler routes an escalated Fatal fault (a recovered panic in an
// obs.Go goroutine, ARCHITECTURE §14.5) to the log and a matching exit code.
func installFatalHandler(ctx obs.Ctx, flush func()) {
	obs.OnFatal(func(err error) {
		obs.LogFault(ctx, err)
		flush()
		os.Exit(fault.ExitCode(err))
	})
}

// signalEscape enforces ARCHITECTURE §14.7. sender/receiver.Run install their
// own handler that turns the first SIGINT/SIGTERM into a graceful root-context
// cancel (checkpoint the journal, emit the summary, exit 5). This is the backstop
// for when that graceful stop does not complete promptly: a second signal, or
// grace elapsing after the first, forces the process down with code 5. Without it
// a trapped second signal — and SIGTERM, also trapped — is silently swallowed and
// only SIGKILL ends the process.
func signalEscape(ctx obs.Ctx, sigc <-chan os.Signal, grace time.Duration, exit func(int)) {
	obs.Go(ctx, "signal.escape", func() error {
		if _, ok := <-sigc; !ok {
			return nil
		}
		var graceCh <-chan time.Time
		if grace > 0 {
			graceCh = time.After(grace)
		}
		select {
		case <-sigc:
			obs.Warn(ctx, "second signal received, exiting now")
		case <-graceCh:
			obs.Warn(ctx, "shutdown grace elapsed, exiting now")
		}
		exit(5) // §14.6: interrupted by a signal
		return nil
	})
}

// installSignalEscape wires signalEscape to real OS signals and a real exit.
func installSignalEscape(ctx obs.Ctx, grace time.Duration, flush func()) {
	sigc := make(chan os.Signal, 2)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	signalEscape(ctx, sigc, grace, func(code int) {
		flush()
		os.Exit(code)
	})
}

// once wraps flush so the deferred call and the fatal handler cannot double-close.
func once(f func()) func() {
	var o sync.Once
	return func() { o.Do(f) }
}

// ---------------------------------------------------------------------------
// sender
// ---------------------------------------------------------------------------

func runSender(args []string) int {
	fs := flag.NewFlagSet("esync", flag.ContinueOnError)
	var help bytes.Buffer
	fs.SetOutput(&help)

	c := registerCommon(fs)
	bind := fs.String("bind", "", "listen address (default all interfaces)")
	port := fs.Int("port", 0, "listen port (default ephemeral)")
	pairTimeout := fs.Duration("pair-timeout", 10*time.Minute, "how long to wait for a receiver")
	maxPairAttempts := fs.Int("max-pair-attempts", 5, "rejected-handshake budget")
	followSymlinks := fs.Bool("follow-symlinks", false, "follow symlinks instead of recording them")
	oneFileSystem := fs.Bool("one-file-system", true, "do not cross mount points")
	hardlinks := fs.Bool("hardlinks", true, "preserve hard links")
	keepSystemFiles := fs.Bool("keep-system-files", false, "disable the sys-v1 exclusion set")
	hashWorkers := fs.Int("hash-workers", min(8, runtime.NumCPU()), "concurrent hashers")
	readConcurrency := fs.Int("read-concurrency", 4, "concurrent file reads per channel")
	groupBytes := fs.Int64("group-bytes", 512<<20, "target manifest group size in bytes")
	_ = fs.Int("spill-threshold", 500_000, "external-sort spill threshold (parsed; in-memory sort in this build)")

	var filters []plan.FilterRule
	fs.Var(filterFlag{&filters, true}, "include", "include glob (repeatable)")
	fs.Var(filterFlag{&filters, false}, "exclude", "exclude glob (repeatable)")

	if err := fs.Parse(args); err != nil {
		fmt.Fprint(os.Stderr, help.String())
		return usageErr(err)
	}
	if err := applyEnv(fs); err != nil {
		return usageErr(err)
	}

	rest := fs.Args()
	switch len(rest) {
	case 0:
		return usageErr(fmt.Errorf("a source path is required"))
	case 1:
	default:
		return usageErr(fmt.Errorf("exactly one source path is expected, got %d", len(rest)))
	}

	lvl, err := c.resolveLevel()
	if err != nil {
		return usageErr(err)
	}
	hashAlg, err := parseHash(*c.hash)
	if err != nil {
		return usageErr(err)
	}
	for _, e := range []error{
		atLeast("log-ring", *c.logRing, 1),
		atLeast("chunk-size", *c.chunkSize, 4096),
		atLeast("socket-buffer", *c.socketBuffer, 4096),
		atLeast("max-retries", *c.maxRetries, 0),
		atLeast("port", *port+1, 1), // port 0 allowed (ephemeral)
		atLeast("max-pair-attempts", *maxPairAttempts, 1),
		atLeast("hash-workers", *hashWorkers, 1),
		atLeast("read-concurrency", *readConcurrency, 1),
		atLeast("group-bytes", int(*groupBytes), 1<<20),
	} {
		if e != nil {
			return usageErr(e)
		}
	}

	ctx, flushRaw := obs.Init(c.obsConfig("sender", lvl))
	flush := once(flushRaw)
	defer flush()
	installFatalHandler(ctx, flush)
	installSignalEscape(ctx, *c.shutdownGrace, flush)

	cfg := sender.Config{
		SourcePath:       rest[0],
		Bind:             *bind,
		Port:             *port,
		Version:          version,
		MaxChannels:      32,
		PairTimeout:      *pairTimeout,
		MaxPairAttempts:  *maxPairAttempts,
		HandshakeTimeout: *c.handshakeTimeout,
		DrainTimeout:     60 * time.Second,
		ProgressInterval: *c.progressInterval,
		GroupBytes:       *groupBytes,
		HashAlg:          hashAlg,
		HashWorkers:      *hashWorkers,
		ReadConcurrency:  *readConcurrency,
		ChunkSize:        *c.chunkSize,
		SocketBuffer:     *c.socketBuffer,
		FollowSymlinks:   *followSymlinks,
		OneFileSystem:    *oneFileSystem,
		Hardlinks:        *hardlinks,
		KeepSystemFiles:  *keepSystemFiles,
		Quick:            *c.quick,
		NoCache:          *c.noCache,
		Filters:          filters,
	}

	_, code := sender.Run(ctx, cfg)
	return code
}

// ---------------------------------------------------------------------------
// receiver
// ---------------------------------------------------------------------------

func runReceiver(args []string) int {
	fs := flag.NewFlagSet("esync", flag.ContinueOnError)
	var help bytes.Buffer
	fs.SetOutput(&help)

	c := registerCommon(fs)
	link := fs.String("link", "", "pairing code (required)")
	dest := fs.String("dest", "", "destination directory (default ./<source-basename>)")
	dryRun := fs.Bool("dry-run", false, "pair, plan, decide; write nothing")
	channels := fs.Int("channels", 4, "data-channel count; pins N and disables adaptive tuning")
	minChannels := fs.Int("min-channels", 4, "adaptive tuner floor for N")
	maxChannels := fs.Int("max-channels", 32, "adaptive tuner ceiling for N")
	pipelineDepth := fs.Int("pipeline-depth", 2, "outstanding requests per channel")
	groupCredit := fs.Int("group-credit", 4, "manifest credit window")
	decideWorkers := fs.Int("decide-workers", min(8, runtime.NumCPU()), "concurrent decision workers")
	noFsync := fs.Bool("no-fsync", false, "skip fsync before publish")
	checkpointInterval := fs.Int64("checkpoint-interval", 64<<20, "resume checkpoint interval (parsed)")
	resumeWindow := fs.Duration("resume-window", 60*time.Second, "reconnect window (parsed only)")
	drainTimeout := fs.Duration("drain-timeout", 60*time.Second, "post-transfer drain watchdog")
	stallTimeout := fs.Duration("stall-timeout", 60*time.Second, "max silence on a data channel before it is declared dead")
	allowUnsafeLinks := fs.Bool("allow-unsafe-links", false, "materialise absolute/escaping symlinks")
	owner := fs.Bool("owner", false, "restore ownership")

	if err := fs.Parse(args); err != nil {
		fmt.Fprint(os.Stderr, help.String())
		return usageErr(err)
	}
	if err := applyEnv(fs); err != nil {
		return usageErr(err)
	}
	if fs.NArg() > 0 {
		return usageErr(fmt.Errorf("unexpected argument %q (receiver takes no positional path)", fs.Arg(0)))
	}
	if strings.TrimSpace(*link) == "" {
		return usageErr(fmt.Errorf("--link <code> is required"))
	}

	lvl, err := c.resolveLevel()
	if err != nil {
		return usageErr(err)
	}
	hashAlg, err := parseHash(*c.hash)
	if err != nil {
		return usageErr(err)
	}
	for _, e := range []error{
		atLeast("log-ring", *c.logRing, 1),
		atLeast("chunk-size", *c.chunkSize, 4096),
		atLeast("socket-buffer", *c.socketBuffer, 4096),
		atLeast("max-retries", *c.maxRetries, 0),
		atLeast("channels", *channels, 1),
		atLeast("min-channels", *minChannels, 1),
		atLeast("max-channels", *maxChannels, *minChannels),
		atLeast("pipeline-depth", *pipelineDepth, 1),
		atLeast("group-credit", *groupCredit, 1),
		atLeast("decide-workers", *decideWorkers, 1),
	} {
		if e != nil {
			return usageErr(e)
		}
	}

	ctx, flushRaw := obs.Init(c.obsConfig("receiver", lvl))
	flush := once(flushRaw)
	defer flush()
	installFatalHandler(ctx, flush)
	installSignalEscape(ctx, *c.shutdownGrace, flush)

	cfg := receiver.Config{
		Link:               *link,
		Dest:               *dest,
		Version:            version,
		DryRun:             *dryRun,
		Channels:           *channels,
		ChannelsPinned:     explicitlySet(fs, "channels"),
		MinChannels:        *minChannels,
		MaxChannels:        *maxChannels,
		PipelineDepth:      *pipelineDepth,
		GroupCredit:        *groupCredit,
		DecideWorkers:      *decideWorkers,
		NoFsync:            *noFsync,
		CheckpointInterval: *checkpointInterval,
		ResumeWindow:       *resumeWindow,
		DrainTimeout:       *drainTimeout,
		StallTimeout:       *stallTimeout,
		ProgressInterval:   *c.progressInterval,
		AllowUnsafeLinks:   *allowUnsafeLinks,
		Owner:              *owner,
		Quick:              *c.quick,
		NoCache:            *c.noCache,
		ConnectTimeout:     *c.connectTimeout,
		HandshakeTimeout:   *c.handshakeTimeout,
		Stagger:            250 * time.Millisecond,
		SocketBuffer:       *c.socketBuffer,
		MaxRetries:         *c.maxRetries,
		HashAlg:            hashAlg,
	}

	_, code := receiver.Run(ctx, cfg)
	return code
}
