// Command esync is a one-shot, zero-configuration, single-direction file-tree
// transfer for two machines on the same LAN. See doc/ARCHITECTURE.md.
//
// The sender runs "esync <path>" and prints a pairing code; the receiver runs
// "esync --link <code>". main.go does flag parsing, mode dispatch, and exit-code
// mapping only (ARCHITECTURE §17) — the state machines live in internal/sender
// and internal/receiver.
package main

import (
	"fmt"
	"os"
	"runtime"
)

// Stamped by the linker via -ldflags (see Makefile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run is the real entrypoint; it returns the process exit code (ARCHITECTURE §14.6).
func run(args []string) int {
	for _, a := range args {
		switch a {
		case "--help", "-h", "-help":
			printUsage(os.Stdout)
			return 0
		case "--version", "-version":
			fmt.Printf("esync %s (commit %s, built %s, %s/%s)\n",
				version, commit, date, runtime.GOOS, runtime.GOARCH)
			return 0
		}
		if a == "--" {
			break
		}
	}

	if hasLinkFlag(args) {
		return runReceiver(args)
	}
	if len(args) > 0 {
		return runSender(args)
	}

	fmt.Fprintln(os.Stderr, "esync: give a source path (sender) or --link <code> (receiver)")
	fmt.Fprintln(os.Stderr, "try 'esync --help'")
	return 3 // usage error (ARCHITECTURE §14.6)
}

func printUsage(w *os.File) {
	fmt.Fprint(w, `esync — one-shot encrypted file-tree transfer over a LAN

usage:
  esync [common flags] <path>                 send a directory; prints a pairing code
  esync [common flags] --link <code> [flags]  receive into ./<name> or --dest <dir>

common flags (ARCHITECTURE §16.1):
  -v, -vv                 raise log level (INFO → DEBUG → TRACE)
  -q, -qq                 lower log level (INFO → WARN → ERROR)
  --log-level <lvl>       error|warn|info|debug|trace (wins over -v/-q)
  --log-format text|json  default text
  --log-file <path>       mirror full-fidelity logs to a file
  --log-ring <n>          trace ring size (default 2048)
  --log-sync              format and write log records synchronously
  --redact-paths          replace paths with digests in logs
  --progress-interval <d> default 5s
  --shutdown-grace <d>    force-exit delay after the first signal (default 5s)
  --hash md5|sha256       content digest (default md5; blake3 reserved)
  --quick                 trust size+mtime, skip same-size re-hash
  --no-cache              ignore the on-disk digest cache
  --chunk-size <bytes>    default 1MiB
  --socket-buffer <bytes> default 4MiB
  --max-retries <n>       default 3
  --connect-timeout <d>   default 10s
  --handshake-timeout <d> default 5s
  --version, --help

sender flags (ARCHITECTURE §16.2):
  --bind <addr>           listen address (default all interfaces)
  --port <n>              listen port (default ephemeral)
  --pair-timeout <d>      default 10m
  --max-pair-attempts <n> default 5
  --follow-symlinks       follow instead of recording symlinks
  --one-file-system       do not cross mount points (default on)
  --hardlinks             preserve hard links (default on)
  --keep-system-files     do not apply the sys-v1 exclusion set
  --include <glob>        (repeatable, interleaved with --exclude)
  --exclude <glob>        (repeatable)
  --hash-workers <n>      default min(8, NumCPU)
  --read-concurrency <n>  default 4

receiver flags (ARCHITECTURE §16.3):
  --link <code>           pairing code (required)
  --dest <dir>            destination (default ./<source-basename>)
  --dry-run               pair, plan, and decide; write nothing
  --channels <n>          pin the data-channel count (disables adaptive tuning)
  --min-channels <n>      adaptive tuner floor (default 4)
  --max-channels <n>      adaptive tuner ceiling (default 32)
  --pipeline-depth <n>    default 2
  --group-credit <n>      default 4
  --decide-workers <n>    default min(8, NumCPU)
  --no-fsync              skip fsync before publish
  --drain-timeout <d>     default 60s
  --allow-unsafe-links    materialise absolute/escaping symlinks
  --owner                 restore ownership (needs privilege)

exit codes: 0 ok · 1 item failures · 2 fatal · 3 usage · 4 pairing/auth · 5 signal
`)
}
