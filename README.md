# esync

One-shot, zero-configuration, single-direction encrypted file-tree transfer for
two machines on the same LAN. The sender runs `esync <path>` and prints a short
pairing code; the receiver runs `esync --link <code>` and the directory is copied
once — encrypted, authenticated, integrity-checked, differential, and
parallelised to saturate the link. No accounts, no daemon, no persistent state.

See [`doc/ARCHITECTURE.md`](doc/ARCHITECTURE.md) and
[`doc/INITIAL_REQS.md`](doc/INITIAL_REQS.md) for the full design.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/thanosa75/esync/main/scripts/install.sh | sh
```

Downloads the latest build for your platform (`linux/amd64`, `darwin/arm64`),
verifies its SHA-256, and installs it to `~/.local/bin` when that's on your
`PATH` (no sudo), otherwise `/usr/local/bin`. Override with `ESYNC_BIN_DIR`
(install location) or `ESYNC_VERSION` (release tag):

```sh
curl -fsSL https://raw.githubusercontent.com/thanosa75/esync/main/scripts/install.sh | ESYNC_BIN_DIR="$HOME/.local/bin" sh
```

The `latest` release is republished on every push to `main`. Binaries and their
`SHA256SUMS` are also attached to each run under
[Releases](https://github.com/thanosa75/esync/releases).

## Build from source

Requires Go 1.27.1.

```sh
make build          # ./bin/esync for the host platform
make dist           # static binaries for linux/macos amd64+arm64 into ./dist
make test           # full suite with -race
```

## Usage

```sh
esync ./project          # sender: prints a pairing code
esync --link <code>      # receiver: pulls the tree into the current directory
esync --help
```
