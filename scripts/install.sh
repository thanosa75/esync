#!/bin/sh
# esync installer — downloads the latest prebuilt binary for this host.
#
#   curl -fsSL https://raw.githubusercontent.com/thanosa75/esync/main/scripts/install.sh | sh
#
# Env overrides:
#   ESYNC_VERSION   release tag to fetch          (default: latest)
#   ESYNC_BIN_DIR   install directory             (default: /usr/local/bin)
set -eu

REPO="thanosa75/esync"
TAG="${ESYNC_VERSION:-latest}"
BIN_DIR="${ESYNC_BIN_DIR:-/usr/local/bin}"

die() { echo "esync: $*" >&2; exit 1; }

os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
	Linux)  os="linux" ;;
	Darwin) os="darwin" ;;
	*) die "unsupported OS: $os" ;;
esac
case "$arch" in
	x86_64 | amd64) arch="amd64" ;;
	arm64 | aarch64) arch="arm64" ;;
	*) die "unsupported architecture: $arch" ;;
esac

case "${os}-${arch}" in
	linux-amd64 | darwin-arm64) ;;
	*) die "no prebuilt binary published for ${os}/${arch} — build from source with 'make build'" ;;
esac

asset="esync-${os}-${arch}"
base="https://github.com/${REPO}/releases/download/${TAG}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "esync: downloading ${asset} (${TAG})" >&2
curl -fsSL -o "$tmp/$asset" "${base}/${asset}" || die "download failed: ${base}/${asset}"

if curl -fsSL -o "$tmp/SHA256SUMS" "${base}/SHA256SUMS" 2>/dev/null; then
	if command -v sha256sum >/dev/null 2>&1; then
		got="$(sha256sum "$tmp/$asset" | awk '{print $1}')"
	elif command -v shasum >/dev/null 2>&1; then
		got="$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')"
	else
		got=""
	fi
	want="$(awk -v a="$asset" '$2 == a || $2 == "*"a {print $1}' "$tmp/SHA256SUMS")"
	if [ -n "$got" ] && [ -n "$want" ] && [ "$got" != "$want" ]; then
		die "checksum mismatch for ${asset} (got ${got}, want ${want})"
	fi
	[ -n "$got" ] && [ -n "$want" ] && echo "esync: checksum ok" >&2
fi

chmod +x "$tmp/$asset"

target="${BIN_DIR}/esync"
if [ -d "$BIN_DIR" ] && [ -w "$BIN_DIR" ]; then
	mv "$tmp/$asset" "$target"
elif [ "$(id -u)" = 0 ]; then
	mkdir -p "$BIN_DIR" && mv "$tmp/$asset" "$target"
elif command -v sudo >/dev/null 2>&1; then
	echo "esync: ${BIN_DIR} needs elevated write — using sudo" >&2
	sudo mkdir -p "$BIN_DIR" && sudo mv "$tmp/$asset" "$target"
else
	die "cannot write to ${BIN_DIR}; set ESYNC_BIN_DIR to a writable path"
fi

echo "esync: installed $("$target" --version 2>/dev/null || echo "to $target")" >&2
