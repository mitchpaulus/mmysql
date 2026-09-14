#!/bin/sh
# Installs the latest mmysql release (or $MMYSQL_VERSION, e.g. v1.2.0).
#   curl -fsSL https://raw.githubusercontent.com/mitchpaulus/mmysql/main/install.sh | sh
# Set MMYSQL_INSTALL_DIR to change the destination (default: ~/.local/bin).
set -eu

repo="mitchpaulus/mmysql"
install_dir="${MMYSQL_INSTALL_DIR:-$HOME/.local/bin}"
version="${MMYSQL_VERSION:-}"

case "$(uname -s)" in
    Linux)  os="linux" ;;
    Darwin) os="darwin" ;;
    *) echo "error: unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
    x86_64|amd64)  arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) echo "error: unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

asset="mmysql-$os-$arch"
if [ -n "$version" ]; then
    url="https://github.com/$repo/releases/download/$version/$asset"
else
    url="https://github.com/$repo/releases/latest/download/$asset"
fi

mkdir -p "$install_dir"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "Downloading $url"
if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$tmp" "$url"
elif command -v wget >/dev/null 2>&1; then
    wget -qO "$tmp" "$url"
else
    echo "error: curl or wget is required" >&2
    exit 1
fi

chmod +x "$tmp"
mv "$tmp" "$install_dir/mmysql"
trap - EXIT

echo "Installed $("$install_dir/mmysql" version) to $install_dir/mmysql"

case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) echo "Note: $install_dir is not on your PATH." ;;
esac
