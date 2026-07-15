#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TOOLS_DIR="$ROOT/integrations/fmu/tools"
VERSION="${UNIFMU_VERSION:-0.14.0}"

case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
        TARGET="x86_64-unknown-linux-gnu"
        BINARY="unifmu"
        ;;
    Darwin-x86_64)
        TARGET="x86_64-apple-darwin"
        BINARY="unifmu"
        ;;
    MINGW*-x86_64|MSYS*-x86_64|CYGWIN*-x86_64)
        TARGET="x86_64-pc-windows-gnu"
        BINARY="unifmu.exe"
        ;;
    *)
        echo "error: unsupported UniFMU release platform: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

URL="https://github.com/INTO-CPS-Association/unifmu/releases/download/v${VERSION}/unifmu-${TARGET}-${VERSION}.zip"
ARCHIVE="$TOOLS_DIR/unifmu-${TARGET}-${VERSION}.zip"

mkdir -p "$TOOLS_DIR"
curl -L "$URL" -o "$ARCHIVE"
python -m zipfile -e "$ARCHIVE" "$TOOLS_DIR"
chmod +x "$TOOLS_DIR/$BINARY"

"$TOOLS_DIR/$BINARY" --version
