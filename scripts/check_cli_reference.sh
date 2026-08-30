#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:---check}"

case "$MODE" in
    --write)
        cd "$ROOT_DIR"
        cargo run --quiet --bin generate_cli_reference -- \
            --output docs/src/reference/cli.md
        ;;
    --check)
        cd "$ROOT_DIR"
        cargo run --quiet --bin generate_cli_reference -- \
            --check --output docs/src/reference/cli.md
        ;;
    *)
        printf 'Usage: %s [--write|--check]\n' "$0" >&2
        exit 2
        ;;
esac
