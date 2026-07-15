#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FMU_PATH="${1:-$ROOT/integrations/fmu/dist/trustworthiness_checker.fmu}"

if [[ ! -f "$FMU_PATH" ]]; then
    echo "error: FMU not found: $FMU_PATH" >&2
    echo "run integrations/fmu/scripts/build.sh first, or pass an FMU path" >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv executable not found in PATH" >&2
    exit 1
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trustworthiness-checker-uv-cache}"

run_fmpy() {
    uv run \
        --project "$ROOT/integrations/python" \
        --locked \
        --group fmu-dev \
        fmpy "$@"
}

run_fmpy validate "$FMU_PATH"
run_fmpy simulate "$FMU_PATH" --stop-time 1.0
