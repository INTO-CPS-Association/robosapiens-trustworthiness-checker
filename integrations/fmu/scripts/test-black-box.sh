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

FMU_PATH="$(cd "$(dirname "$FMU_PATH")" && pwd)/$(basename "$FMU_PATH")"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trustworthiness-checker-uv-cache}"

env -u PYTHONPATH -u VIRTUAL_ENV \
    TC_FMU_PATH="$FMU_PATH" \
    uv run \
        --project "$ROOT/integrations/python" \
        --locked \
        --group dev \
        --group fmu-dev \
        pytest -v "$ROOT/integrations/fmu/tests/test_black_box.py"
