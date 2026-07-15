#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DEFAULT_FMU="$ROOT/integrations/fmu/dist/trustworthiness_checker.fmu"

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv executable not found in PATH" >&2
    exit 1
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trustworthiness-checker-uv-cache}"

env -u PYTHONPATH -u VIRTUAL_ENV \
    uv run \
        --project "$ROOT/integrations/python" \
        --locked \
        --group fmu-dev \
        python "$ROOT/integrations/fmu/benchmarks/benchmark_fmu.py" \
        --fmu "$DEFAULT_FMU" \
        "$@"
