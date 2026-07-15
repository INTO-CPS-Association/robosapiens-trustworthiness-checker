#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FMU_DIR="$ROOT/integrations/fmu"
BUILD_DIR="$FMU_DIR/build"
SKELETON_DIR="$BUILD_DIR/trustworthiness_checker"
DIST_DIR="$FMU_DIR/dist"
FMU_PATH="$DIST_DIR/trustworthiness_checker.fmu"
UNIFMU_BINARY="unifmu"
FMU_PLATFORM="linux64"
case "$(uname -s)" in
    Darwin*)
        FMU_PLATFORM="darwin64"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        UNIFMU_BINARY="unifmu.exe"
        FMU_PLATFORM="win64"
        ;;
esac
UNIFMU="${UNIFMU:-$FMU_DIR/tools/$UNIFMU_BINARY}"
SPEC_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --spec-dir)
            SPEC_DIR="$(cd "$2" && pwd)"
            shift 2
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            echo "usage: $0 --spec-dir PATH" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$SPEC_DIR" ]]; then
    echo "error: --spec-dir PATH is required" >&2
    echo "usage: $0 --spec-dir PATH" >&2
    exit 2
fi

if [[ ! -f "$SPEC_DIR/spec.dsrv" ]]; then
    echo "error: specification example does not contain spec.dsrv: $SPEC_DIR" >&2
    exit 1
fi

if [[ ! -x "$UNIFMU" ]]; then
    if command -v unifmu >/dev/null 2>&1; then
        UNIFMU="$(command -v unifmu)"
    else
        echo "error: UniFMU release executable not found" >&2
        echo "run integrations/fmu/scripts/install-unifmu.sh or set UNIFMU=/path/to/unifmu" >&2
        exit 1
    fi
fi

if ! "$UNIFMU" generate --help 2>&1 | grep -q "FMU_VERSION"; then
    echo "error: '$UNIFMU' does not look like UniFMU 0.14.x" >&2
    echo "run integrations/fmu/scripts/install-unifmu.sh or set UNIFMU=/path/to/the/latest/release/unifmu" >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv executable not found in PATH" >&2
    exit 1
fi

PYTHON="${PYTHON:-python}"
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "error: Python executable not found: $PYTHON" >&2
    exit 1
fi
PYTHON_VERSION="$("$PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$PYTHON_VERSION" != "3.12" ]]; then
    echo "error: FMU packaging requires CPython 3.12; '$PYTHON' is $PYTHON_VERSION" >&2
    echo "set PYTHON=/path/to/python3.12 and rerun this script" >&2
    exit 1
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trustworthiness-checker-uv-cache}"

rm -rf "$BUILD_DIR" "$DIST_DIR"
mkdir -p "$BUILD_DIR" "$DIST_DIR"

"$UNIFMU" generate python "$SKELETON_DIR" fmi2
for platform_dir in "$SKELETON_DIR"/binaries/*; do
    if [[ "$(basename "$platform_dir")" != "$FMU_PLATFORM" ]]; then
        rm -rf "$platform_dir"
    fi
done

cp "$FMU_DIR/runtime/backend.py" "$SKELETON_DIR/resources/backend.py"
cp "$FMU_DIR/runtime/launch.sh" "$SKELETON_DIR/resources/launch.sh"
cp "$FMU_DIR/runtime/launch.toml" "$SKELETON_DIR/resources/launch.toml"
cp "$FMU_DIR/runtime/main.py" "$SKELETON_DIR/resources/main.py"
cp "$FMU_DIR/runtime/model.py" "$SKELETON_DIR/resources/model.py"
cp "$SPEC_DIR/spec.dsrv" "$SKELETON_DIR/resources/spec.dsrv"
chmod +x "$SKELETON_DIR/resources/launch.sh"

GENERATOR_ARGS=(
    --spec "$SPEC_DIR/spec.dsrv"
    --model-description "$SKELETON_DIR/modelDescription.xml"
    --interface "$SKELETON_DIR/resources/interface.json"
)
if [[ -f "$SPEC_DIR/fmi.toml" ]]; then
    GENERATOR_ARGS+=(--annotations "$SPEC_DIR/fmi.toml")
fi
cargo run --quiet --no-default-features --bin generate_fmu_interface -- "${GENERATOR_ARGS[@]}"

build_wheel() {
    uv run --project "$ROOT/integrations/python" --locked maturin build \
        --manifest-path "$ROOT/integrations/python/Cargo.toml" \
        --interpreter "$PYTHON" \
        --release \
        --out "$SKELETON_DIR/resources/wheels"
}

build_wheel

WHEEL_PATH=("$SKELETON_DIR"/resources/wheels/trustworthiness_checker-*.whl)
uv pip install \
    --target "$SKELETON_DIR/resources/site-packages" \
    "${WHEEL_PATH[0]}" \
    "protobuf==5.27.3" \
    "pyzmq==27.1.0"

(
    cd "$SKELETON_DIR"
    "$PYTHON" -m zipfile -c "$FMU_PATH" binaries resources modelDescription.xml
)

echo "$FMU_PATH"
