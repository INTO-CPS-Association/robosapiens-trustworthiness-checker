#!/usr/bin/env sh
set -eu

usage() {
    cat <<'EOF'
Usage: scripts/use_zed_settings.sh <default|ros-devcontainer>

Copies one of the committed Zed settings templates to .zed/settings.json.
The generated .zed/settings.json is intentionally ignored by git.
EOF
}

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

case "$1" in
    default)
        template="$repo_root/.zed/settings.default.json"
        ;;
    ros-devcontainer|ros)
        template="$repo_root/.zed/settings.ros-devcontainer.json"
        ;;
    -h|--help|help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown Zed settings profile: $1" >&2
        usage >&2
        exit 2
        ;;
esac

if [ ! -f "$template" ]; then
    echo "Missing Zed settings template: $template" >&2
    exit 1
fi

cp "$template" "$repo_root/.zed/settings.json"
printf 'Updated .zed/settings.json from %s\n' "$template"
