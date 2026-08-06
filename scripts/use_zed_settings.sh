#!/usr/bin/env sh
set -eu

usage() {
    cat <<'EOF'
Usage: scripts/use_zed_settings.sh <default|ros-devcontainer|ros-local-jazzy>

Selects a Zed settings profile and writes it to .zed/settings.json.
The generated .zed/settings.json is intentionally ignored by git.

The ros-local-jazzy profile requires /opt/ros/jazzy and a built
ros_interfaces/install overlay.
EOF
}

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
settings="$repo_root/.zed/settings.json"

use_local_jazzy() {
    template="$repo_root/.zed/settings.ros-local-jazzy.json"
    ros_setup=/opt/ros/jazzy/setup.sh
    overlay_setup="$repo_root/ros_interfaces/install/setup.sh"

    if [ ! -f "$template" ]; then
        echo "Missing Zed settings template: $template" >&2
        exit 1
    fi
    if [ ! -f "$ros_setup" ]; then
        echo "Missing ROS Jazzy setup: $ros_setup" >&2
        exit 1
    fi
    if [ ! -f "$overlay_setup" ]; then
        echo "Missing local ROS interface overlay: $overlay_setup" >&2
        echo "Build it with: cd ros_interfaces && colcon build" >&2
        exit 1
    fi
    if ! command -v python3 >/dev/null 2>&1; then
        echo "python3 is required to generate local ROS Zed settings" >&2
        exit 1
    fi

    (
        set +u
        . "$ros_setup"
        . "$overlay_setup"
        dsrv_lsp=
        if command -v dsrv-lsp >/dev/null 2>&1; then
            dsrv_lsp=$(command -v dsrv-lsp)
        fi
        python3 - "$template" "$settings" "$dsrv_lsp" <<'PY'
import json
import os
import sys

keys = (
    "ROS_VERSION",
    "ROS_PYTHON_VERSION",
    "ROS_DISTRO",
    "AMENT_PREFIX_PATH",
    "COLCON_PREFIX_PATH",
    "CMAKE_PREFIX_PATH",
    "PYTHONPATH",
    "LD_LIBRARY_PATH",
    "PKG_CONFIG_PATH",
    "PATH",
)
environment = {key: os.environ[key] for key in keys if key in os.environ}
with open(sys.argv[1], encoding="utf-8") as source:
    settings = json.load(source)

initialization_options = settings["lsp"]["rust-analyzer"]["initialization_options"]
initialization_options["cargo"]["extraEnv"] = environment
initialization_options["check"]["extraEnv"] = environment
settings["terminal"]["env"] = environment

if sys.argv[3]:
    settings["lsp"]["dsrv-lsp"]["binary"]["path"] = sys.argv[3]
else:
    settings["languages"].pop("DSRV")
    settings["lsp"].pop("dsrv-lsp")
    if not settings["languages"]:
        settings.pop("languages")

with open(sys.argv[2], "w", encoding="utf-8") as output:
    json.dump(settings, output, indent=2)
    output.write("\n")
PY
    )
    printf 'Updated .zed/settings.json for local ROS Jazzy\n'
}

case "$1" in
    default)
        template="$repo_root/.zed/settings.default.json"
        ;;
    ros-devcontainer|ros)
        template="$repo_root/.zed/settings.ros-devcontainer.json"
        ;;
    ros-local-jazzy|local-jazzy|jazzy)
        use_local_jazzy
        exit 0
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

cp "$template" "$settings"
printf 'Updated .zed/settings.json from %s\n' "$template"
