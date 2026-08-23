#!/usr/bin/env sh
set -eu

usage() {
    cat <<'EOF'
Usage: scripts/use_zed_settings.sh [OPTIONS] <default|ros-devcontainer|ros-local-jazzy>

Selects a Zed settings profile and writes it to .zed/settings.json.
The generated .zed/settings.json is intentionally ignored by git.

Options override the selected profile for both rust-analyzer project loading
and `cargo check`:
  --features LIST          Replace enabled features (comma-separated)
  --all-features           Enable every Cargo feature
  --no-all-features        Do not enable every Cargo feature
  --default-features       Enable the package's default features
  --no-default-features    Disable the package's default features
  --all-targets            Analyze all Cargo targets
  --no-all-targets         Do not analyze all Cargo targets
  --lean                    Equivalent to --features '' --no-all-features
                            --no-default-features --no-all-targets
  -h, --help                Show this help

Examples:
  scripts/use_zed_settings.sh --lean default
  scripts/use_zed_settings.sh --features ros ros-local-jazzy
  scripts/use_zed_settings.sh --features jit --no-all-targets default
  scripts/use_zed_settings.sh --all-features --all-targets default

The ros-local-jazzy profile requires /opt/ros/jazzy and a built
ros_interfaces/install overlay.
EOF
}

profile=
features_override=
features_set=false
all_features=
no_default_features=
all_targets=

while [ "$#" -gt 0 ]; do
    case "$1" in
        --features)
            if [ "$#" -lt 2 ]; then
                echo "Missing value for --features" >&2
                usage >&2
                exit 2
            fi
            features_override=$2
            features_set=true
            shift 2
            ;;
        --all-features)
            all_features=true
            shift
            ;;
        --no-all-features)
            all_features=false
            shift
            ;;
        --default-features)
            no_default_features=false
            shift
            ;;
        --no-default-features)
            no_default_features=true
            shift
            ;;
        --all-targets)
            all_targets=true
            shift
            ;;
        --no-all-targets)
            all_targets=false
            shift
            ;;
        --lean)
            features_override=
            features_set=true
            all_features=false
            no_default_features=true
            all_targets=false
            shift
            ;;
        -h|--help|help)
            usage
            exit 0
            ;;
        --*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [ -n "$profile" ]; then
                echo "Only one Zed settings profile may be selected" >&2
                usage >&2
                exit 2
            fi
            profile=$1
            shift
            ;;
    esac
done

if [ -z "$profile" ]; then
    echo "A Zed settings profile is required" >&2
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

case "$profile" in
    default)
        template="$repo_root/.zed/settings.default.json"
        ;;
    ros-devcontainer|ros)
        template="$repo_root/.zed/settings.ros-devcontainer.json"
        ;;
    ros-local-jazzy|local-jazzy|jazzy)
        use_local_jazzy
        template=
        ;;
    *)
        echo "Unknown Zed settings profile: $profile" >&2
        usage >&2
        exit 2
        ;;
esac

if [ -n "$template" ]; then
    if [ ! -f "$template" ]; then
        echo "Missing Zed settings template: $template" >&2
        exit 1
    fi
    cp "$template" "$settings"
    printf 'Updated .zed/settings.json from %s\n' "$template"
fi

if [ "$features_set" = true ] || [ -n "$all_features" ] || \
    [ -n "$no_default_features" ] || [ -n "$all_targets" ]; then
    if ! command -v python3 >/dev/null 2>&1; then
        echo "python3 is required to override Zed settings" >&2
        exit 1
    fi

    python3 - "$settings" "$features_set" "$features_override" \
        "$all_features" "$no_default_features" "$all_targets" <<'PY'
import json
import sys

path, features_set, features, all_features, no_default_features, all_targets = sys.argv[1:]
with open(path, encoding="utf-8") as source:
    contents = "".join(
        line for line in source if not line.lstrip().startswith("//")
    )
settings = json.loads(contents)

initialization_options = settings["lsp"]["rust-analyzer"]["initialization_options"]
configurations = [
    initialization_options.setdefault("cargo", {}),
    initialization_options.setdefault("check", {}),
]

def optional_bool(value):
    return None if not value else value == "true"

for configuration in configurations:
    if features_set == "true":
        configuration["features"] = [
            feature.strip() for feature in features.split(",") if feature.strip()
        ]
    for key, value in (
        ("allFeatures", optional_bool(all_features)),
        ("noDefaultFeatures", optional_bool(no_default_features)),
        ("allTargets", optional_bool(all_targets)),
    ):
        if value is not None:
            configuration[key] = value

with open(path, "w", encoding="utf-8") as output:
    json.dump(settings, output, indent=2)
    output.write("\n")
PY
    printf 'Applied rust-analyzer overrides to .zed/settings.json\n'
fi
