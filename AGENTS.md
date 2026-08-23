# Normal development

- Prefer `--profile dev-fast` for routine builds, checks, and tests.
- Do not use `dev-fast` for debugging or profiling; use the profile appropriate to the debugger or profiling task.
- Production release builds should enable jemalloc explicitly: `cargo build --release --features jemalloc`.

# Testing

- Default features cover everything except ROS and container-backed I/O:
  - `cargo test --profile dev-fast --lib` for the fast unit tests.
  - `cargo test --profile dev-fast` also runs `cli_tests`, `runtime_tests`, and `test_distributed`.
- Check a feature set with `cargo check --profile dev-fast --all-targets [--features ...]` before running its tests. A feature-gated suite reports `0 tests` rather than failing when its feature is off, so a green run proves nothing about the suites you did not enable.
- `test_mqtt_io`, `test_redis_io`, and `test_distributed_mqtt` need `--features testcontainers` and a Docker-compatible socket. Rootless Podman works unchanged:
  - `systemctl --user start podman.socket`
  - `export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock`
  - `cargo test --profile dev-fast --features testcontainers`
  - The suites start and remove their own Mosquitto and Redis containers.
- `test_ros_io` and the ROS-gated unit tests need `--features ros`, which requires a sourced ROS 2 environment plus a `ros_interfaces` overlay containing every message under `ros_interfaces/`:
  - With a local install, build the overlay once (`cd ros_interfaces && colcon build`), source `/opt/ros/<distro>/setup.bash` and `ros_interfaces/install/setup.bash`, then run `cargo test --profile dev-fast --features ros`.
  - Otherwise use the project image (`docker/docker-compose.yaml`, target `dev`), which prebuilds the overlay and sources both from its entrypoint:

    ```sh
    podman run --rm --userns=keep-id --security-opt label=disable \
        -v "$PWD":/ws/tc -w /ws/tc \
        -e CARGO_TARGET_DIR=/ws/tc/target/ros-container \
        -e CARGO_HOME=/ws/tc/target/ros-cargo-home \
        trustworthiness-checker:dev \
        bash -c 'export PATH=$HOME/.cargo/bin:$PATH; cargo test --profile dev-fast --features ros'
    ```

  - Keep a container-specific `CARGO_TARGET_DIR` so the container toolchain does not invalidate the host build cache, and use `--userns=keep-id` so the container user can write to the mounted worktree.
  - The image fixes the ROS overlay in at build time, so rebuild it after changing `ros_interfaces/`; otherwise ROS builds fail with missing message types. With Podman, build it as `podman build --format docker -f docker/Dockerfile --target dev --build-arg UID=$(id -u) --build-arg GID=$(id -g) -t trustworthiness-checker:dev .` — the default OCI format ignores the Dockerfile's `SHELL` directive, which breaks the `colcon build` layer.
  - If you are already inside a ROS devcontainer or a system with a functioning local ROS installation, do not launch additional nested ROS containers.
- Finish with `cargo fmt --all -- --check` and `git diff --check`.

# Git safety

- Never force push to `origin/main`, including with `--force` or `--force-with-lease`.

## Delta worktree safety

- When operating through Delta, do not use `git stash --include-untracked`: removing and recreating untracked files can cause Delta to misidentify renames and corrupt file paths. Prefer a Delta-provided checkpoint/apply workflow when available; otherwise use a temporary checkpoint commit and squash it afterward.
- When operating through Delta, after a `HEAD`-moving operation, stop and inspect any Delta external-change report that infers unexpected renames before making further changes.

# Benchmarking

- Once benchmarking has started, consistently use `--profile bench-fast` for subsequent builds, checks, and benchmark runs so the benchmark cache stays warm.
- Always use the `bench-fast` profile and jemalloc: `cargo bench --profile bench-fast --features jemalloc ...`.
- Compile normally so Rust can use all cores: `cargo bench --profile bench-fast --features jemalloc --bench <name> --no-run`.
- Pin only the resulting benchmark executable to a P-core. Identify P-cores with `lscpu -e=CPU,CORE,MAXMHZ`; choose an otherwise-idle CPU with the highest `MAXMHZ`, then run `taskset -c <P_CORE> target/bench-fast/deps/<benchmark-binary> '<filter>' --bench`. Never pin `cargo` or `rustc` to one core.
- For comparisons with `main`, create once and reuse the named worktree `../robosapiens-trustworthiness-checker-main-bench` (for example, `git worktree add ../robosapiens-trustworthiness-checker-main-bench main`). Build and benchmark `main` there so its target cache remains warm; do not repeatedly switch the working branch or recreate the worktree.
- Compare equivalent benchmark binaries with the same P-core, filters, profile, and machine load. Avoid running benchmarks during backups or other sustained CPU activity.
