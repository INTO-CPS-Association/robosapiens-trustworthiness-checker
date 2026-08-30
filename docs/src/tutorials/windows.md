# Extended Windows usage

The main [Getting started](../getting-started.md) path is Linux/Unix-first. Use this page when working in Windows Subsystem for Linux (WSL), building the Trustworthiness Checker (TC) as a native Windows executable, connecting that executable to Docker Desktop services, or testing a cross-compiled Windows executable with Wine on Linux.

The native and Wine examples add three pairs of explicitly typed integers, write the results to stdout, and exit. Run commands from the repository root unless a section says otherwise.

## Windows Subsystem for Linux (WSL)

WSL runs the Linux TC toolchain inside a Windows-hosted Linux distribution. Follow the [Linux/Unix Getting started instructions](../getting-started.md#linux-or-unix) inside that distribution: install the Linux build tools and rustup there, clone the repository, and run the running-total example with `cargo`. A Rust or Cargo installation on the Windows host is separate and is not used by the WSL shell.

Keep active source checkouts in the WSL filesystem, such as under `~/src`, rather than under `/mnt/c`, for normal Linux filesystem performance. Windows drives remain available under `/mnt/<drive>` when a command needs to read or write a Windows-hosted file.

Docker Desktop can provide MQTT or Redis containers to WSL when Docker Desktop WSL integration is enabled for that distribution. This is optional for the running-total example in Getting Started. Confirm availability from WSL with `docker version` before following a Docker-backed tutorial.

A successful WSL run proves the Linux target in the selected WSL distribution. It does not build or test the native Windows MSVC executable. Use the next section for a native build, or [Linux with Wine](#linux-with-wine) for the Windows GNU compatibility path.

## Native Windows

### Prerequisites

Install Visual Studio Build Tools with the Desktop development with C++ workload and a Windows SDK, plus Git and CMake.

Install Rustup from the Windows Package Manager community repository:

```powershell
winget install --exact --id Rustlang.Rustup
```

Close and reopen PowerShell so that `rustup` and `cargo` are on `PATH`. Install Rust 1.95 and explicitly add the native Windows MSVC target:

```powershell
rustup toolchain install 1.95 --profile minimal
rustup target add x86_64-pc-windows-msvc --toolchain 1.95
rustc +1.95 --version
rustup target list --toolchain 1.95 --installed
```

The installed-target list must contain `x86_64-pc-windows-msvc`.

### Add two values from a file

The Windows example uses explicit integer types while adding `x` and `y`:

```dsrv
in x : Int
in y : Int
out z : Int
z = x + y
```

The repository stores this program as `tests/fixtures/simple_add_typed.dsrv` and supplies three input pairs in `tests/fixtures/simple_add_typed.input`. Start with this file-input run before configuring a live broker. Build without the optional MQTT and Redis integrations, then run the native executable:

```powershell
cargo +1.95 build `
  --package trustworthiness_checker `
  --bin trustworthiness_checker `
  --no-default-features

& .\target\debug\trustworthiness_checker.exe `
  tests/fixtures/simple_add_typed.dsrv `
  --input-file tests/fixtures/simple_add_typed.input `
  --output-stdout
```

The process writes these results to stdout and exits:

```text
z[0] = Int(3)
z[1] = Int(7)
z[2] = Int(11)
```

This file-input process reaches end of input and exits without manual cleanup. To check the broader default feature set on a prepared Windows development host, run:

```powershell
cargo +1.95 check --all-targets
cargo +1.95 test
```

The `ros` feature requires a sourced ROS 2 environment and the project message overlay. The `testcontainers` suites require the Docker-compatible environment described in the project testing instructions; the native Windows compatibility workflow does not run them.


## Linux with Wine

Wine runs a Windows GNU executable on Linux. This is a fast compatibility check, but it does not replace a native Windows MSVC build.

### Install the tools

On Fedora:

```bash
sudo dnf install curl wine mingw64-gcc mingw64-gcc-c++ cmake ninja-build
```

On Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install curl wine mingw-w64 cmake ninja-build
```

If Rustup is not installed, download its installer from the official Rust infrastructure, inspect it, and install the minimal profile:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  --output /tmp/rustup-init.sh
less /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal
rm /tmp/rustup-init.sh
. "$HOME/.cargo/env"
```

Install Rust 1.95 and the Windows GNU target on either distribution:

```bash
rustup toolchain install 1.95 --profile minimal
rustup target add x86_64-pc-windows-gnu --toolchain 1.95
rustc +1.95 --version
rustup target list --toolchain 1.95 --installed
```

The installed-target list must contain `x86_64-pc-windows-gnu`.

The checked-in `.cargo/config.toml` selects `x86_64-w64-mingw32-gcc` as the linker and Wine as the Cargo runner.

### Initialize Wine and run the typed addition example

From the repository root, create a project-local Wine prefix:

```bash
export WINEPREFIX="$PWD/target/wine-prefix"
export WINEDEBUG=-all
WINEARCH=win64 wineboot --init
```

Build the Windows GNU executable, run it through Wine, process the fixture, and exit:

```bash
WINEPREFIX="$PWD/target/wine-prefix" WINEDEBUG=-all \
cargo +1.95 run \
  --target x86_64-pc-windows-gnu \
  --package trustworthiness_checker \
  --bin trustworthiness_checker \
  --no-default-features \
  -- \
  tests/fixtures/simple_add_typed.dsrv \
  --input-file tests/fixtures/simple_add_typed.input \
  --output-stdout
```

The expected stdout is the same three-line result shown in the native Windows section. Remove `target/wine-prefix` if you want Wine to recreate a clean Windows environment on the next run.

## Run with MQTT or Redis on Windows

After the file-input example works, use these live-input examples to run the TC natively in PowerShell with a local broker provided by Docker Desktop. The first `docker run` downloads a public image from Docker Hub if it is not already present. Run all commands from the repository root.

Unlike finite file input, the TC remains in the foreground while it waits for broker messages. Press `Ctrl+C` to stop it after observing the expected output.

### MQTT input with Rumqttc

Generic MQTT input uses Rumqttc by default. Rumqttc input is compiled even with `--no-default-features`; the optional `mqtt` feature selects Paho-backed functionality such as MQTT output and is not required here.

Start Mosquitto with the image's included unauthenticated local-development configuration. The published broker port is restricted to Windows loopback:

```powershell
docker run --rm --detach `
  --name tc-mqtt `
  --publish 127.0.0.1:1883:1883 `
  docker.io/library/eclipse-mosquitto:2 `
  mosquitto -c /mosquitto-no-auth.conf
```

In the first PowerShell terminal, start the TC. The explicit `--mqtt-rumqttc` documents the selected backend; omitting it has the same effect.

```powershell
cargo +1.95 run `
  --package trustworthiness_checker `
  --bin trustworthiness_checker `
  --no-default-features `
  -- `
  tests/fixtures/simple_add_typed.dsrv `
  --mqtt-input `
  --mqtt-rumqttc `
  --mqtt-port 1883 `
  --output-stdout
```

After the TC has connected, publish two pairs of JSON integer values from a second PowerShell terminal:

```powershell
docker exec tc-mqtt mosquitto_pub -t x -m 1
docker exec tc-mqtt mosquitto_pub -t y -m 3
docker exec tc-mqtt mosquitto_pub -t x -m 2
docker exec tc-mqtt mosquitto_pub -t y -m 4
```

The TC evaluates each received update as a logical input tick. Its stdout includes a line containing `Int(4)` after receiving `x = 1` and `y = 3`, and a line containing `Int(6)` after receiving `x = 2` and `y = 4`. Updates arrive independently, so stdout may also contain an intermediate result between the two pairs.

Stop the TC with `Ctrl+C`, then remove the broker:

```powershell
docker rm --force tc-mqtt
```

### Redis Pub/Sub input

Redis Pub/Sub input requires the `redis` Cargo feature. Start a local Redis server:

```powershell
docker run --rm --detach `
  --name tc-redis `
  --publish 127.0.0.1:6379:6379 `
  redis:7-alpine
```

In the first PowerShell terminal, build with Redis support and start the TC:

```powershell
cargo +1.95 run `
  --package trustworthiness_checker `
  --bin trustworthiness_checker `
  --no-default-features `
  --features redis `
  -- `
  tests/fixtures/simple_add_typed.dsrv `
  --redis-input `
  --redis-port 6379 `
  --output-stdout
```

After the TC has subscribed, publish the values from a second PowerShell terminal:

```powershell
docker exec tc-redis redis-cli PUBLISH x 1
docker exec tc-redis redis-cli PUBLISH y 3
docker exec tc-redis redis-cli PUBLISH x 2
docker exec tc-redis redis-cli PUBLISH y 4
```

As with MQTT, stdout includes lines containing `Int(4)` and `Int(6)` and may include an intermediate result. Redis Pub/Sub does not retain these messages, so publish only after the TC is running.

Stop the TC with `Ctrl+C`, then remove Redis:

```powershell
docker rm --force tc-redis
```

The current MQTT output backend is Paho-only and requires the Cargo `mqtt` feature, which is enabled by default. When combining MQTT output with `--no-default-features`, add `--features mqtt`. Redis output similarly requires the `redis` feature. Select these destinations with `--mqtt-output` or `--redis-output`, respectively. These output paths have different native dependency and remote-observation boundaries from the Rumqttc and Redis input examples above.
