# Getting started

This page installs the build prerequisites and runs an example Trustworthiness Checker (TC) monitor. The example introduces the basic shape of a DSRV program, reads four input values, prints a running total, and exits. It requires no broker, ROS installation, or optional Cargo feature.

## Linux or Unix

The TC requires Rust 1.95 and a native C/C++ build toolchain. Install Git, `curl`, and the build tools with your platform package manager. For example, on Ubuntu or Debian:

```sh
sudo apt-get update
sudo apt-get install build-essential curl git
```

On Fedora:

```sh
sudo dnf install gcc gcc-c++ curl git
```

### Install Rust with rustup

Download the official rustup installer, inspect it, and install the minimal Rust 1.95 toolchain:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  --output /tmp/rustup-init.sh
less /tmp/rustup-init.sh
sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain 1.95
rm /tmp/rustup-init.sh
. "$HOME/.cargo/env"
rustc --version
cargo --version
```

The download comes from the [official rustup service](https://rustup.rs/) and requires network access. `rustc --version` must report Rust 1.95 or newer. The repository declares Rust 1.95 as its minimum supported Rust version.

### Get the source

Clone the repository and enter its root directory:

```sh
git clone https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker.git
cd robosapiens-trustworthiness-checker
```

If you already have a checkout, run the remaining commands from its repository root.

### Run a running-total example

The first DSRV example has one input, `x`, and one output, `z`:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

`in x` declares the value supplied to the monitor. `out z` declares the result it exposes. The equation adds the current `x` to the previous value of `z`; `default(..., 0)` starts the total at zero when there is no previous value.

The repository stores this program as `examples/counter.dsrv` and supplies four input values in `examples/counter.input`. Run that example from the repository root:

```sh
cargo run --quiet -- examples/counter.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

Cargo builds the TC on the first run. The example evaluates the four inputs and writes this exact result to stdout:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

The process exits after the fourth input. The output index is zero-based, so `z[0]` is the first result. DSRV calls each input position a logical tick; `z[1]` reads `z` from the preceding tick.

## Windows paths

For a native Windows build, install rustup and the MSVC C++ prerequisites, then run the compact PowerShell example in [Extended Windows usage](tutorials/windows.md#native-windows). That page also covers Docker Desktop transports and testing a cross-compiled Windows executable with Wine on Linux.

Under WSL, use the Linux instructions on this page inside the WSL distribution. Install Rust and build tools inside WSL rather than reusing a Windows Rust installation. See [Windows Subsystem for Linux](tutorials/windows.md#windows-subsystem-for-linux-wsl) for filesystem and Docker Desktop considerations.

## Continue from the first run

- [Add two input streams from a trace](tutorials/finite-trace.md) explains timestamps, simultaneous assignments, missing values, and history.
- [Write a DSRV monitor](tutorials/write-dsrv-monitor.md) introduces declarations, equations, `defer`, and `dynamic`.
- [Monitor timed signals with MSTLO](tutorials/mstlo-monitor.md) runs a timestamped Signal Temporal Logic property.
- [Capabilities](features/capabilities.md) maps live inputs, outputs, reconfiguration, distributed monitoring, and integrations to focused pages.
- [General usage](general-usage.md) gives the shortest command shape for each input route.
