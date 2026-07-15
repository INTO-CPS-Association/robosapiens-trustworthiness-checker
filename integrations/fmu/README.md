# Trustworthiness Checker FMU

The FMU packages a selected typed DSRV checker specification behind an FMI 2.0
Co-Simulation interface. The DSRV specification is authoritative for variable
names, causality, and types. An optional `fmi.toml` adds FMI-only metadata such
as units, descriptions, start values, and stable value references.

## Setup

The build and development scripts require:

- A supported 64-bit UniFMU host: Linux x86-64, macOS x86-64, or Windows
  x86-64 under MSYS, MinGW, or Cygwin
  (only Linux is well-tested)
- CPython 3.12, available as `python`
- A current stable Rust toolchain with Cargo
- A native C/C++ build toolchain and linker
- `curl`, Bash, and Git
- Internet access for the initial tool and Python dependency downloads

The FMU contains a CPython-specific native wheel. The current integration targets CPython
3.12. Install CPython 3.12 with the operating system's package manager and
verify it before continuing:

```bash
python --version
```
Install the current stable Rust toolchain with
[rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup default stable
```

Then install `uv` with its standalone installer:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Restart the shell, or add the installer-reported directory to `PATH`, then
create the locked base environment from the repository root:

```bash
uv sync \
    --project integrations/python \
    --python 3.12 \
    --locked
```

Finally, install the pinned UniFMU release into `integrations/fmu/tools/`:

```bash
integrations/fmu/scripts/install-unifmu.sh
```

The installer defaults to UniFMU 0.14.0. The integration currently supports
the UniFMU 0.14.x command-line interface; select another release in that series
with `UNIFMU_VERSION=<version>` if required.

## Specification examples

Each specification example lives under `examples/<name>/`:

```text
examples/velocity-safety/
├── spec.dsrv
└── fmi.toml
```

The provided `velocity-safety` example monitors a system's velocity and
emergency-stop output and produces a Boolean safety verdict.

## Input initialisation

Input `start` values are FMI initialisation defaults; they must not be assumed
to represent behaviour observed from the system under test. The importing
simulator should set every checker input after initialisation and before the
first `fmi2DoStep`. Otherwise, the checker evaluates the configured start values
and may produce a verdict for observations the system never supplied.

## Build

```bash
integrations/fmu/scripts/build.sh \
    --spec-dir integrations/fmu/examples/velocity-safety
```

Build another checked-in or external specification directory with:

```bash
integrations/fmu/scripts/build.sh --spec-dir path/to/specification-directory
```

During the build, `generate_fmu_interface` parses and strictly type-checks
`spec.dsrv`, merges `fmi.toml`, and generates both `modelDescription.xml` and
`resources/interface.json`. The adapter consumes the JSON mapping, so its value
references and types cannot drift from the generated FMI description.
The artefact contains only the current host platform's FMI binary and native
Python extension; build a separate FMU on each target platform.

Only DSRV `Int`, `Float`, `Bool`, and `Str` variables can currently be exposed.
Unsupported complex types fail the build.

The artefact is written to:

```text
integrations/fmu/dist/trustworthiness_checker.fmu
```

## Validate and test

```bash
integrations/fmu/scripts/validate.sh
integrations/fmu/scripts/test-black-box.sh
```

`validate.sh` first validates the FMI metadata and then performs a packaging
smoke simulation using the configured input start values. It checks that the
FMU can load and execute, but does not fully validate checker behaviour.

The black-box suite runs the packaged FMU from temporary directories with
ambient Python paths removed and asserts safety verdicts through the public FMI
interface.

## Benchmark

Run the small end-to-end benchmark against the packaged FMU with:

```bash
integrations/fmu/scripts/benchmark.sh
```

It measures FMU instantiation, IPC, checker execution, and output retrieval. It
reports median wall time, simulation steps per second, and the real-time factor.
The workload can be adjusted, for example:

```bash
integrations/fmu/scripts/benchmark.sh --steps 10000 --repetitions 5
```
