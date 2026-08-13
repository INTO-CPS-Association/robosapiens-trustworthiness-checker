# Trustworthiness Checker FMU

The FMU packages a selected typed DSRV checker specification behind an FMI 2.0
Co-Simulation interface. The DSRV specification is authoritative for variable
names, causality, types, and any declaration-local FMI annotations. An optional
`fmi.toml` provides an alternative source of variable metadata and can add
model metadata.

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

The FMU contains a CPython-specific native extension. The current integration
targets CPython 3.12. Install CPython 3.12 with the operating system's package
manager and verify it before continuing:

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

The adapter selects `causal-set` semantics by default. Set
`TC_CAUSAL_SEMANTICS=role-causal-set` or
`TC_CAUSAL_SEMANTICS=role-causal-antichain` to select a role-aware causal mode.
Causal Python outputs use the structured
`{"values": ..., "causality": ...}` contract; ordinary flat outputs remain
unchanged. If `TC_CAUSAL_LOG` is set, the adapter writes one deterministic
JSONL record per causal failure, including FMI time, communication point,
phase, logical tick, output values, and normalized cause occurrences with
optional role lists.
The causal schema is documented in `docs/causal-semisync.md`.

FMI variable metadata can be declared natively in `spec.dsrv`. Place one or
more consecutive `// @fmi key=value ...` lines immediately before the related
`in` or `out` declaration. A compact annotation can put several fields on one
line:

```dsrv
// @fmi start=0.0 unit="m/s" description="Observed velocity" variability="continuous" value-reference=0
in velocity: Float
```

The same metadata can be split across multiple consecutive lines:

```dsrv
// @fmi start=false
// @fmi description="True when the observed behaviour is safe"
// @fmi variability="discrete"
// @fmi value-reference=2
out verdict: Bool
```

The supported fields are:

- `start`
- `unit`
- `description`
- `variability`
- `value-reference`

When `start` is omitted, its default depends on the DSRV type: `Float` is
`0.0`, `Int` is `0`, `Bool` is `false`, and `Str` is `""`.

`fmi.toml` remains optional. It can be used instead of inline annotations for
variable metadata and can provide model metadata such as the model name, GUID,
and description. When inline and TOML variable metadata overlap, identical
values are accepted, complementary fields are merged, and conflicting values
are rejected. Unknown fields, duplicate fields, and `// @fmi` directives that
are not attached to an `in` or `out` declaration are errors.

## Input initialisation

Input `start` values are FMI initialisation defaults; they must not be assumed
to represent behaviour observed from the system under test. After
initialisation, the importing runtime master can override checker inputs with
the type-appropriate `fmi2SetReal`, `fmi2SetInteger`, `fmi2SetBoolean`, or
`fmi2SetString` call before `fmi2DoStep`. Otherwise, the checker evaluates the
configured start values and may produce a verdict for observations the system
never supplied. Once set, an input holds its most recently supplied value
across subsequent steps until the master sets it again.

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
`spec.dsrv`, merges any inline and `fmi.toml` metadata, and generates both
`modelDescription.xml` and `resources/interface.json`. The adapter consumes the
JSON mapping, so its value references and types cannot drift from the generated
FMI description.
UniFMU supplies the FMI binary, protocol schemas, Python entry point, and command
dispatcher unchanged; the build replaces only the generated example model with
the checker adapter and packages its runtime dependencies alongside it.
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
