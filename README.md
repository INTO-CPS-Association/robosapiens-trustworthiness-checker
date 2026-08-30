# RoboSAPIENS Trustworthiness Checker

The Trustworthiness Checker (TC) evaluates streaming observations against an executable monitor specification. It can replay a finite trace, remain attached to live MQTT/Redis/ROS sources, publish results to stdout or transports, and run selected distributed or reconfigurable runtimes.

## Run an example monitor on Linux or Unix

Prerequisites: Rust 1.95 with Cargo and a native C/C++ build toolchain.

The first DSRV example adds each input to its previous result:

```dsrv
in x
out z
z = default(z[1], 0) + x
```

The repository stores this running-total program as `examples/counter.dsrv` and supplies four input values in `examples/counter.input`. Run it from the repository root:

```sh
cargo run -- examples/counter.dsrv \
  --input-file examples/counter.input \
  --output-stdout
```

The command exits after the four input ticks. Its exact stdout is:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

The output index is zero-based. `z[1]` reads the preceding logical tick and `default(z[1], 0)` supplies the initial value at tick zero. The example needs no broker, ROS installation, or extra Cargo feature.

## Run on Windows

Native Windows builds use Rust 1.95 and the MSVC toolchain. The Windows example adds two explicitly typed integer inputs:

```dsrv
in x : Int
in y : Int
out z : Int
z = x + y
```

The repository stores it as `tests/fixtures/simple_add_typed.dsrv` with three input pairs in `tests/fixtures/simple_add_typed.input`. From PowerShell in the repository root, run it without optional transport features:

```powershell
cargo +1.95 run `
  --package trustworthiness_checker `
  --bin trustworthiness_checker `
  --no-default-features `
  -- `
  tests/fixtures/simple_add_typed.dsrv `
  --input-file tests/fixtures/simple_add_typed.input `
  --output-stdout
```

The [extended Windows usage](docs/src/tutorials/windows.md) page covers prerequisites, WSL, exact output, native testing, Docker Desktop MQTT/Redis input, and testing the cross-compiled Windows GNU executable with Wine on Linux.

## Documentation

See the repository's [Documentation](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/) including:
- [Getting started](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/getting-started.html) — install Rust with rustup and complete the first Linux/Unix run.
- [Capabilities](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/features/capabilities.html) — see what the checker can monitor and where results can go.
- [Languages and runtimes](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/features/languages-and-runtimes.html) — compare DSRV, MSTLO, finite, live, and reconfigurable runs; start with the [MSTLO tutorial](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/tutorials/mstlo-monitor.html) for timestamped STL monitoring.
- [Inputs](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/features/inputs.html) and [outputs](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/features/outputs.html) — select transports, route catalogs, windows, and destinations.
- [Integrations and deployment](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/features/integrations-and-deployment.html) — use the Python binding, FMU, or deploy image.
- [Extended Windows usage](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/tutorials/windows.html) — use native Windows, WSL, Docker Desktop, or Linux/Wine cross-testing.
- [CLI reference](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/reference/cli.html) — generated option spelling, defaults, values, and Clap conflicts.

Also see the [Benchmark dashboard](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/benchmarks/) to track performance regressions.

The repository also contains standalone integration material in [`integrations/python/README.md`](integrations/python/README.md) and [`integrations/fmu/README.md`](integrations/fmu/README.md).

## License

This project is licensed under the INTO-CPS Association Public License (ICAPL). The selected usage mode is documented in `ICA-USAGE-MODE.txt`. See `LICENSE.md`.
