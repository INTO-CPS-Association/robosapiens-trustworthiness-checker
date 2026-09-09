# RoboSAPIENS Trustworthiness Checker

The Trustworthiness Checker (TC) evaluates streaming observations against an executable monitor specification. It can replay a finite trace, remain attached to live MQTT/Redis/ROS sources, publish results to stdout or transports, and run selected distributed or reconfigurable runtimes.

## Run an example monitor

Prerequisites: Rust 1.95 with Cargo and a native C/C++ build toolchain. Run commands from the repository root.

The first DSRV example adds each input to its previous result:

```dsrv
in x: Int
out z: Int
z = default(z[1], 0) + x
```

The repository stores this running-total program as `examples/counter.dsrv` and supplies four input values in `examples/counter.input`. Run it from the repository root:

```console
cargo run -- examples/counter.dsrv --input-file examples/counter.input --output-stdout
```

The command exits after the four input ticks. Its exact stdout is:

```text
z[0] = Int(1)
z[1] = Int(2)
z[2] = Int(3)
z[3] = Int(4)
```

The output index is zero-based. `z[1]` reads the preceding logical tick and `default(z[1], 0)` supplies the initial value at tick zero. The example needs no broker, ROS installation, or extra Cargo feature.

The command is the same in a Unix shell and PowerShell. The [Getting started guide](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/getting-started.html) covers Linux and Unix prerequisites. Native Windows, WSL, and Wine setup are in [Extended Windows usage](https://into-cps-association.github.io/robosapiens-trustworthiness-checker/tutorials/windows.html).

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
