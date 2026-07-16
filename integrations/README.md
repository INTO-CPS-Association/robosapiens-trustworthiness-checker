# Integrations

The integration layers depend on the core in one direction:

```text
Rust core -> Python binding -> FMU adapter and package
```

## Python binding

The PyO3 crate and uv project live in `integrations/python`.

```bash
uv sync --project integrations/python
uv run --project integrations/python --locked pytest integrations/python/tests integrations/fmu/tests
```

## FMU

The authored UniFMU model adapter and launch configuration live in
`integrations/fmu/adapter`, while selectable specification examples live in
`integrations/fmu/examples/<name>`. The build preserves UniFMU's generated entry
point and command dispatcher, type-checks the selected DSRV specification,
generates its FMI description and runtime interface, and assembles the adapter
with the local Python extension under the ignored `integrations/fmu/build`
directory.

```bash
integrations/fmu/scripts/install-unifmu.sh
integrations/fmu/scripts/build.sh --spec-dir integrations/fmu/examples/velocity-safety
integrations/fmu/scripts/validate.sh
integrations/fmu/scripts/test-black-box.sh
```

The distributable FMU is written to
`integrations/fmu/dist/trustworthiness_checker.fmu`.
