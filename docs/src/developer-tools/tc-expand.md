# Inspect expanded DSRV with `tc-expand`

`tc-expand` shows a DSRV program after expansion. It loads the root model and
its filesystem or embedded modules, resolves imports, and inlines named
functions. The report lists the activated modules, stream declarations, type
aliases, and expanded equations. With `--check-mode`, it also type-checks the
program and shows resolved stream annotations. The tool does not evaluate the
program.

## Example

This example imports the embedded `std::option` module and calls its functions:

```dsrv
{{#include ../../../examples/std_option.dsrv}}
```

From the repository root, run:

```console
cargo run --bin tc-expand -- --check-mode strict examples/std_option.dsrv
```

The report begins:

```text
# tc-expand inspection (checked (strict)); non-standalone source report
# language: Full DSRV, edition 2026-09, experiments tagged_unions, generics, modules
# activated modules:
#   std::option [embedded catalogue]

in reading: Union<None, Some: Int>
out present: Bool
present = (\option -> matches(option, Some(_))(reading): Bool)
```

The command exits 0 on success. Omit `--check-mode strict` for an expanded,
unchecked view, or use `--check-mode gradual` for gradual checking. Errors and
warnings go to stderr; a model or checking failure exits 1 without a report on
stdout.
