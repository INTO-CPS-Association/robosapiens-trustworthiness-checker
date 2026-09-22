# Inspect expanded DSRV

`tc-expand` gives developers one global, post-expansion view of a DSRV model.
Use it to inspect declarations and equations after filesystem and embedded
modules have been activated and functions have been inlined. It is a finite
inspection command, not a monitor.

## Run the inspection

From the repository root, with the Rust toolchain from `Cargo.toml` installed,
run:

```console
cargo run --bin tc-expand -- MODEL
```

`MODEL` is the root DSRV file. Relative module paths are resolved from that
file. With no check mode, the heading marks the report `expanded, unchecked`.
To type-check before printing, select exactly one mode:

```console
cargo run --bin tc-expand -- --check-mode strict MODEL
cargo run --bin tc-expand -- --check-mode gradual MODEL
```

On success, stdout contains one complete text report and the process exits 0.
The report starts with its inspection/check state and language, lists activated
modules and whether each came from the filesystem or embedded catalogue, then
prints expanded inputs, outputs, auxiliaries, equations, and aliases in
declaration order. A checked report includes resolved stream annotations.
Warnings and errors go to stderr. A model load or type-check failure exits 1
and leaves stdout empty. An output I/O failure also exits 1; only that failure
can leave a partial stdout report. Invalid command-line arguments exit 2.

For example, inspect the checked-in [`std::option`
example](#embedded-stdoption):

```console
cargo run --bin tc-expand -- --check-mode strict examples/std_option.dsrv
```

Its report begins as follows (expanded expressions later in the report may be
longer):

```text
# tc-expand inspection (checked (strict)); non-standalone source report
# language: Full DSRV, edition 2026-09, experiments tagged_unions, generics, modules
# activated modules:
#   std::option [embedded catalogue]

in reading: Union<None, Some: Int>
out present: Bool
...
```

The command constructs the whole report before writing it, but the text is for
human inspection. It resembles DSRV without being standalone source. No stable
format, parse/print round trip, or execution guarantee is provided. The report
does not show runtime capability admission, deployment configuration,
localisation, or a monitor run. Inspect those separately with the selected
runtime and deployment. `tc-expand` is a repository developer tool and is not
copied into the deployment image.

## Embedded `std::option`

The embedded module defines this structural API:

```dsrv
type Option<T> = Union<Some: T, None>
def is_some<T>(option: Option<T>) -> Bool
def is_none<T>(option: Option<T>) -> Bool
def unwrap_or<T>(option: Option<T>, fallback: T) -> T
```

`unwrap_or` is eager: its fallback argument is evaluated as an ordinary
function argument even when the option is `Some`.

Activate the module with an ordinary `use std::option`. Do not declare it with
`mod`; embedded modules are loaded from the checker catalogue. There is no
prelude, so no option names exist until a `use` reaches this module.

The complete checked-in example is:

```dsrv
{{#include ../../../examples/std_option.dsrv}}
```

With the module import shown there, the current qualified spellings are
`option::Option<T>` for the type and `std::option::is_some`,
`std::option::is_none`, and `std::option::unwrap_or` for functions. A glob
import (`use std::option::*`) also permits the unqualified `Option<T>`,
`is_some`, `is_none`, and `unwrap_or` spellings.

`Some(value)` and `None` are contextual constructors: an annotation or another
surrounding expected type must identify their union. The caller enables the
experiments needed by syntax it writes. The example needs `modules` and
`generics` for the import and `Option<Int>`, and `tagged_unions` because it
writes `Some` and `None`. The embedded module's own implementation experiments
do not need to be repeated merely because its functions are called.

Runtime capability admission remains a separate step after parsing and type
checking. A successful `tc-expand --check-mode ...` report does not promise
that a chosen runtime accepts constructs in the expanded program, such as
tagged unions or pattern matching. See [runtime
capabilities](runtime-capabilities.md) before deployment.

### Current ergonomic limits

Treat these as future work rather than available API:

- generic constructors cannot currently be imported or qualified;
- `unwrap_or(None, 7)` cannot infer which `Option<T>` owns the unanchored
  `None`; bind or annotate an `Option<Int>` first;
- type and function qualification is asymmetric
  (`option::Option<T>` versus `std::option::is_some(...)`);
- the module does not provide `unwrap`, `or`, `map`, or `and_then`.
