# DSRV language settings

A DSRV specification can say which language it is written in and which
edition of that language. These settings come first in the file. A file that
states neither is Full DSRV, edition `2026-09`, which is exactly how every
existing specification is read.

```dsrv
language core      // optional: core or distributed
edition 2026-09    // optional
```

`language` and `edition` are keywords everywhere, so they cannot be used as
stream names. The words after them are not reserved: a stream may still be
called `core` or `distributed`.

## Dialects

| Dialect | Declared by | Contents |
|---|---|---|
| Core DSRV | `language core` | The basic temporal language: literals, variables, operators, `if`, `x[n]`, `default`, `when`, `update`, `is_defined`, `latch`, `init`, `dynamic`, `defer`, one-line definitions (`out y: Int = x`), and scalar types (`Int`, `Float`, `Str`, `Bool`, `Unit`). |
| Full DSRV | the default | Core plus maths functions, type aliases, collections, structs, lambdas and calls. |
| Distributed DSRV | `language distributed` | Full plus the distribution primitives `dist` and `monitored_at`, for specifications placed across nodes. |

Core DSRV is the language closest to the published DSRV calculus. A Core file
that uses anything outside Core is rejected at that construct:

```text
invalid language settings: `abs` at Span { start: 39, end: 45 } is not part of Core DSRV
```

Text supplied at runtime through `dynamic` or `defer` in a Core
specification is held to Core as well.

In this release Core's `if` evaluates both branches, like Full DSRV's. Core's
`if` is specified to evaluate only the selected branch, as in the calculus,
and will do so once lazy `if` is implemented; the two differ only when the
branch not taken has no value yet.

`dist` and `monitored_at` belong to Distributed DSRV. A file that uses them
without `language distributed` is rejected at the construct. For

```dsrv
in x: Int
out y: Bool
y = monitored_at(x, a)
```

the checker reports

```text
invalid language settings: `monitored_at` at Span { start: 26, end: 44 } needs `language distributed`
```

Declaring the dialect makes the file valid; running it is a separate
question. Only the distributed runtime evaluates the distribution
primitives, and the others refuse such a specification before it runs (see
[runtime capabilities](runtime-capabilities.md)). A Distributed file that
does not use them runs anywhere Full DSRV runs.

From Rust, `trustworthiness_checker::lang::dsrv::check_core_source` accepts a
Core file and returns a `CoreDsrvSpecification`, a type that only a
successful Core check can produce.

## Editions

An edition fixes the language's default behaviours at a date, so a
specification keeps its meaning when later releases change a default. The
only edition is `2026-09`, the language as of September 2026. Naming an
unknown edition is an error that lists the known ones.

## Command line

`--language core-dsrv` and `--language distributed-dsrv` choose the dialect
for a file without a `language` line, and `--dsrv-edition YYYY-MM` the
edition for a file without an `edition` line. `--language dsrv`, the default,
leaves the choice to the file. When the file and the command line disagree,
the file is rejected:

```text
invalid language settings: the file declares `language distributed` but `language core` was requested
```

## Printing

A specification printed by the checker, for example in its logs, begins
with its settings when they differ from the defaults, so printed text parses
back to the same specification.
