# DSRV language settings

A DSRV specification can say which language it is written in, which edition
of that language, and which experimental features it uses. These settings
come first in the file. A file that states none of them is Full DSRV, edition
`2026-09`, with no experiments, which is exactly how every existing
specification is read.

```dsrv
language core                       // optional: core or distributed
edition 2026-09                     // optional
use experimental::{tagged_unions}   // optional
use experimental::*                 // every current experiment
```

`language`, `edition` and `use` are keywords everywhere, so they cannot be
used as stream names. The words after them are not reserved: a stream may
still be called `core` or `experimental`.

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

## Experiments

`use experimental::{…}` opts into features that are still being designed.
They may change or disappear between releases. When the checker runs a
specification that uses any, it logs a warning naming them. This notice is
a log message, shown only when logging is enabled at warning level or above,
for example with `RUST_LOG=warn`; semantic warnings about the specification
itself are always written to standard error.
`use experimental::*` enables every current experiment. Core DSRV accepts no
experiments.

| Feature | Contents |
|---|---|
| `tagged_unions` | `Union<…>` types and their constructors |
| `pattern_matching` | `match`, `matches`, and the patterns both take |
| `generics` | type aliases that take type parameters, and the uses that supply them |

Several `use experimental` lines add up. Any name that is not a current
experiment is an error listing the ones there are, and `use` of any other
namespace is rejected until modules exist.

Two specifications whose settings differ, experiments included, are different
programs: the source fingerprint that reconfiguration caches are keyed on
carries the settings, and carries the release's experiment revision whenever
any experiment is on, so work compiled under one meaning of an experiment is
never reused under another.

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
