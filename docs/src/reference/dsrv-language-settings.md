# DSRV dialects, editions, and experiments

A DSRV specification can select its edition, dialect, and experimental
features. The checker resolves those declarations into one set of language
settings before it expands or checks the specification. A file that states
none of them is Full DSRV, edition `2026-09`, with no experiments, preserving
the interpretation of existing specifications.

```dsrv
language core                       // optional: core or distributed
edition 2026-09                     // optional
use experimental::{tagged_unions}   // optional
use experimental::high_level_dsrv   // prototype extended language
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

Without the `lazy_if` experiment, Core and Full `if` expressions evaluate both
branches. The experiment changes this behavior as described under
[Experiments](#experiments).

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

`use experimental::{…}` opts into implemented features that are still being
designed. They may change or disappear between releases. Several such lines
add up. Core DSRV accepts no experiments.

The table is the complete implemented experiment registry. In the last column,
`all` means every DSRV runtime; it does not include MSTLO. An experiment may
still use a construct that has a narrower runtime requirement.

<!-- dsrv-experiments:start -->
| Experiment | User-visible behavior | Material dependencies and interactions | Supported runtimes |
|---|---|---|---|
| `tagged_unions` | Adds `Union<…>` types and union constructors. | Constructors are used by `pattern_matching`; embedded libraries can expose union values under their own settings. | all |
| `pattern_matching` | Adds `match`, `matches`, and their patterns. | Commonly used with `tagged_unions`; a selected `match` arm is already lazy and is distinct from `lazy_if`. | all except `distributed` |
| `generics` | Adds parameters to type aliases and arguments to alias uses. | Used with `modules` by generic library types such as `std::option::Option<T>`. | all |
| `modules` | Adds filesystem and embedded-module declarations and imports. | Each module's own header governs syntax written in that module; imports do not transfer the caller's experiments into it. | all |
| `functions` | Adds named `def` functions. | Functions imported through `modules` are expanded under the defining module's language settings. | all |
| `constants` | Adds `const` declarations and named stream offsets. | A constant declared inside a module also requires `modules`. | all |
| `casts` | Adds `as`, `trunc`, `floor`, `ceil`, and `round`. | Float-to-integer conversion uses an explicit rounding function; see [DSRV syntax](dsrv-syntax.md#operators). | all |
| `lazy_if` | Makes `if` evaluate only its selected branch. | Changes behavior; each branch has a separate local timeline. A `match` arm is independently selected-only. | `dataflow`, `reconf-dataflow` |
<!-- dsrv-experiments:end -->

`use experimental::high_level_dsrv` selects a prototype for an extended
Trustworthiness Checker language. It currently includes every experiment in
the table. `use experimental::*` currently selects the same set.

## Command line

`--language dsrv`, the CLI default, makes no dialect request: a source header
selects Core or Distributed, and a file without one resolves to Full.
`--language core-dsrv` and `--language distributed-dsrv` request a dialect for
a file without a `language` line. `--dsrv-edition YYYY-MM` similarly requests
an edition for a file without an `edition` line; without either a header or
that option, the edition is `2026-09`.

A matching source declaration and CLI request are accepted. A conflicting
declaration is rejected rather than overridden:

```text
invalid language settings: the file declares `language distributed` but `language core` was requested
```

CLI requests apply to the root specification and filesystem modules; each may
use a matching header or inherit the request when its header is absent.
Embedded libraries are different: their explicit headers remain their own.
There is no CLI option that enables experiments; they are selected in source.
