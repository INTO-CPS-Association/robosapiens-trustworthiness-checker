# DSRV language settings

A DSRV specification can select its edition, dialect, and experimental
features. The checker resolves those declarations into one set of language
settings before it expands or checks the specification. A file that states
none of them is Full DSRV, edition `2026-09`, with no experiments, preserving
the interpretation of existing specifications.

```dsrv
language core                       // optional: core or distributed
edition 2026-09                     // optional
use experimental::{tagged_unions}   // optional
use experimental::high_level_dsrv   // every implemented Full preview
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

The table is the complete implemented experiment registry. “Capability
admission” means that, after the language checks succeed, the selected runtime
must separately support constructs introduced by that experiment.

<!-- dsrv-experiments:start -->
| Experiment | User-visible behavior | Material dependencies and interactions | Capability admission |
|---|---|---|---|
| `tagged_unions` | Adds `Union<…>` types and union constructors. | Constructors are used by `pattern_matching`; embedded libraries can expose union values under their own settings. | Yes — tagged unions |
| `pattern_matching` | Adds `match`, `matches`, and their patterns. | Commonly used with `tagged_unions`; a selected `match` arm is already lazy and is distinct from `lazy_if`. | Yes — pattern matching |
| `generics` | Adds parameters to type aliases and arguments to alias uses. | Used with `modules` by generic library types such as `std::option::Option<T>`. | No |
| `modules` | Adds filesystem and embedded-module declarations and imports. | Each module's own header governs syntax written in that module; imports do not transfer the caller's experiments into it. | No |
| `functions` | Adds named `def` functions. | Functions imported through `modules` are expanded under the defining module's language settings. | No |
| `constants` | Adds `const` declarations and named stream offsets. | A constant declared inside a module also requires `modules`. | No |
| `casts` | Adds `as`, `trunc`, `floor`, `ceil`, and `round`. | Float-to-integer conversion uses an explicit rounding function; see [DSRV syntax](dsrv-syntax.md#operators). | No |
| `lazy_if` | Makes `if` evaluate only its selected branch. | Changes behavior; each branch has a separate local timeline. A `match` arm is independently selected-only. | Yes — lazy if |
<!-- dsrv-experiments:end -->

`use experimental::high_level_dsrv` is an umbrella for **all experiments in
that table**, including the behavior-changing `lazy_if`. `use experimental::*`
currently resolves to the same set. Both forms are resolved to explicit
experiment settings, so adding a future experiment can change what a source
file using either broad form means. Neither form selects Distributed DSRV.
They also do not bypass runtime capability admission, the selected semantics'
constraint profile, or distributed localisation admission.

Under `lazy_if`, only the selected branch executes on an outer tick. Each
branch's local timeline advances only on ticks that select that branch:
temporal state in the other branch does not advance or commit, and its missing
values and errors are not observed. Initially, only the `dataflow` runtime
admits this construct; other runtimes refuse it before building streams. See
[runtime capabilities](runtime-capabilities.md) for the admission boundary and
the runtime matrix for stable capabilities.

When the checker runs a specification with experiments, it logs a warning
naming the resolved explicit experiments. This notice is visible when logging
is enabled at warning level or above, for example with `RUST_LOG=warn`;
semantic warnings about the specification itself are always written to
standard error.

Any experimental name that is neither implemented nor an umbrella is an error
listing the current names. Without `modules`, `use` of another namespace is
rejected.

An embedded library is checked under its own explicit experiment header. A
caller enables experiments for syntax the caller writes; it need not copy the
library's implementation experiments merely to call an imported function.
Conversely, `high_level_dsrv` in the caller does not rewrite the embedded
library's settings.

Two specifications whose settings differ, experiments included, are different
programs: the source fingerprint that reconfiguration caches are keyed on
carries the settings, and carries the release's experiment revision whenever
any experiment is on, so work compiled under one meaning of an experiment is
never reused under another.

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

## Printing

A specification printed by the checker, for example in its logs, begins
with its settings when they differ from the defaults, so printed text parses
back to the same specification.
