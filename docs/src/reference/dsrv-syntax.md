# DSRV syntax

This page records DSRV expression syntax and its boundary behavior. Start with
the [DSRV tutorial](../tutorials/write-dsrv-monitor.md) if you are writing your
first monitor. For `language` and `use experimental` declarations,
see [DSRV dialects and experiments](dsrv-language-settings.md).

## Naming conventions

Functions, streams, constants and modules use `snake_case`.
Types and union variants use `PascalCase`. External payload fields retain the
spellings defined by their contract.

Stream function calls with fixed arguments write `::<...>` after the function name,
for example `rpio::journal::knowledge::last_access::<Role::Plan, Access::Read, masks>(observation)`.
Calls without fixed arguments use ordinary parentheses, such as
`rpio::maplek::loop(observation)`. Def declarations and type arguments retain their `<...>` spelling.

## Declarations and equations

A specification is a sequence of declarations. `in x`, `out y` and `aux a`
declare an input, an output and an auxiliary stream, optionally with a type
(`out y: Int`); `var` is an older spelling of `aux`. An *equation* `y = e`
defines an output or auxiliary stream. Inputs have no equation, and a stream
has at most one: a second equation for the same stream is an error.

An output or auxiliary stream can be declared and defined on one line:

```dsrv
in x: Int
out y: Int = x + 1
aux a = default(y[1], 0)
```

This is the same specification as declaring each stream and writing its
equation separately. The checker prints specifications in that separate
form.

### Local auxiliaries on an equation

Under `experimental::local_streams`, an `aux` or `out` equation can keep
helpers private in a `where` block. The `high_level_dsrv` bundle includes
this experiment. For example, this running total keeps its feedback helper
inside the output equation:

```dsrv
use experimental::high_level_dsrv
in x: Int
out total: Int = previous + x
where {
  previous: Int = default(total[1], 0)
}
```

Inputs `2`, `-1`, `3` produce totals `2`, `1`, `4`. The helper advances on
every enclosing tick, including when a conditional result does not select it.
An absent input keeps ordinary input/history behaviour. References to `total`
reuse this equation's state.

The block may follow a combined declaration or a split equation, on the same
line or the next. Write locals as `name = expression`, `name: Type = expression`,
or a separate `name: Type` declaration followed by its equation. No `aux`
keyword is needed inside `where`; the explicit spelling is also accepted.
An empty block is valid. Local equations may have
nested `where` blocks. Locals can refer to enclosing streams, constants and
functions, other locals declared later, and the owner's history. They shadow
outer names and are invisible to sibling equations. Duplicate locals and a
local with its owner's name are refused. Local `in`, `out` and `def`
declarations are refused too.

A local stream may call a [stream function](#local-stream-functions) as its whole
right-hand side. Calls inside larger expressions or lazy arms remain refused.
A plain equation block adds no stream-function runtime requirement; actual
stream-function instances retain their runtime restrictions. The block belongs
to the whole equation, rather than to an arbitrary expression or branch.

## The dynamic type `Any`

A stream without a type annotation is checked as `Any` under the default
`gradual-typed-untimed` semantics, and `Any` can also be written. A value of
type `Any` is checked when it is used, not in advance: reading a field or a
map key, indexing a list or calling it as a function is accepted, and gives
`Any` again; `List.len` gives `Int` and `Map.has_key` gives `Bool`. A value
whose type is known is still checked in advance, so `List.len` of an `Int`
is a type error.

## Type aliases

`type State = Struct<speed: Int, stopped: Bool>` declares a structural alias.
Aliases can name any supported type, refer to later declarations, and nest in
collections and structs. Duplicate names, unknown names, and recursive alias
cycles are rejected. Aliases do not create nominally distinct types: two aliases
that expand to the same structure are interchangeable. A
[newtype](#newtypes) is the declaration that does.

An alias belongs to the module that declares it, in a namespace separate from
stream and local names. Forward references resolve within that module; imports
make selected aliases available to another module. A function imported from a
module keeps the aliases and language settings of the module that wrote its
body. An argument passed to that function keeps the caller's context. Text
supplied at run time to a `dynamic` or `defer` node is not the module that
wrote the node: it sees that module's aliases, constants, and defs as an
importer would, so anything the module declares `internal` is not available to
it, whereas the module's own source text still uses its internal items. See
[language settings](dsrv-language-settings.md#experiments) for the `modules`
and `functions` experiments. A newtype is imported like an alias, and the
same `internal` rule hides it, but only the module that declares it may build
or read its representation.

Struct field names in types can be quoted, as in
`Struct<"quoted field": Int>`. Printed specifications put alias declarations
before stream declarations and print other types in expanded structural form,
so the result parses back to the same specification.

### Generic aliases

Under `use experimental::{generics}`, an alias can take type parameters:
`type Boxed<A> = Struct<value: A, count: Int>`. A use supplies one argument
per parameter, as in `in x: Boxed<Str>`, and an argument can be any type,
including another application: `Boxed<Boxed<Int>>`.

A generic alias has no type of its own, so it is resolved once per use rather
than once for the file: an alias that is declared and never used need not
resolve at all. Supplying the wrong number of arguments is an error, as is
naming a generic alias with none or applying arguments to one that takes none.
Arguments are resolved before they are substituted, so an argument that names
the alias being applied is not a cycle; an alias whose own body reaches itself
still is.

Parameters are positional and scoped to the alias that declares them. They are
capitalised like any other type name, and a parameter shadows a declared alias
of the same name within that body.

### Embedded `std::option`

The embedded `std::option` module provides the generic alias
`Option<T> = Union<Some: T, None>` and the functions `is_some`, `is_none`, and
`unwrap_or`. It has no prelude: `use std::option` activates the module, and
`use std::option::*` also brings its names into unqualified scope. With a plain
module import, the current spellings are `option::Option<T>` for the type and
`std::option::is_some`, `std::option::is_none`, and
`std::option::unwrap_or` for functions.

A caller enables `modules` and `generics` to import and use `Option<T>`, plus
`tagged_unions` if it writes `Some` or `None`. Constructors need an expected
`Option<T>` type; an unanchored `unwrap_or(None, 7)` cannot infer one.
`unwrap_or` evaluates its fallback argument even for `Some`. The module does
not currently provide `unwrap`, `or`, `map`, or `and_then`. The
[inspection example](../developer-tools/tc-expand.md#example) shows a complete
model using the module.

### Embedded `std::time` and `std::timing`

Both modules are generic in a time domain `D`, a nullary newtype that the
specification declares, such as `newtype JournalTime = Unit`. An instant or a
duration of one domain is a type error where another domain is expected.

`std::time` holds the time values, their arithmetic and comparisons:

- `Instant<D>` and `Duration<D>` are newtypes over `Float` seconds; a
  duration is signed. `instant(seconds)` and `seconds(amount)` construct
  them, so a budget is written
  `const budget: Duration<JournalTime> = std::time::seconds(0.2)`.
- `TimeInterval<D>` has `start` and `end` instants. `between(start, end)`
  always builds one, even when it runs backwards, which `is_backwards`
  reports. `duration`, `elapsed`, `latest` and `since` work on these values.
- `exceeds(span, limit)` and `shorter_than(span, limit)` compare an
  `Option<TimeInterval<D>>` with a `Duration<D>`, strictly. Both hold only on
  a forward interval, so neither holds for a backwards or an absent one.
- `TimedValue<T, D>` and `ClockSample<D>` pair a value or a clock reading with
  its instant.

### Duration literals

`use experimental::duration_literals` enables unit suffixes; the
`high_level_dsrv` bundle includes them from experimental revision 8. Each
literal has type `std::time::Duration<D>`. The spec must ground `D` through a
declared result type, a constant annotation or another argument of the same
generic call:

```dsrv
use experimental::high_level_dsrv
use std::time::{Duration, TimeInterval}
use std::option::Option
newtype JournalTime = Unit
const budget: Duration<JournalTime> = 100 ms
in reassessment_interval: Option<TimeInterval<JournalTime>>
out reassessment_late: Bool = std::time::exceeds(reassessment_interval, 0.1 s)
```

| Suffix | Seconds |
|---|---:|
| `ns` | 0.000000001 |
| `us`, `µs`, `μs` | 0.000001 |
| `ms` | 0.001 |
| `s` | 1 |
| `min` | 60 |
| `h` | 3600 |
| `d` | 86400 |

A day is a fixed duration of 86400 seconds. Decimal and scientific notation
are accepted, with optional spaces or tabs between the number and suffix:
`100ms`, `0.1 s` and `1e-1 s` denote the same value. Newlines and comments
cannot separate the number and suffix. The suffixes remain available as
ordinary stream names. Negative literals, such as `-100 ms`, retain the
existing signed-duration meaning.

Units normalise in decimal before a single conversion to the existing Float
seconds representation. Thus equivalent decimal spellings share the exact
same threshold, including `0.03 ms` and `0.00003 s`. Float precision and
underflow still apply; a non-finite result is rejected. Duration arithmetic
continues to use library functions; literals add no implicit newtype
arithmetic or clock conversions.

Literals load `std::time` automatically and lower through its public
`seconds` factory. Printed literal expressions use canonical seconds, such as `0.1 s`, which
reparse under the same imports and expected type. Expanded whole-spec printing
retains the existing limitations of imported types and folded newtype
constants: a folded duration constant may print private `Duration::new(...)`
syntax, so expanded output is an inspection form rather than portable source.
Runtime expression text can use literals when the site's own language settings
enable them and its existing imports make `std::time` available; runtime text
cannot load a new module. An explicit `use std::time` at the site supplies it.

Contextual generic inference also applies without `duration_literals`:
`std::time::exceeds(reassessment_interval, std::time::seconds(0.1))` infers the
duration's domain from the interval. Grounded sibling arguments supply context
in either argument order, including through nested generic calls. Either
operand of a comparison can also supply the other operand's type context. Missing
clock context and mixed clock domains remain errors.

`std::time::exceeds_seconds(reassessment_interval, 100 ms)` is a type error:
that helper takes Float seconds. Use `std::time::exceeds` for typed literals,
or pass a raw Float to `exceeds_seconds`.

### Timing property patterns

`std::timing` holds timing property patterns: functions that keep state
across ticks.

- `while(condition, now, event)` measures how long a condition has held.
- `bounded_response(triggered, responded, withdrawn, now, event, limit)` checks
  the bounded-response pattern □(p → ◇[0,T] q) for each occurrence, and its
  "response, unless withdrawn" variant.
- `deadline(open, close, cancel, budget, clock)` is the same pattern as one
  verdict per tick: `overdue` reports a late closing response as well as a
  silence breach (each wait once), `completed` is the interval a response
  closed, and `pending` counts the open waits. `clock` is a
  `Clock<D> = Struct<now: Option<Instant<D>>, at: Option<Instant<D>>>`: waits
  open and close at `at`, the instant of this tick's record, and age with
  `now`. A `None` budget keeps the closed interval in `completed` for a later
  verdict.

#### Bounded response

`bounded_response<K, D>` takes keys of any type `K` that supports `==`. A key in
`triggered` creates an **obligation**, a pending occurrence that a key in
`responded` must discharge within `limit`. A key in `withdrawn` closes the
obligation without judgement. An obligation is created at `event`, the instant
of the record that triggers it, or at `now` when the tick has no record
instant. A response or a withdrawal closes it at its own record's instant in
the same way. Distinct keys are distinct obligations, judged independently,
so overlapping occurrences need distinct keys, such as the identity of the
event that triggers each one.

Each tick returns a `BoundedResponseTiming<K, D>`:

| Field | Meaning |
|---|---|
| `pending: List<Obligation<K, D>>` | The obligations open after this tick, oldest first. Each has its `key`, the instant it was created (`since`), and whether it has been `reported`. |
| `overdue: List<K>` | The obligations that breached `limit` for the first time on this tick. |
| `closed: Option<Closure<K, D>>` | The obligation this tick closed: its `key`, its `span` from creation to closing, and `by: Responded` or `by: Withdrawn`. |
| `conflicting: Bool` | A key was triggered while an obligation with that key was pending. The original obligation and its instant are kept. |
| `unmatched: Bool` | A key was responded to or withdrawn while no obligation with that key was pending. |

An obligation is reported in `overdue` exactly once: on the first tick where
its age, from creation to `now`, strictly exceeds `limit`, or where a response
closes it after a span that strictly exceeds `limit`, whichever comes first. A
tick that carries only a later `now`, such as a clock sample, can therefore
report a silent obligation, and a response arriving later does not report it
again. A withdrawn obligation is never reported, and an age or a span that
runs backwards is never a breach.

Within one tick, a response is applied first, then a withdrawal, then a
trigger. A tick that responds to a pending key and triggers the same key
closes the old obligation and opens a fresh one. When a response and a
withdrawal close two obligations on one tick, `closed` reports the response.
A tick with neither `event` nor `now` leaves the obligations as they were and
reports nothing.

In this illustrative fragment, `request`, `answer` and `cancelled` are
`Option` streams naming the emission that raised a request, and `now` and
`event` come from `rpio::journal::timing::now(observation)` and
`rpio::journal::timing::phase_time(observation)`:

```dsrv
use std::time::{Duration}
use std::timing
const answer_budget: Duration<JournalTime> = std::time::seconds(0.2)
aux answering = std::timing::bounded_response(request, answer, cancelled, now, event, answer_budget)
out answer_late: Bool = List.len(answering.overdue) > 0
```

`answer_late` holds on each tick where some request first breaches the
budget.

## Tagged unions and `match`

Both are experimental, so a file that uses them declares them (see
[language settings](dsrv-language-settings.md)):

```dsrv
use experimental::{tagged_unions, pattern_matching}

type State = Union<Stopped, Moving: Int>
```

A union names its alternatives. An alternative either carries a payload,
written after its tag, or carries none. Alternatives are a set, so
`Union<Stopped, Moving: Int>` and `Union<Moving: Int, Stopped>` are the same
type, and a repeated tag is rejected.

**Tags are capitalised, and names that bind are not.** In a file that has
taken on `tagged_unions`, a capitalised name in an expression is a tag, so
that file cannot also name a stream or a lambda parameter with a capital,
and says so where the name is declared.

**Constructors** are written as the tag, with the payload in parentheses when
the alternative carries one:

```dsrv
aux state: State
state = Moving(speed)
```

Which union a tag belongs to comes from the type the expression is expected
to have, so libraries can share tag names. Where nothing says which union is
meant — an operand of `==`, or a `match` scrutinee — the union is named:
`State::Moving(speed)`. A tag that cannot be resolved is reported, and the
message names the unions in scope that do have it.

**`match`** decides between arms, and only the selected arm is evaluated:

```dsrv
out speed: Int
speed = match(state) {
  Moving(n) if n > limit -> limit,
  Moving(n) -> n,
  Stopped -> 0,
}
```

A pattern is a tag with an optional payload pattern, a tuple `(a, b)`, a list
`[a, b]`, a struct `{ field: p, .. }`, an `Int`, `Str`, `Bool` or `Unit`
literal, an `Int` range (`1..5`, `1..=5`), alternatives joined by `|`, a
lower-case name that binds what it matched, `name @ pattern`, or `_`. A
`Float` is not a pattern: comparing one with an operator says what was meant.
Or-alternatives bind the same names, and a name a pattern binds is in scope
for that arm's guard and body only.

Arms must leave no value unmatched: a union names every alternative, and
anything else ends with a pattern that matches whatever it is given. An arm
with a guard covers nothing, because its guard may refuse the value its
pattern matched.

**`matches(e, p)`**, with an optional guard, reports whether one pattern
selects:

```dsrv
out moving: Bool
moving = matches(state, Moving(n) if n > 0)
```

A scrutinee that has no value gives the `match` no value either, rather than
falling through to a later arm, and so does a guard without one.

An arm runs only on the ticks that select it, but a stream's previous value
read in it, such as `default(x[1], 0)`, is still that stream's (see
[history access](#history-and-collection-access)).

## Newtypes

Under `use experimental::{newtypes}`, `newtype` declares a type that is
distinct from every other type, including the type it is written over:

```dsrv
use experimental::{newtypes}

newtype Meters = Float
newtype Feet = Float

in x: Float
out m: Meters = Meters::new(x)
out back: Float = Meters::value(m)
```

`Meters` and `Feet` hold the same representation, a `Float`, but a `Feet` is
not accepted where a `Meters` is expected, a `Meters` is not a `Float`, and
neither is compared with, added to or assigned from the other. Each mismatch
is a type error when the specification is checked.

`Name::new(x)` makes a value of the newtype from its representation, and
`Name::value(x)` reads the representation back. Neither validates anything
beyond the type, so **only the module that declares the newtype may write
them**. Another module, even one that imports the newtype or names it through
an alias, calls the functions the declaring module exports instead, and those
functions decide what makes a valid value. Text supplied at run time to
`dynamic` or `defer` never gets `new` or `value`, even at a node the declaring
module wrote; it too calls the exported functions.

A newtype may take parameters, as a generic alias does, and a parameter need
not appear in the representation:

```dsrv
use experimental::{newtypes, generics, functions}

newtype JournalTime = Unit
newtype Producer = Unit
newtype Instant<D> = Float

def at<D>(x: Float) -> Instant<D> = Instant::new(x)
def later<D>(a: Instant<D>, b: Instant<D>) -> Bool =
  Instant::value(b) > Instant::value(a)

in x: Float
aux start: Instant<JournalTime> = at(0.0)
aux now: Instant<JournalTime> = at(x)
out moved: Bool = later(start, now)
```

`Instant<JournalTime>` and `Instant<Producer>` are different types, so a call that
mixes them is a type error. A parameter is inferred from the arguments or from
the type the expression is expected to have; where nothing says which it is,
as in `later(at(x), at(x))`, checking asks for an annotation. Parameters that
distinguish domains in this way are conventionally newtypes over `Unit`, so
two domains never meet through a structural alias.

This complete fixture checks and evaluates the example above, and shows two
specifications that checking refuses:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_newtypes}}
```

A newtype's identity is its *brand*: the path of the module that declared it,
its name, and its arguments, as in `time::Instant<app::JournalTime>`. An
argument is spelled as the structure its aliases resolve to, and an argument
that is itself a newtype as that newtype's brand, as in
`time::Duration<time::Instant<app::JournalTime>>`. A newtype
declared in the root module has no module prefix. However a newtype is
reached, through an alias, a glob import or another module's alias, it has
the same brand, and two modules that each declare a `Meters` declare two
different newtypes. Renaming or moving a declaration changes its brand.

**`Any` keeps a value's identity.** A newtype value assigned to `Any` is still
a value of that newtype, and `value as Name` (under `casts`) only checks that
it is: casting never makes a newtype value out of another type. The same
check is made wherever a value of type `Any` is used as a newtype: assigned to
a stream declared with one, passed as a function argument, or placed in a
branch, element or field whose type holds one, whether or not the value is
used afterwards. Comparing, with `==` or `!=`, a newtype value that arrived
through `Any` with a value of another newtype, or with a value that is not a
newtype value, is likewise a type error rather than `false`. A failed check
stops the monitor with a type error; it is never `false` and never an absent
value.

Containers stay structural: a `List<Meters>` is an ordinary list whose
elements are newtype values. A newtype value is written in JSON with its own
marker, as `{"$newtype": "<brand>", "value": <representation>}`; the
`$newtype` member is reserved in the same way as a union's `$tag`. Values that
arrive on an input are not checked against the input's declared type, as no
input value is: a value with the wrong brand, or a bare representation, is
refused at its first use as that newtype, such as `Name::value` or a cast.
Standard output writes a newtype value as its brand followed by `::new(…)`. A
[constant](#constants) can hold a newtype value, built by the declaring
module's exported functions.

Newtype values are held in the general value representation, so a stream of
a newtype type does not take the typed scalar path that `Int`, `Float` and
`Bool` streams do. Programs that declare no newtype are unaffected.

## Operators

The operators, from highest to lowest precedence, are:

| Operators | Meaning | Associativity |
|---|---|---|
| calls, field access, `x[n]` | application, fields, stream history | left |
| `**` | exponentiation | right |
| unary `-`, `!`, `not` | numeric and Boolean negation | right |
| `as` | scalar cast | left |
| `*`, `/`, `%` | multiplication, division, remainder | left |
| `+`, `-` | addition and subtraction | left |
| `<`, `<=`, `>`, `>=` | ordering | left |
| `==`, `!=` | equality and inequality | left |
| `&&`, `and` | conjunction | left |
| `||`, `or` | disjunction | left |
| `=>` | implication | left |
| `++` | string concatenation | left |

`and`, `or`, and `not` are reserved aliases for `&&`, `||`, and `!`.

The `casts` experiment adds `value as Type` plus `trunc`, `floor`, `ceil`,
and `round`. Casts admit an identity conversion, `Int as Float`, and
`Int`/`Float`/`Bool`/`Unit` to `Str`; an identity conversion produces a
redundant-cast warning. A newtype value can be cast to `Any` and back to its
own newtype, which checks its identity at run time (see
[Newtypes](#newtypes)). Convert a `Float` to `Int` explicitly with one of the
four rounding functions. `round` follows IEEE 754 ties-to-even:
`round(-3.5)`, `round(-2.5)`, `round(-1.5)`, and `round(-0.5)` are
`-4`, `-2`, `-2`, and `0`; `round(0.5)`, `round(1.5)`, `round(2.5)`, and
`round(3.5)` are `0`, `2`, `2`, and `4`. A non-finite or out-of-range result
is a runtime operation failure. `as`, `trunc`, `floor`, `ceil`, and `round`
are reserved words in every DSRV file, including files that do not enable the
experiment.

Integer power is checked: overflow terminates evaluation, and an integer
exponent must be non-negative. Power promotes mixed `Int`/`Float` operands to
`Float`, so `4 ** 0.5` is `2.0`. It is right-associative and binds above unary
minus: `2 ** 3 ** 2` is 512, while `-2 ** 2` is -4.

`==` and `!=` compare whole values, including structs:
`{x: 1, label: "p"} == {label: "p", x: 1}` is `true`, since field order does
not matter, and nested structs compare field by field. Both operands must
have the same type, so comparing structs with different fields or field
types is a type error. A value of a permissive struct type
(`Struct<x: Int, ...>`) is compared with all its fields, the extra ones too.

This complete fixture parses and evaluates first-class `!=`, `**`, and the
keyword spelling `and`:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_operator_syntax}}
```

## Literals

Integer source magnitudes are limited to `i64::MAX`, so literals range from
`-9223372036854775807` through `9223372036854775807`.

Floats include `0.5`, `1e6`, `1E-6`, and `1.5e+3`; a digit is required on
each side of the decimal point, so `1.0` is a Float and `1.` is not.
Leading-zero mantissas are allowed. Float underflow
rounds to signed zero, while a source literal that overflows to infinity is
rejected. `NaN`, `inf`, and `Infinity` remain identifiers rather than numeric
literals.

The accepted forms and malformed scientific literals are exercised by this
complete parser example:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_numeric_literals}}
```

## Object literals

`{x: 1, label: "p"}` builds a struct value. A field name is an identifier
or, for any other name, a quoted string. A field whose value is the stream or
binder of the same name can be written by its name alone:

```dsrv
in x: Int
in label: Str
out reading: Struct<x: Int, label: Str, "unit name": Str> = {x, label, "unit name": "mm"}
```

Here `{x, label, …}` is `{x: x, label: label, …}`. A quoted field name has no
short form, and naming the same field twice is an error.

### Struct update

`{ field: value, ..base }` is the struct `base` with the fields it names
replaced. The base comes last, after the fields, where the `..` of a struct
pattern stands for the fields the pattern does not name. The update has the
base's type, so every field it names must be one of that type's, and each
value must have that field's type. The other fields keep the base's values:

```dsrv
type Reading = Struct<x: Int, label: Str, valid: Bool>
in reading: Reading
out checked: Reading = { valid: reading.x >= 0, ..reading }
```

Fields take the same short forms as in an object literal, and a trailing
comma may follow the base. `{ ..base }` is the base itself. The base can be
any struct-typed expression, such as a stream, a field, or a call:
`{ sequence: Some(n), ..empty(head) }` builds on what `empty` returns. A
field the base's type lacks, a field named twice, and a base that is not a
struct are all errors. Under gradual checking a base of type `Any` is
accepted, and its fields are only known when the monitor runs.

This example evaluates two updates and shows the forms that do not check:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_struct_update}}
```

## Delimited lists

One trailing comma is allowed in delimited value, call, variable-set, and
collection, tuple, or struct type lists:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_trailing_commas_and_list_get}}
```

Lambda parameters and function types do not gain a trailing-comma form.

## Conditional chains

Use nested `else if` for a conditional chain. There is no `elif` keyword:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_else_if_chain}}
```

## Functions

`\x: Int -> x + 1` is a function of one parameter; `\acc: Int, x: Int -> acc + x`
takes two. A function can be called in place, as in `(\n: Int -> n * 2)(s)`,
or passed to `List.map`, `List.filter` and `List.fold`.

A parameter's type can be left out when the context determines it: in a
list callback, from the list's element type (and, for `List.fold`, the
initial value), and in a function called in place, from its arguments.

```dsrv
in xs: List<Int>
out ys: List<Int> = List.map(\x -> x + 1, xs)
out s: Int = List.fold(\acc, x -> acc + x, 0, xs)
out d: Int = (\n -> n * 2)(s)
```

A parameter written with a type keeps that type, and the context must agree
with it. Where nothing determines a missing type, for instance a function
stored in a list, strict type checking reports `cannot infer the type of
lambda parameter`, and gradual checking gives the parameter the dynamic type
`Any`.

## Local stream functions

Under `use experimental::local_streams`, a `def` can keep private stream
history in a `where` block. Enable `functions` too, or use the
`high_level_dsrv` bundle:

```dsrv
use experimental::high_level_dsrv

def count_events(hit: Bool) -> Int = count
where {
  count: Int = default(count[1], 0) + (if hit then 1 else 0)
}

in hit: Bool
aux count: Int = count_events(hit)
out seen: Bool = count > 0
```

Each written call creates an independent instance. Binding a call's result to
one stream and reusing that stream shares the instance. Arguments and local
streams advance on every enclosing tick, with ordinary history and `NoVal`
behaviour. Local names are private. The `where` block may appear on the same
line as the result and may be empty; it admits stream declarations and equations,
including split declarations and forward references, but no `in`, `out` or
nested `def` declarations.

Call a stream function as the whole right-hand side of an `aux` or `out`
equation, including a local equation. A function may also return another
stream function's entire result. Such a forwarding function has the same
placement restrictions. Calls inside operators, conditionals, match arms,
lambdas, list callbacks, constants or runtime `dynamic`/`defer` text are refused.
Bind a stateful intermediate to a local stream in a `where` block instead.

A stream function may separate inferred type parameters and fixed configuration
from tick-varying arguments:

```dsrv
def remembered<T, selected: (T -> Bool)>(input: T) -> Bool = seen
where {
  seen: Bool = default(seen[1], false) || selected(input)
}
```

Call it with `remembered::<is_relevant>(observation)`, or qualify the name when
it is imported. Fixed data must be closed and independent of streams; fixed
functions must be named ordinary defs or forwarded fixed parameters. Inline
lambdas, captured stream/history/runtime text and stateful callbacks are
refused. The returned value has the declared result type.

The experiment is checked in the defining module, including for unused defs.
Actual instances run with `dataflow` and `semi-sync`; reconfigurable runtimes
refuse them in both the initial model and replacements. Enabling the experiment
or declaring an unused function alone does not require instance support.
An ordinary `def` without `where` keeps its existing semantics.

## Constants

Under `use experimental::{constants}`, `const name: Type = e` names a value
that is worked out once, when the specification is read, and written in
wherever the name is used, including as a stream offset such as `x[lag]`. A
constant is not a stream: it has no history and is never an output. Its type
must be declared, and its value must have that type.

The body of a constant may use literals, operators, other constants and calls
to `def` functions, but nothing that only a running monitor settles. A body
that reads a stream, or calls a function that reads history or evaluates text
supplied at run time, is refused when the specification is read, as is a
constant defined in terms of itself, directly or through a function it
calls. A body that calls a function is checked against the constant's declared
type, as an equation is checked against its stream's type, and the declared
type supplies any parameter that nothing else determines.

A constant can hold a [newtype](#newtypes) value:

```dsrv
use experimental::{newtypes, generics, functions, constants}

newtype JournalTime = Unit
newtype Producer = Unit
newtype Duration<D> = Float

def seconds<D>(x: Float) -> Duration<D> =
  if x >= 0.0 then Duration::new(x) else Duration::new(0.0)
def longer<D>(a: Duration<D>, b: Duration<D>) -> Bool =
  Duration::value(a) > Duration::value(b)

const budget: Duration<JournalTime> = seconds(0.2)

in x: Float
aux elapsed: Duration<JournalTime> = seconds(x)
out over: Bool = longer(elapsed, budget)
```

Here `seconds` mentions `D` only in its result, so the declared type
`Duration<JournalTime>` is what gives `budget` its domain. The value keeps its
brand wherever the constant is named, including in another module that
imports it: `budget` is not a `Float`, and is not a `Duration<Producer>`, and
using it as either is a type error when the specification is checked. A
constant's body follows the same rule for `Name::new` and `Name::value` as an
equation does: only the module that declares the newtype may write them, so
another module builds such a constant by calling the declaring module's
exported functions. The declaring module may write them in a constant it
keeps `internal`.

This fixture checks and evaluates the example above, and shows two uses of
`budget` that checking refuses:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_newtype_constants}}
```

A module's constants are imported like its defs, and an `internal` constant is
not available to an importer. Text supplied at run time to `dynamic` or
`defer` sees a module's constants as an importer does, so it can name a
constant the module exports, with that constant's brand, but never an
internal one.

## History and collection access

Square brackets on an expression are stream-history access: `x[1]` reads one
earlier logical tick. They are not general collection indexing. Use
`List.get(values, index)` for a dynamic list lookup.

Some code runs only on some ticks: a `match` arm runs on the ticks that
select it and a guard on the ticks that try it, and under the `lazy_if`
experiment so does an `if` branch (see
[Lazy `if`](dsrv-language-settings.md#lazy-if)). Inside such code, and inside
a lambda written there, `x[n]` of a named stream still reads that stream as
it was `n` ticks ago, exactly as outside. The history of a computed
expression, such as `(x + 1)[1]`, is kept only on the ticks that run it.

Two reads are refused, because they would silently see only some ticks. A
lambda parameter or a name a pattern binds is not a stream, so its previous
value cannot be read there:

```text
invalid stream program: `v[1]` in a lazy `if` branch or a `match` arm or guard would read only the ticks that run it, and `v`, bound by a lambda parameter or pattern, is not a stream whose previous value can be read instead; read the previous value of the stream it comes from
```

Text supplied at run time to `dynamic` or `defer` has no equation of its own,
so a stream's previous value inside such code there is refused too. Read the
value in an equation, and name that stream in the text.
