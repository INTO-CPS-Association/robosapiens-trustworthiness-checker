# DSRV syntax

This page records DSRV expression syntax and its boundary behavior. Start with
the [DSRV tutorial](../tutorials/write-dsrv-monitor.md) if you are writing your
first monitor.

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
cycles are rejected. Aliases do not create nominally distinct types.

Struct field names in types can be quoted, as in
`Struct<"quoted field": Int>`. See the
[source-context architecture](../architecture/dsrv-source-contexts.md) for
how aliases reach runtime expressions.

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
redundant-cast warning. Convert a `Float` to `Int` explicitly with one of the
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

## History and collection access

Square brackets on an expression are stream-history access: `x[1]` reads one
earlier logical tick. They are not general collection indexing. Use
`List.get(values, index)` for a dynamic list lookup.
