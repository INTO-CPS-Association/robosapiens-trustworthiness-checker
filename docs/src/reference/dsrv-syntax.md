# DSRV syntax

This page describes how to write DSRV declarations and expressions. Start with
the [DSRV tutorial](../tutorials/write-dsrv-monitor.md) if you are writing your
first monitor. For `language` and `use experimental` declarations,
see [DSRV dialects and experiments](dsrv-language-settings.md). For type
annotations, inference and conversions, see [DSRV type system](dsrv-types.md).

## Declarations and equations

A specification is a sequence of declarations. `in x: Int`, `out y` and `aux a`
declare an input, an output and an auxiliary stream. Inputs require a type;
outputs and auxiliaries may omit it. `var` is an older spelling of `aux`. An *equation* `y = e`
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

Type annotations use forms such as `Int`, `List<Float>` and
`Struct<level: Int, ...>`. See [DSRV type system](dsrv-types.md) for the
available types, inference and conversions.

## Type declarations

`type Reading = Struct<level: Int, valid: Bool>` names a structural alias.

See [type aliases](dsrv-types.md#type-aliases) for their scope and meaning.

## Operators

The operators, from highest to lowest precedence, are:

| Operators | Meaning | Associativity |
|---|---|---|
| calls, field access, `x[n]` | application, fields, stream history | left |
| `**` | exponentiation | right |
| unary `-`, `!`, `not` | numeric and Boolean negation | right |
| `as` | cast | left |
| `*`, `/`, `%` | multiplication, division, remainder | left |
| `+`, `-` | addition and subtraction | left |
| `<`, `<=`, `>`, `>=` | ordering | left |
| `==`, `!=` | equality and inequality | left |
| `&&`, `and` | conjunction | left |
| `||`, `or` | disjunction | left |
| `=>` | implication | left |
| `++` | string concatenation | left |

`and`, `or`, and `not` are reserved aliases for `&&`, `||`, and `!`.

`value as Type` writes a cast. See [casts and numeric types](dsrv-types.md#casts-and-numeric-types)
for accepted conversions, integer-literal inference and rounding. `as`,
`trunc`, `floor`, `ceil`, and `round` are reserved in every DSRV file and
need no experiment declaration.

Integer power is checked: overflow terminates evaluation, and an integer
exponent must be non-negative. Power of two `Int`s is an `Int`, and of two
`Float`s a `Float`. It is right-associative and binds above unary minus:
`2 ** 3 ** 2` is 512, while `-2 ** 2` is -4.

`==` and `!=` compare whole values, including structs:
`{x: 1, label: "p"} == {label: "p", x: 1}` is `true`, since field order does
not matter, and nested structs compare field by field. A value of one
operand's type must be usable as the other's, so comparing closed structs with
different fields, or structs with different field types, is a type error. A value of a permissive struct type
(`Struct<x: Int, ...>`) is compared with all its fields, the extra ones too.

Equality also follows the [numeric typing rules](dsrv-types.md#casts-and-numeric-types).

For example:

```dsrv
in base: Int
in exponent: Int
out different: Bool = base != exponent
out signed_power: Int = (-base) ** exponent
out both_positive: Bool = base > 0 and exponent > 0
```

## Literals

`true` and `false` are Boolean literals, quoted strings such as `"ready"`
have type `Str`, and `()` is the `Unit` value. `Tuple(1, "ready")` constructs
a tuple of type `(Int, Str)`; a singleton is `Tuple(1)` with type `(Int,)`.
Parentheses alone group expressions: `(1, "ready")` is not a tuple expression.
Lists can be written `[1, 2]` or `List(1, 2)`.

Integer source magnitudes are limited to `i64::MAX`, so literals range from
`-9223372036854775807` through `9223372036854775807`.

Floats include `0.5`, `1e6`, `1E-6`, and `1.5e+3`; a digit is required on
each side of the decimal point, so `1.0` is a Float and `1.` is not.
Leading-zero mantissas are allowed. Float underflow
rounds to signed zero, while a source literal that overflows to infinity is
rejected. `NaN`, `inf`, and `Infinity` remain identifiers rather than numeric
literals.

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

```dsrv
in values: List<Int,>
out first: Int = List.get(values, 0,)
out count: Int = List.len(values,)
```

Lambda parameters and function types do not gain a trailing-comma form.

## Conditional chains

Use nested `else if` for a conditional chain. There is no `elif` keyword:

```dsrv
in temperature: Int
out level: Int = if temperature > 90 then 2
                else if temperature > 70 then 1
                else 0
```

## Functions

`\x: Int -> x + 1` is a function of one parameter; `\acc: Int, x: Int -> acc + x`
takes two. A function can be called in place, as in `(\n: Int -> n * 2)(s)`,
or passed to `List.map`, `List.filter` and `List.fold`.

A parameter's type can be left out when a concrete type is already supplied
by a list callback, an expected function type, or the arguments of a function
called in place. Otherwise annotate it before checking the body.

```dsrv
in xs: List<Int>
out ys: List<Int> = List.map(\x -> x + 1, xs)
out s: Int = List.fold(\acc, x -> acc + x, 0, xs)
out d: Int = (\n -> n * 2)(s)
out zs: List<Float> = List.map(\a -> a * 2.0, List(1.0, 2.0))
```

In the last line the list supplies `Float` for `a`. A parameter written with a
type keeps that type, and the context must agree with it. Later expressions do
not resolve an unknown parameter type.

See [function compatibility](dsrv-types.md#function-compatibility) for using
function values and callbacks at expected types.

## History and collection access

Square brackets on variables are stream-history access: `x[1]` reads one
earlier logical tick of the stream. They are not general collection indexing. Use
`List.get(values, index)` for a dynamic list lookup.

## Experimental features

These features require explicit opt-in through `use experimental`. See
[language settings](dsrv-language-settings.md#experiments) for the experiment
registry and supported runtimes.

### Generic aliases

**Experimental:** `use experimental::generics`.

An alias may take parameters, for example
`type Boxed<T> = Struct<value: T>`. Use it as `Boxed<Int>`.
See [generic aliases](dsrv-types.md#generic-aliases) for their typing rules.

### Tagged unions and `match`

**Experimental:** `tagged_unions` for unions and constructors;
`pattern_matching` for `match`, `matches` and patterns. Declare both when using
them together:

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

A struct pattern matches a value with exactly the fields it names, unless it
ends in `..`, which allows others. A closed struct type has all its fields, so
a pattern without `..` that leaves one out can never match it, and is a type
error naming the omitted fields: with `s: Struct<a: Int, b: Int>`, write
`{ a: v, .. }` or `{ a: v, b: _ }`, not `{ a: v }`.

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

### Named functions

**Experimental:** `use experimental::functions`.

Named functions use `def`:

```dsrv
use experimental::{functions}

def double(n: Int) -> Int = n * 2

in x: Int
out y: Int = double(x)
```

### Constants

**Experimental:** `use experimental::constants`.

`const n: Int = 3` declares a constant.
A constant is checked at its declared type and evaluated before the monitor
starts. Initializers support literals, arithmetic, casts and containers, and
may name other constants, including ones declared later. They use the same
typing rules as equations; invalid initializers are reported while the model is read.

A constant keeps its declared type wherever it is used. The literal in
`const c: Float = 3` adopts `Float`, but a declared `Int` constant does not:
with `n: Int`, both `const c: Float = n` and `2.0 * n` are type errors.
Use `2.0 * (n as Float)` to convert it explicitly; for `n = 3`, this gives `6.0`.

### Lazy conditionals

**Experimental:** `use experimental::lazy_if`.

Only the selected branch of an `if` runs, with its own local timeline.
This experiment requires the `dataflow` or `reconf-dataflow` runtime.
