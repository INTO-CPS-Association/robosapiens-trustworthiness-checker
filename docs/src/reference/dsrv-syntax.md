# DSRV syntax

This page records DSRV expression syntax and its boundary behavior. Start with
the [DSRV tutorial](../tutorials/write-dsrv-monitor.md) if you are writing your
first monitor.

## Operators

The operators, from highest to lowest precedence, are:

| Operators | Meaning | Associativity |
|---|---|---|
| calls, field access, `x[n]` | application, fields, stream history | left |
| `**` | exponentiation | right |
| unary `-`, `!`, `not` | numeric and Boolean negation | right |
| `*`, `/`, `%` | multiplication, division, remainder | left |
| `+`, `-` | addition and subtraction | left |
| `<`, `<=`, `>`, `>=` | ordering | left |
| `==`, `!=` | equality and inequality | left |
| `&&`, `and` | conjunction | left |
| `||`, `or` | disjunction | left |
| `=>` | implication | left |
| `++` | string concatenation | left |

`and`, `or`, and `not` are reserved aliases for `&&`, `||`, and `!`.

Integer power is checked: overflow terminates evaluation, and an integer
exponent must be non-negative. Power promotes mixed `Int`/`Float` operands to
`Float`, so `4 ** 0.5` is `2.0`. It is right-associative and binds above unary
minus: `2 ** 3 ** 2` is 512, while `-2 ** 2` is -4.

This complete fixture parses and evaluates first-class `!=`, `**`, and the
keyword spelling `and`:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_operator_syntax}}
```

## Literals

Integer source magnitudes are limited to `i64::MAX`, so literals range from
`-9223372036854775807` through `9223372036854775807`.

Floats include `0.5`, `1.`, `1e6`, `1E-6`, and `1.5e+3`; a digit is required
before the decimal point. Leading-zero mantissas are allowed. Float underflow
rounds to signed zero, while a source literal that overflows to infinity is
rejected. `NaN`, `inf`, and `Infinity` remain identifiers rather than numeric
literals.

The accepted forms and malformed scientific literals are exercised by this
complete parser example:

```rust
{{#include ../../../tests/docs_examples.rs:dsrv_numeric_literals}}
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

## History and collection access

Square brackets on an expression are stream-history access: `x[1]` reads one
earlier logical tick. They are not general collection indexing. Use
`List.get(values, index)` for a dynamic list lookup.
