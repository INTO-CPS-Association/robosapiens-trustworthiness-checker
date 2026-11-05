# DSRV type system

DSRV checks types before monitoring. This page explains annotations, inference and
conversions. For declaration and expression forms, see [DSRV syntax](dsrv-syntax.md).

## Types and annotations

| Type | Values |
|---|---|
| `Int`, `Float` | Integers and floating-point numbers |
| `Bool`, `Str` | Booleans and strings |
| `Unit` | The single value `()` |
| `List<T>`, `Map<T>` | Lists and string-keyed maps with items of type `T` |
| `(A, B)` | A tuple whose positions have types `A` and `B` |
| `Struct<x: T>` | A struct with exactly the listed fields |
| `Struct<x: T, ...>` | A struct with the listed fields and possibly others |
| `(A -> B)` | A function from `A` to `B` |
| `Expr<T>` | Source text for an expression expected to produce `T` |

Experimental [unions](dsrv-syntax.md#tagged-unions-and-match) and
[generic aliases](#generic-aliases) add further forms.
The [dialect and experiment settings](dsrv-language-settings.md) determine
which forms a file may use.

Inputs require type annotations. An unannotated output or auxiliary stream
takes the type of its equation once its dependencies are known.
Recursive dependencies need enough annotations to establish their types.

`Unit` is ordinary data. It differs from a tick with no event (`NoVal`) or an
expression waiting for history (`Deferred`). Those states pass type checks;
they do not turn into `()`. See the
[execution model](../architecture/dataflow/model.md#values-that-are-not-ordinary-data)
for their temporal behaviour.

## Inference and explicit common types

Inference uses known argument types and expected types already available at
an expression. It does not leave an unknown type for later code to resolve.
Supply an annotation for an ambiguous empty collection or lambda.
For example, `out xs: List<Int> = []` gives the empty list its element type.
`List.len([])` alone does not say what that element type is.

When alternatives have the same type, `if`, `default`, `update`, `init`
and collection literals preserve it. Different record or function shapes need
an explicit common type:

```dsrv
in choose: Bool
in left: Struct<a: Int>
in right: Struct<a: Int, b: Bool>
out selected: Struct<a: Int, ...> = if choose then left else right
```

Both inputs fit the declared open type. Without the annotation, the checker
requests a common type instead of inferring the intersection of record fields.
Incompatible scalar alternatives such as `Int` and `Str` are errors.

Invalid operations, such as division by zero or `List.head` of an empty list,
still stop monitoring. See the [process contract](process-contract.md#startup-and-failure-boundaries).

## Struct compatibility

A struct type that ends in `...`, as in `Struct<x: Int, ...>`, is open: a value
must have the listed fields and may have others. An open struct only guarantees
the fields it lists, so it is a type error to use one where a struct that needs
more fields is expected, whether that struct is closed (`Struct<x: Int, y: Int>`)
or open (`Struct<x: Int, y: Int, ...>`). It is also a type error to use one
where a closed struct listing just the same fields is expected: a closed struct
has no other fields, and an open struct does not say that of its value. Build the
closed value instead, as in `t = {x: p.x}`. The other way round is accepted: a
`Struct<x: Int, y: Int>` can be used as a `Struct<x: Int, ...>`.

Only the fields a struct type lists can be read. With `p: Struct<x: Int, ...>`,
`p.y`, `Map.get(p, "y")` and `Map.has_key(p, "y")` are type errors, whether or
not a value of `p` happens to have a `y`. Declare every field the program reads.
Extra fields are still part of the runtime value: equality compares them and
output prints them.

## Type aliases

`type State = Struct<speed: Int, stopped: Bool>` declares a structural alias.
Aliases can name any supported type, refer to later declarations, and nest in
collections and structs. Duplicate names, unknown names, and recursive alias
cycles are rejected. Aliases do not create nominally distinct types: two aliases
that expand to the same structure are interchangeable.

Struct field names in types can be quoted, as in
`Struct<"quoted field": Int>`. Printed specifications put alias declarations
before stream declarations and print other types in expanded structural form,
so the result parses back to the same specification.

## Casts and numeric types

Write `value as Type` for an explicit cast. Casts and the rounding functions
are available in every DSRV dialect without an experiment declaration.

| Cast | Behaviour |
|---|---|
| A value to its own type | Preserves it; warns that the cast is redundant |
| `Int` to `Float` | Converts the number |
| `Str` to `Expr<T>` | Annotates source text; `eval` or `defer` checks that it produces `T` |
| `Int`, `Float`, `Bool` or `Unit` to `Str` | Converts to text |
| An empty list or map to a collection type | Sets its element type, through a named alias of that collection type |

An alias of a target type follows the same rules. A non-empty collection cannot
be cast to a collection with incompatible element types. Cast validity is
checked from the source's type; casts do not recover an unknown type.

`Float as Int` is rejected. Use `trunc`, `floor`, `ceil` or `round` to convert
explicitly. `round` uses ties-to-even: `round(2.5)` is `2`, `round(3.5)` is `4`,
and `round(-2.5)` is `-2`. A non-finite or out-of-range result fails at runtime.

`Int` and `Float` never convert implicitly: with `n: Int` and `f: Float`,
`n + f` is a type error, and so is `n < f`. Write `(n as Float) + f`.

An integer literal takes the type its context needs, so `f * 2` is a `Float`,
`n * 2` is an `Int`, and a literal with no context is an `Int`. Division is
the one thing to watch: `7 / 2` is always the `Int` 3, so `f + 7 / 2` is an
error.

## Function compatibility

A function used at another function type must accept every argument that type
allows and return a compatible result. For example, a function accepting
`Struct<a: Int, ...>` can serve a caller passing `Struct<a: Int, b: Int>`;
the reverse would require a field the caller need not supply.

List callbacks follow these rules too. Known list element types provide
callback parameter types, and the checked body supplies its result type.
`List.filter` requires a `Bool` result. For `List.fold`, the initial value and
callback result must fit the accumulator type; supply a common accumulator type
when different record or function shapes are intended.

## Experimental features

These features require explicit opt-in through `use experimental`. See
[language settings](dsrv-language-settings.md#experiments) for the experiment
registry and supported runtimes.

### Tagged unions and match

**Experimental:** `tagged_unions` for unions and constructors; `pattern_matching`
for `match` and patterns.

Use a [tagged union](dsrv-syntax.md#tagged-unions-and-match) to represent
alternatives such as `Int` and `Str`. A constructor needs an expected union type
or an explicit qualifier, as in `State::Moving(speed)`. The alternatives of a
`match` follow the same common-type rules as those of `if`.

### Module-scoped aliases

**Experimental:** `use experimental::modules`.

An alias belongs to the module that declares it, in a namespace separate from
stream and local names. Forward references resolve within that module; imports
make selected aliases available to another module. A function imported from a
module keeps the aliases and language settings of the module that wrote its
body. An argument passed to that function keeps the caller's context. See
[language settings](dsrv-language-settings.md#experiments) for the `modules`
and `functions` experiments.

### Generic aliases

**Experimental:** `use experimental::generics`.

An alias can take type parameters:
`type Boxed<A> = Struct<value: A, count: Int>`. A use supplies one argument
per parameter, as in `in x: Boxed<Str>`, and an argument can be any type,
including another application: `Boxed<Boxed<Int>>`.

Supplying the wrong number of type arguments is an error. An alias body that
refers back to itself is a recursive alias cycle and is rejected. Generic alias
bodies are checked when used; an unused generic alias need not resolve.

Parameters are positional and scoped to the alias that declares them. They are
capitalised like any other type name, and a parameter shadows a declared alias
of the same name within that body.

### Embedded `std::option`

**Experimental:** `modules` and `generics` to import and use the type;
`tagged_unions` to write its constructors.

The embedded `std::option` module provides the generic alias
`Option<T> = Union<Some: T, None>` and the functions `is_some`, `is_none`, and
`unwrap_or`. It has no prelude: `use std::option` activates the module, and
`use std::option::*` also brings its names into unqualified scope. With a plain
module import, the current spellings are `option::Option<T>` for the type and
`std::option::is_some`, `std::option::is_none`, and
`std::option::unwrap_or` for functions.

A caller enables `modules` and `generics` to import and use `Option<T>`, plus
`tagged_unions` if it writes `Some` or `None`. Constructors need an expected
`Option<T>` type. A declared result can supply it to `unwrap_or(None, 7)`;
`is_some(None)` has no concrete payload type and needs a typed intermediate.
`unwrap_or` evaluates its fallback argument even for `Some`. The module does
not currently provide `unwrap`, `or`, `map`, or `and_then`. The
[inspection example](../developer-tools/tc-expand.md#example) shows a complete
model using the module.

### Generic function arguments

**Experimental:** `use experimental::{functions, generics}`.

Generic `def` signatures declare the type parameters explicitly:

```dsrv
use experimental::{functions, generics}
def id<T>(x: T) -> T = x
in numbers: List<Int>
out copied = id(numbers)
out empty: List<Int> = id([])
```

The checker matches known argument types against the signature. A required
result type supplies concrete substitutions before arguments are checked, so
`empty` determines `T = List<Int>` before the empty argument is visited.
Repeated uses of a parameter must agree with that choice. Different struct
shapes do not trigger common-field inference.

Arguments are checked in order. An unresolved earlier call does not take its
type from a later sibling. Give that value a typed intermediate instead:

```dsrv
use experimental::{functions, generics}
def make<T>() -> List<T> = []
def same<T>(a: List<T>, b: List<T>) -> Bool = a == b
in numbers: List<Int>
aux empty: List<Int> = make()
out equal: Bool = same(empty, numbers)
```

Without the intermediate, `same(make(), numbers)` needs an annotation at its
first argument. The required result is only `Bool`, which cannot determine `T`.
