# DSRV type aliases and source contexts

Structural aliases give DSRV types reusable source names. This page explains
how those names are resolved into structural types and how the resulting
namespace travels with expressions into runtime-compiled source. For author
syntax, see the [DSRV reference](../reference/dsrv-syntax.md#type-aliases).

## Structural identity and source ownership

An alias is a name for a structural type, not a nominal type. Two aliases
expanding to the same structure are interchangeable, and consumers of a
resolved specification only see the expanded structure. Alias declarations
share one specification-level namespace, separate from stream and local
variable names. Forward references resolve; duplicate names, unknown names,
and cycles fail resolution.

| Entity | Responsibility |
|---|---|
| Private parsed trees and source types | Retain unresolved names and source spans until resolution. |
| `SourceContext` | Own an immutable name-to-expanded-type namespace shared by a specification and its runtime expressions. |
| Semantic `Expr` and `StreamType` | Represent resolved expressions and structural types without nominal alias identity. Expression metadata retains the source context. |

Parsing transcodes the private source tree into the semantic tree after
resolution. The generic contiguous-tree library supplies fallible
cross-schema transformation; it does not know DSRV aliases. Resolution errors
retain source spans rather than becoming evaluation failures.

Displaying a specification writes its alias declarations before the stream
declarations. Types elsewhere are written in their expanded structural form,
with struct field names quoted, so the displayed text parses back to the same
specification.

## Runtime source

Runtime `dynamic` and `defer` source text resolves against the owning
specification's immutable `SourceContext`, so it can use the specification's
aliases. Reconfiguration cache identity includes the complete
name-to-expanded-type mapping; declaration order does not matter, while
changing an unused alias still changes the namespace fingerprint.

## Implementation and executable evidence

| Boundary | Implementation |
|---|---|
| Parsing and resolution | `src/lang/dsrv/parsed.rs`, `source.rs`, `parser.rs`, `lalr.lalrpop` |
| Lowering of runtime source nodes | `src/dataflow/compiler/lower.rs` |
| Source-aware reconfiguration | `src/dataflow/execution/reconfigurable_expressions.rs` |

The `source_tests` module in `src/lang/dsrv/parser.rs` and the tests in
`src/lang/dsrv/source.rs` exercise resolution and display.
`tests/compile_fail.rs` checks that the parsed tree cannot be used outside the
frontend.
