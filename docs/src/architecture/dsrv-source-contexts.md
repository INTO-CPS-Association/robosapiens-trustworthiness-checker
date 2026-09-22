# DSRV type aliases and source contexts

Structural aliases give DSRV types reusable source names. This page explains
how those names are resolved into structural types and how the resulting
namespace travels with expressions into runtime-compiled source. For author
syntax, see the [DSRV reference](../reference/dsrv-syntax.md#type-aliases).

## Structural identity and source ownership

An alias is a name for a structural type, not a nominal type. Two aliases
expanding to the same structure are interchangeable, and consumers of a
resolved specification only see the expanded structure. Alias declarations
belong to the module that declares them, in a namespace separate from stream
and local variable names. Forward references resolve within that namespace;
imports make selected names available to another module. Duplicate names,
unknown names, and cycles fail resolution.

| Entity | Responsibility |
|---|---|
| Private parsed trees and source types | Retain unresolved names and source spans until resolution. |
| `SourceContext` | Owns one module's immutable expanded-type namespace and resolved language settings. |
| Definition environment | Retains the context and callable names of the module that wrote a definition. |
| Semantic `Expr` and `StreamType` | Represent resolved expressions and structural types without nominal alias identity. Expression metadata identifies the context that owns each node. |
| `RuntimeExpressionSite` | Captures the context, callables, typing, and required source files for one `dynamic` or `defer` occurrence. |

Collection parses each source file before expansion builds its namespace and
definition environment. Expansion then transcodes the private source trees
into one semantic program. The generic contiguous-tree library supplies
fallible cross-schema transformation; it does not know DSRV aliases.
Resolution errors retain source locations rather than becoming evaluation
failures.

When a function body is inlined, nodes written in the body keep the defining
module's context. Argument subtrees keep the caller's context. This separation
lets a library change its internal aliases or experiments without looking up
same-named declarations in its caller.

Displaying a specification writes its alias declarations before the stream
declarations. Types elsewhere are written in their expanded structural form,
with struct field names quoted, so the displayed text parses back to the same
specification.

## Runtime source

Source supplied to `dynamic` and `defer` resolves against the context of the
occurrence that accepts it. A root occurrence therefore sees the root
namespace; an occurrence written in an inlined library definition sees that
library's aliases, constants, and callable definitions. The prepared
`RuntimeExpressionSite` keeps that environment after the original program has
been dropped.

Reconfiguration identity includes the semantic namespace and callable
environment, but not source labels, paths, or diagnostic spans. Declaration
order does not matter, while changing an available alias or definition changes
the identity even when the current runtime source does not name it.

## Implementation and executable evidence

| Boundary | Implementation |
|---|---|
| Parsing | `src/lang/dsrv/syntax/parsed.rs`, `syntax/lalr.lalrpop` |
| Module namespaces and definition environments | `src/lang/dsrv/expand/graph.rs`, `expand/functions.rs`, `source.rs` |
| Semantic expansion and context ownership | `src/lang/dsrv/expand/mod.rs` |
| Runtime-expression preparation and acceptance | `src/lang/dsrv/runtime_expression.rs` |
| Lowering of runtime source nodes | `src/dataflow/compiler/lower.rs` |
| Source-aware reconfiguration | `src/dataflow/execution/reconfigurable_expressions.rs` |

The `source_tests` module in `src/lang/dsrv/parser.rs`, the lexical-environment
tests in `src/lang/dsrv/lexical_tests.rs`, and the runtime-expression tests
exercise resolution, ownership, and retained environments.
`tests/compile_fail.rs` checks that the parsed tree cannot be used outside the
frontend.
