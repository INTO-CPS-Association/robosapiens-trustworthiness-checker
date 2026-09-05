# The scalar IR

Every region, of either scope, is expressed once as a `ScalarProgram`, and both the quickened interpreter and the native backend derive their artifacts from that one form. This page is that form: what may appear in it, how it is shaped, and which consumers admit which parts of it.

This layer owns the legality rules and the shared representation. It owns no registers, no evaluator state, and no compiled artifact — those belong to the tiers that consume it.

[Fusion and regions](fusion.md) motivates this representation and defines the regions expressed in it; this page is the representation itself.

## What counts as scalar

Scalar describes a *representation*: a value that fits a machine register directly, so a region can hold it in one and hand it to the next instruction without going through the arena. Everything this layer does rests on that property, not on the particular list of types that have it.

`ScalarKind` is that list as it currently stands — `Int`, `Float` and `Bool`. The other `Value` variants, `Str`, `List` and `Map`, have no such representation today, and streams producing them stay canonical. Widening `ScalarKind` would widen what can be fused without changing anything else on this page; the region scopes, the legality predicate and the arena boundary are all stated over the property rather than over the list.

Alongside the kind, every scalar value records a `ScalarPresence`: `MaybeSpecial` when it may still be `NoVal` or `Deferred`, `AlwaysPresent` when it cannot. Scalar is therefore not a promise that a value is present. Sparse semantics survive fusion because presence rides in the type rather than being checked for at each use.

Both come from the type checker. That is the design commitment: eligibility is settled when the plan is built, never reconsidered per row, and never inferred by watching values.

## Principal entities

| Entity | Responsibility |
|---|---|
| scalar program (`ScalarProgram`) | The scalar portion of one bound graph in SSA form: typed values, instructions, and one output value. Carries no register assignment and no state. |
| scalar value (`ScalarValueId`, `ScalarValueType`) | An SSA value, its `ScalarKind`, and whether it can carry a special value at all (`ScalarPresence`). |
| canonical arena (`CanonicalArena`) | The borrowed view of canonical node values, node states, and history that a graph region's members read boundary values from and retain temporal state in. |
| legality predicate (`supports_program`) | The single function deciding whether the scalar engine can execute a program. Pure; no state, no side effects. |

## The shape of a scalar program

A `ScalarProgram` is a flat SSA program with three parts and no nesting: a table of typed `values`, a list of `instructions` in execution order, and one `ScalarValueId` naming the result.

```text
ScalarProgram {
    values:       [ScalarValue],        // every value the program can name
    instructions: [ScalarSsaInstruction],
    output:       ScalarValueId,
}

ScalarValue { ty, canonical_node: Option<NodeId>, definition }
```

### What SSA gives this layer

*Static single assignment* means each value in the table is written once and never reassigned. Construction enforces two rules, and both are checked when a program is built:

- an instruction's result must be a value whose definition is that instruction, so no two instructions can write the same value (`ScalarProgramError::InvalidResult`);
- an operand that names an instruction result must name an *earlier* one, so a program is straight-line with no back edges (`NonDominatingInput`).

Two things follow, and the region scopes on [fusion and regions](fusion.md) depend on both. A value identifier can be used directly as a register slot, because nothing else will ever write it. And "does this value have to reach the arena?" becomes a scan of who reads it, which is exactly how an island's exports are computed: a value read only inside the region stays in a register.

A recursive stream does not break this. `total[1]` does not refer back to the value `total` produces this tick; it is a `Temporal` instruction that reads retained state, and its result is an ordinary value defined before its consumers. The recursion crosses a tick boundary, not an edge in the program, which is why a stream that refers to its own previous output still lowers to straight-line code.

Every value carries where it came from, and there are only four answers:

| `ScalarValueDefinition` | Meaning |
|---|---|
| `Constant(Value)` | A literal, such as the fallback of a `default`. |
| `External(EnvironmentSlot)` | A value read from the current environment row. |
| `CanonicalNode(NodeId)` | A value a canonical node of the enclosing graph produced. Only islands have these, because an island covers a node range rather than a whole graph; a whole-stream program never has one. |
| `Instruction(index)` | The result of an earlier instruction in this same program. |

`ty` is the `ScalarValueType` described above: a kind and a presence.

`canonical_node` is the link back out. Every instruction also records the `NodeId` it was lowered from, which is what lets any tier write results into the canonical arena under the identity the rest of the system already uses.

### A program in full

`total = default(total[1], 0) + merged` lowers to one island covering its three nodes:

```text
values
  v0  Int  MaybeSpecial   node 0   Instruction(0)
  v1  Int  AlwaysPresent           Constant(Int(0))
  v2  Int  MaybeSpecial   node 1   Instruction(1)
  v3  Int  MaybeSpecial            External(slot 4)      // merged
  v4  Int  MaybeSpecial   node 2   Instruction(2)

instructions
  i0  Temporal { result: v0, node: 0, op: RecursiveDelay }
  i1  Temporal { result: v2, node: 1, op: Default { input: v0, fallback: v1 } }
  i2  Binary   { result: v4, node: 2, op: Add, left: v2, right: v3 }

output   v4
exports  node 2
```

The delay and the default are instructions like any other; what stays behind is their retained state, which the canonical arena keeps. `v3` is the only external read, `v1` is the only constant, and only node 2 is exported, because nothing outside the island reads nodes 0 or 1.

Four instruction kinds cover everything the IR admits:

| `ScalarSsaInstruction` | Purpose |
|---|---|
| `Unary` | One typed scalar operand. |
| `Binary` | Two typed scalar operands. |
| `Temporal` | `Delay`, `RecursiveDelay` or `Default`. The result lands in a register; the retained state stays in the canonical evaluator arena. |
| `EagerSelect` | A non-recursive `if`, holding a program per branch. |

## Which consumers admit which instructions

`supports_program` is the single predicate deciding whether the quickened engine can run a program, and it is recursive: an `EagerSelect` is admitted when both of its branch programs are. Native lowering admits the same instruction set. Neither consumer defines its own; they agree on this one.

`EagerSelect` represents a non-recursive `if`, and holds a complete program per branch rather than a jump. What that costs at run time, and how each branch's state is written back, is [tier behaviour](execution-tiers.md#native-execution); what it means for sparse values is [language semantics](language-state.md#conditional-timelines).

## Implementation mapping

- backend-neutral IR, SSA construction, and instruction encoding: `src/dataflow/execution/scalar_ir.rs`;
- `supports_program` and the register-level executor: `src/dataflow/execution/quickening/region.rs`;
- native lowering from the same programs: `src/dataflow/execution/jit/backend/lowering.rs`.

Regions, islands and the partition they form are mapped on [fusion and regions](fusion.md#implementation-mapping).

Continue with [execution tiers](execution-tiers.md) for who owns a region's state while it runs, or return to [runtime ownership](runtime-ownership.md) for why that state is not held by the evaluator.
