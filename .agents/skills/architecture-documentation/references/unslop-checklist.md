# Preservation-first anti-slop checklist

Use this checklist to make technical prose clearer without deleting information that gives the architecture its contract.

## 1. Freeze the facts

Before rewriting, inventory:

- exact component and type names;
- phase and dependency order;
- visibility and temporal rules;
- ownership and mutation rights;
- modal verbs and prohibitions;
- numeric values, units, defaults, and bounds;
- special cases and feature conditions;
- error variants, scope, and retryability;
- lifecycle and reconfiguration boundaries;
- links, citations, and attributed comparisons.

After rewriting, compare against this inventory line by line. A shorter draft that drops a condition is not an improvement.

## 2. Remove low-information framing

Delete or replace:

- generic importance openings: “In modern systems, architecture is crucial”;
- throat-clearing: “It is worth noting that”;
- fake previews: “In the following section, we will explore”;
- repeated recaps that add no new constraint;
- inflated adjectives: “robust,” “powerful,” “seamless,” “comprehensive”;
- vague praise: “This elegant design improves flexibility”;
- empty conclusions: “Overall, this architecture provides a solid foundation.”

Start with the contract, a concrete problem, or the relationship the reader must understand.

## 3. Name actors and actions

Replace vague constructions with accountable ones:

- “The system stores history” → name the owner and commit phase.
- “Errors are handled” → name the error, boundary, and next action.
- “Values are processed” → name the phase and transformation.
- “This allows optimization” → state the guard, cached artifact, or removed work.

Use active voice when responsibility matters. Passive voice is acceptable when the actor is genuinely irrelevant.

## 4. Keep terms stable

Assign one exact term to each concept. Do not alternate among “node,” “operator,” “stage,” and “step” unless they are distinct types. Match public and implementation terminology where possible.

Preserve deliberate repeated terms in:

- phase lists;
- trace tables;
- lifecycle transitions;
- figure labels and captions;
- contrasts such as current/committed or potential/active.

Technical repetition is often orientation, not slop.

## 5. Preserve logical strength

Check every edited modal and qualifier:

| Original force | Do not silently turn it into |
|---|---|
| must | should, usually |
| never | generally does not |
| may | does |
| only after commit | after evaluation |
| bounded by N entries | bounded, small |
| terminal | error |
| previous committed value | previous value |

Keep conditions attached to the claims they constrain.

## 6. Reject unsupported rationale

Code demonstrates behavior and structure, not necessarily motivation. Avoid “to improve performance,” “for simplicity,” or “because the authors wanted” unless a source establishes that rationale.

Use one of these forms instead:

- verified behavior: “The plan stores indices rather than names.”
- measured consequence: “The benchmark shows…” with the measurement cited;
- explicit source: “The ADR selects this layout because…”;
- labeled inference: “This layout may reduce lookup work; the repository does not record that rationale.”

## 7. Remove maintainer-guide drift

Architecture prose states current facts and invariants. Remove or relocate:

- “when changing” and “when adding” recipes;
- option-selection or design-choice tables for future work;
- code-review checklists;
- generalized implementation advice;
- speculative extension guidance;
- recommendations not expressed by the current system.

Keep factual comparisons of implemented modes and their consequences. Keep explicit constraints such as stable identity, legal transition order, and terminal failure behavior.

## 8. Tighten examples and captions

- Keep one running example within a conceptual sequence rather than introducing decorative variants.
- Permit a labeled companion fixture when a focused mechanism requires it.
- Make every example expose a named semantic rule.
- Keep assertions focused on the documented contract.
- Give each table a reading rule.
- Make captions explain direction, time, ownership, dashed-line meaning, or another interpretation needed to read the figure.

## 9. Check progressive depth

- Does the page answer one architectural question?
- Are responsibilities introduced before internal names?
- Does each detail belong at this page's abstraction level?
- Can deeper facts move to a linked page without weakening this page's answer?
- Does a landing page remain coherent without the implementation map?
- Does a focused page preserve rather than redefine the higher-level contract?

## 10. Final read

Ask:

- Can a reader predict one execution?
- Can a reader tell what persists into the next execution?
- Is every ordering statement unambiguous?
- Are failure and growth policies concrete?
- Are semantics separated from optimization?
- Does every paragraph add a fact, relationship, example, limitation, or source?
- Did the edit retain every item in the frozen fact inventory, either on this page or at its intentional deeper destination?
- Did the page remain architecture explanation rather than maintainer procedure?

Do not optimize prose for AI-detector evasion. Optimize it for technical fidelity, directness, and verification.
