# MAPLE-K loop properties with the resolved journal view

`rpio::journal::view::maplek` helps you write DSRV properties about an observed MAPLE-K loop:
which phase is active, whether an anomaly leads to a validated and executed
plan, which Knowledge values informed a decision, and how long adaptation
steps take. You supply an RPIO observation journal and application-specific
properties; the library reconstructs cycles and adaptation episodes and
exposes the evidence those properties need.

For a worked example across these property families, follow the
[TurtleBot MAPLE-K tutorial](../tutorials/turtlebot-maplek.md).

The experimental library’s local stream functions run with
`dataflow` and `semi-sync`. Use `typed-untimed` or `gradual-typed-untimed`
semantics for the examples below. See
[DSRV language settings](../reference/dsrv-language-settings.md) for the
experiment declarations and [runtime capabilities](../reference/runtime-capabilities.md)
for runtime restrictions.

## Choose the evidence for your property

A **cycle** begins at a Monitor activation. It includes the subsequent phases
caused by that activation's chain of emissions. An **adaptation episode**
begins when Analysis ends with an anomaly. It can contain several proposed
plans if Legitimate rejects a plan and causes replanning. A nominal analysis
can therefore finish a cycle without opening an adaptation episode.

| Property you want to express | Library feature | What your property must supply |
|---|---|---|
| Phases follow the loop's causal order. | `loop.cycle`, `cycle_step`, `protocol_followed` | How to handle a record outside the reconstructed protocol. |
| An anomaly progresses through planning, validation, and execution. | `loop.episode`, `episode_step` | Which closures are acceptable, and whether progress has a deadline. |
| Only the accepted plan is applied. | Episode transitions match causal activations and plan version references. | Whether the reported acceptance and effect meet application requirements. |
| Rejected plans cause another planning attempt. | `Rejected` transitions clear the current plan and retain the rejecting activation. | A retry limit or a requirement to eventually find a plan. |
| A phase used appropriate Knowledge. | `last_access`, `named_version`, `named_value` | A projection of the relevant key and a predicate on its value or version. |
| The latest observed value of a Knowledge key meets a condition. | `latest` | One key to select and the condition to check. |
| Planning, validation, execution, or the whole episode finishes within a budget. | `loop.timing` | Budget values and treatment of unfinished episodes. |
| A callback or service request finishes promptly. | `rpio::journal::view::timing::activation_timing`, `service_timing` | Role or service selection, budget, and acceptable ending. |
| A phase hands work to the intended successor promptly. | `rpio::journal::view::timing::handoff` | The emission that transfers work, the successor role, and any withdrawal evidence. |
| A pending obligation becomes overdue during journal silence. | Journal timing helpers over clock samples | Clock samples in the journal's time domain, and a budget. |

The callback, service, and handoff helpers belong to the companion
`rpio::journal::view::timing` module. They can be combined with MAPLE-K state, but
measure different obligations from the episode's outcome spans.

Knowledge views (`last_access`, `latest`, `Access`, `selected`,
`supersedes`, `named_version`, `named_value`, and `is_access_by`)
belong to `rpio::journal::view::knowledge`. Record projections (`is_phase`,
`position`, `arrival`, `phase_activation`, `identity`, `is_start_of`,
`sent`, `published`, `received`, `end_outcome`, `outcome_evidence`,
`accessed`, `written`, `slot`, `woken_by` and `caused_by`) belong to
`rpio::journal::view`.
`tick` and `TickResult` belong to `rpio::journal::view::timing`. `rpio::journal::view::maplek`
owns the cycle and episode evidence derived from callback End outcomes.

## What the journal must tell you

The helpers below read the resolved property view,
`rpio::journal::view::Record<K, E, M>`. Its `position` is a local admission
position, separate from a Knowledge version’s write sequence. An activation
retains the producer’s run identity and opaque callback token; the compatibility
field `start_seq` stores that token as a string. Its causal emission identifies
the callback and emission ordinal that woke it.

The native journal carries callback records and immutable Knowledge definitions
on separate streams. Callback records name exact version references. The source
joins those references to the recorded value and writer, and validates the whole
application type before admission. An application adapter then selects trigger
meaning, message fields and named evidence slots for the shared view. For
example, the [TurtleBot tutorial](../tutorials/turtlebot-maplek.md) wires a native
input through `native_adapter.dsrv`; ScrewDetection uses `v2_adapter.dsrv`.
The property-view input must not be wired directly to a native source.

In the view, Read and Write steps expose the resolved versions they acquired or
committed, and End outcomes retain the versions they name. Event evidence uses
named slots. Start exposes the received topic and message; Event exposes the
published topic, message and per-callback emission ordinal. Immutable
Activation-owned Knowledge notices remain visible, while their matching Phase
Write supplies the callback context used by commit-based properties.

Every helper reads an observation containing an optional timed record and an
optional clock sample. The application declares the time domain:

```dsrv
use experimental::{newtypes}
use rpio::journal::view::timing
newtype JournalTime = Unit
type PropertyObservation = timing::Observation<Knowledge, Message, Trigger, JournalTime>
```

`observation` in the snippets below is the adapter’s output of that type. See
[RPIO observation sources](../reference/input-configuration.md#rpio-observation-journals)
for native input configuration and frozen capture checks. The declaration
generator supplies the application’s Knowledge, message and trigger types; it
does not choose the adapter’s evidence slots.

Every time the library reports is a `std::time::Instant<JournalTime>`, which
cannot be compared with an instant of another domain. The source takes `D`
from this declaration, so its configuration need not name it. The pure record
views below take a `Record`; apply them to
`rpio::journal::view::timing::record_of(observation)`, the current record if there
is one.

The five roles are `Monitor`, `Analysis`, `Plan`, `Legitimate`, and `Execute`.
The journal records Start, Read, Write, Event, service, and End steps. An End
reports the phase's outcome, such as an anomaly, a generated plan, a rejected
plan, or an applied effect.

Cycle and episode membership uses the activation that emitted the event which
woke the next activation (`caused_by`). Plan matching additionally uses
Knowledge version references. Matching timestamps or similar payloads do not
establish membership. The library reasons about recorded evidence; an
`Applied` outcome alone does not independently establish that the physical
system changed as intended.

Use one fresh, ordered run of one sequential loop per `loop` instance.
`loop` does not partition records by loop path or reset itself on a new run.
Its state does not represent overlapping cycles or concurrent episodes.
Select the appropriate records before monitoring multiple loops or runs.
`tick.new_run` identifies the start of the helper instance; it does not partition successive producer runs or reset its state.

Knowledge notices do not advance the cycle or episode. A clock-only tick
carries no record and leaves those states unchanged. Every record
and clock sample does move "now", the time against which pending spans age.
Absence is represented by `None`; it is not evidence that a property passed.

## Phase order and causal continuity

Instantiate `rpio::journal::view::maplek::loop(observation)` to obtain both cycle and
episode state. Each instance has its own history. `cycle_before` and
`episode_before` describe state before the current tick; `cycle` and `episode`
describe state after it.

The cycle starts with Monitor Start. Later Starts must be caused by the
currently held role activation and follow this order:

```text
Monitor → Analysis → Plan → Legitimate → Execute
                             ↑     |
                             └─────┘  replanning
```

The cycle permits Legitimate to be followed by Plan. The episode layer checks
the rejection and plan evidence needed to interpret that return as replanning.
The cycle closes when its current activation ends with nominal analysis,
execution, no available plan, or an abort.

`cycle_step` is `Stayed`, `Moved(new_cycle)`, or `OutOfProtocol(...)`.
An unrecognised Start or an End from another activation is reported as
out of protocol and leaves the held cycle state unchanged. Many other records
produce `Stayed`; this is not a claim that every journal record has passed a
complete protocol validation.

`loop.protocol_followed` is false when either the cycle or episode layer reports
an out-of-protocol transition on the current tick, and true otherwise. A property
can expose it directly or retain failures in a run-level result. The flag is a
per-tick observation, not a claim that every aspect of the protocol has been
validated. The library assigns no severity or recovery policy.

## Adaptation progress and plan integrity

The episode state distinguishes an idle loop, an anomaly waiting for a plan,
a generated plan waiting for legitimation, and an accepted plan waiting for
execution. These distinctions let you ask different questions at each point:

| Recorded outcome | Episode effect | Property enabled by the evidence |
|---|---|---|
| `Analysed(Anomaly(...))` while idle | Opens an episode identified by the Analysis activation. | Does the anomaly evidence justify adaptation? |
| `Planned(Generated(...))` from an activation within the episode | Retains the plan and its producing activation. | Does the proposed plan satisfy application constraints? |
| `Legitimised(Rejected(...))` caused by that plan activation and naming its version | Clears the plan and records the rejecting activation. | How many plans have been rejected, and is replanning making progress? |
| `Legitimised(Accepted(...))` with the same causal and version match | Retains acceptance and its Knowledge result. | Is the acceptance evidence sufficient for execution? |
| `Executed(...)` caused by the accepting activation and naming the accepted plan version | Closes the episode. | Was the accepted plan applied, and was the effect acceptable? |

`episode_step` exposes `Moved { from, by, to }` for progress, including the
new evidence in `by`. Its transition tags are `Opened`, `Generated`,
`Rejected`, and `Accepted`. A property can use `from` to relate the current
evidence to the plan or anomaly already held by the episode.

Closing produces `Closed { from, how, at }`. The closure distinguishes
`Applied`, `NotApplied`, `NoPlan`, and `Aborted`. Consequently,
`episode_closed` and “adaptation succeeded” are different properties: your
spec must decide which closures and effects count as success. A mismatched
outcome produces `OutOfProtocol` and preserves the episode state.

For example, an anomaly followed by a generated plan, rejection, a replacement
plan, acceptance, and application is one episode with two plan attempts.
Counting `Generated` or `Rejected` transitions can support a retry-budget
property. No retry limit or eventual-success requirement is built into
`loop`.

## Progress through selected publications

`rpio::journal::view::maplek::events::sensing` and `adaptation` track milestones selected
from application publications. The spec supplies static selectors to identify
a request, classify publications, say which successful callback outcomes owe
a publication, and select failure closures. Instantiate each stream function
unconditionally: each instance owns its journal-tick history.

The ScrewDetection spec uses the selectors as follows; their definitions stay
in the application, which knows its event payloads and callback outcomes.
`published_milestone` classifies both sensing and adaptation publications;
for example, ScrewDetection's `DetectionCompleted` maps to the library's
`Milestone::Analysis`:

```dsrv
aux sensor = rpio::journal::view::maplek::events::sensing::<
  is_sensor_request, published_milestone, required_publication
>(observation)
aux adaptation = rpio::journal::view::maplek::events::adaptation::<
  published_milestone, required_publication, ends_adaptation
>(observation)
```

`sensing` exposes `state`, `timing.observation` and `timing.analysis`, `observed`,
`analysed`, `missing_publication` and `overlapping_request`. It retires a
pending request on the matching owner's End, and reports a missing publication
when that outcome owes the pending milestone. `adaptation` exposes `state`,
plan/validation/execution/episode `timing`, `executed`, `missing_publication`
and `protocol_followed`. It closes on a causally matched execution or abort
publication, or an End selected by `ends_adaptation`; rejection returns to
planning while retaining the episode timer. A successful End owing a missing
proposal or execution reports that absence while progress remains pending.

These milestones are separate from `loop`'s callback End outcomes. A successful
End does not establish that its required event was published. Conversely, a
publication can advance an event obligation before its callback ends. A
matching selected failure End can close the corresponding adaptation wait without claiming
a successful publication or adaptation.

Publication causality uses the full emission reference, including its
sequence number, so two events emitted by one activation remain distinct.
Callback progression uses activation identities (run and opaque callback token) and
the activation's parent link. Neither
matching times nor matching payloads substitutes for those identities. These
views retain pending evidence across incomplete observations; EOF supplies no
success verdict. They add reusable evidence tracking, not new application
requirements or evaluated fault classes.

## Knowledge use and decision evidence

The library reads the exact Knowledge evidence records carry. It does not
treat an unrelated Knowledge write as evidence that a phase used the new
value.

`last_access::<role, access, project>(observation)` retains the last selected
version read or written by a role. `access` is `Access::Read` or
`Access::Write`, and `project` maps your Knowledge union to `Option<T>`. The
result is an `Option<Resolved<T>>`: the version reference `ref`, the selected
`value`, and `written_by`, as `latest` reports them. A Start of that
role clears the retained version; a matching access with an accepted
projection replaces it.
For example, retain Analysis's last entropy-window read to compare it with
the evidence used to report an anomaly. The function assumes one activation
at a time per role and does not itself prove that a required read occurred.

`latest::<project>(observation)` retains the newest selected value any
record names: a Read's selection, a Write's commit, or a write outside any
activation. Its `KnowledgeState<T>` has `latest: Option<Resolved<T>>`
and `updated: Bool`; evidence retains its value, version and writer.
`updated` is true on a replacement tick,
including the first accepted value. Within one store epoch, a larger global
write sequence supersedes the held version. A different store epoch also
supersedes it. Your projection must select exactly one Knowledge key so that
the retained versions refer to the same thing.

Both stream functions follow Reads, Writes and Knowledge records only. An
Event or End citing a plan and a legitimation result should be inspected
through its outcome or `slot(record, name)` rather than through these
single-key helpers.

For these Knowledge views, import the names explicitly:

```dsrv
use rpio::journal::view::knowledge::{Access, last_access, latest}
use rpio::journal::view::timing::{tick}
```

Pure record views support more focused properties:

| View | Useful question |
|---|---|
| `is_phase`, `phase_activation`, `is_start_of` | Is this evidence from the role or activation I am checking? |
| `is_access_by` | Is the current record a read or write by that role? |
| `named_version`, `named_value` | Which single resolved version or value did this phase name? |
| `sent`, `received` | What was published, or what message started this activation? |
| `end_outcome` | What did this activation report when it ended? |
| `arrival` | When did this record arrive in the journal? |
| `woken_by`, `supersedes` | Does this activation name the required causal predecessor, or this version supersede the held one? |

`tick(observation)` additionally exposes a phase-record flag, the current
record's `record_time` (`None` on a tick without a record), the latest phase
run, a run-change flag, and the previous phase record's time. It can help
properties identify run boundaries. Phase times are admitted as recorded,
even when one runs backwards:
`std::time::is_backwards(std::time::between(previous, current))` detects
that.

## Timing properties: choose the right interval

`loop` exposes `Option<TimeInterval<D>>` values from `std::time`, where an
interval has `start` and `end` instants in seconds of the domain `D`. It does
not choose budgets or start a clock for you.

| Span | Begins when | Measures until the record that ends it, or "now" |
|---|---|---|
| `role_span` | The held role activation started. | Time in that role, including the wait until a successor starts. |
| `timing.plan` | An anomaly opened the episode, or a plan was rejected. | Time waiting for a generated plan or another outcome while no plan is held. |
| `timing.validation` | A plan was generated. | Time waiting for acceptance or rejection. |
| `timing.execution` | A plan was accepted. | Time waiting for an execution outcome. |
| `timing.episode` | The anomaly opened the episode. | Total adaptation time, including replanning. |

Spans describe the state **before** the current tick. The record completing
a stage still exposes that stage's span, which ends at that record's own
time. The record opening a new stage does not immediately expose its new span.
On any other tick, the span is the stage's age: it runs to "now", described
[below](#detecting-overdue-work-during-silence). Knowledge records age spans
like any other record while retaining the cycle and episode state.

For example, this illustrative fragment checks a two-second planning budget
after an existing `aux loop = rpio::journal::view::maplek::loop(observation)` declaration
over the `JournalTime` domain:

```dsrv
use std::time::{Duration}
const planning_budget: Duration<JournalTime> = std::time::seconds(2.0)
// Planning finishes within its budget.
out planning_on_time: Bool = !std::time::exceeds(loop.timing.plan, planning_budget)
```

Name a Boolean output for the requirement it checks, and make it true while
that requirement holds, so that `false` always marks a violation.

A budget is a `Duration` in the same domain as the span, so a budget written
for another clock is a type error. `exceeds` uses a strict `>` comparison and
returns false for `None`, as `shorter_than` does with `<`. Both hold only on a
forward interval: an interval whose end precedes its start, which
`std::time::is_backwards` reports, satisfies neither, and its signed
`std::time::duration` is negative. Combine them with state and evidence
checks if missing timing information should prevent a passing verdict.

For a numeric seconds limit, `std::time::exceeds_seconds(span, 0.1)` infers
the time domain from the span. It uses the same strict, forward-only comparison
as `exceeds`. The ScrewDetection spec uses this form for its verdict-selected
MF-03-07/08 intervals; a clock tick alone supplies no verdict to judge.

A completed duration within budget also does not establish a successful plan or
application outcome.

### Callback duration, service completion, and handoff

Use `rpio::journal::view::timing` when the required interval has journal lifecycle
boundaries rather than MAPLE-K outcome boundaries:

- `activation_timing::<role>` tracks one matching Start-to-End activation.
  It returns `span`, `pending`, `ended`, and `overlapping_start`. An aborted
  activation can still end promptly; `ended` does not mean successful.
- `service_timing::<service>` tracks one request for that service name,
  matching both its owning activation and request identity. On the tick a
  request ends, `ended` is a `ServiceEnd`: `Completed(Delivered)`,
  `Completed(NotDelivered)`, or `Abandoned` when the owning callback ended
  without completing it. A service-delivery outcome does not establish
  that the adaptation's intended effect occurred.
- `handoff` tracks a transfer selected by your spec: predecessor activation,
  expected successor role, and exact waking emission. It distinguishes a
  completed End-to-Start `Gap(span)`, a successor starting before the
  predecessor ends (`Overlap`), and explicit `Withdrawn(reason)` evidence.
  An arbitrary End does not create a transfer. A pending wait span alone does
  not prove that the successor started.

Each helper holds one obligation at a time. An overlapping opener is reported
without replacing the obligation already held. When occurrences can overlap,
or a deadline should be reported once per occurrence, key each occurrence and
use [`std::timing::bounded_response`](../reference/dsrv-syntax.md#bounded-response)
instead. Compare how each obligation ended, and the overlap and conflict
flags, as well as elapsed time when expressing loop correctness.

### Detecting overdue work during silence

The stream functions measure ages from "now", which
`rpio::journal::view::timing::now(observation)` also exposes. "Now" is the latest
instant any record or clock sample has shown, held between them, so it never
goes back. A source releases a clock sample only after the records it covers,
so "now" never runs ahead of unread evidence. A clock-only tick therefore ages
a pending obligation without replaying the last record.

A tick can carry a record and a clock sample at different instants, when a
schedule makes them simultaneous. It is still one tick. "Now" is the later of
the two, and an interval the record closes ends at the record's own time.
A phase record admitted earlier than "now" is likewise kept at its own time
and does not move "now" back; `tick` reports the backwards phase time.

| Tick | Record | Clock | "now" | Pending Plan activation, opened at 0 |
|---|---|---|---|---|
| 1 | Plan Start at 0 | | 0 | opened |
| 2 | | 13 | 13 | age 13 |
| 3 | Plan End at 11 | 14 | 14 | ended: interval [0, 11] |
| 4 | Knowledge at 20 | | 20 | nothing pending |

This supports properties such as “a pending service request has exceeded its
budget even though no further journal record arrived.” Use the presence-safe
[RPIO observation sources](../reference/input-configuration.md#rpio-observation-journals)
for this purpose, with clock samples in the journal's time domain. `loop`'s
outcome spans age on clock-only ticks in the same way. End of input supplies
no automatic timeout or final success verdict.

## Try the cycle and episode observations

The [TurtleBot tutorial](../tutorials/turtlebot-maplek.md) observes one
adaptation from anomaly through plan acceptance to application. From the
repository root, with Rust/Cargo installed, replay its accepted journal.
Default Cargo features suffice; no broker or ROS installation is needed:

```sh
cargo run --bin trustworthiness_checker -- \
  examples/turtlebot-maplek/properties.dsrv \
  --input-config examples/turtlebot-maplek/accepted-input.json5 \
  --output-stdout --runtime dataflow --semantics typed-untimed
```

The input configuration names the tutorial's 26-record journal and the
declared interval `after: 0, through: 26`. Selected stdout
lines show the episode opening at Analysis's anomaly End (tick 9) and closing
at Execute's applied End (tick 25):

```text
adaptation_in_progress[8] = false
adaptation_in_progress[9] = true
adaptation_in_progress[25] = false
plans_applied[25] = 1
adaptation_requirements_held[25] = true
```

## The journal coordinator and a complete paper specification

`rpio::journal::state::track` is the one historical owner of a journal. It
checks what records claim about each other, with no deployment contract:
activation lifecycle (a record of an activation that never started, a
record restating its activation otherwise, an outcome its role cannot end
with), causality (a Start woken by a publication that is not
pending, on another topic than the publication's, and emission numbering),
and that the evidence an Event or End cites is evidence its activation
acquired (every Read that found a value and every Write; identical tuples are
kept once). It keeps the activations in flight (each leaves at its End), the
publications on topics the application's `consumed` hook names (each leaves
at the first Start it wakes; a publication on any other topic is never kept)
and the latest write of every Knowledge key. Every tick exports the raw
record, the current activation's entry with the publication that woke it,
the publication an Event created, the latest writes, this tick's finding (the
first that applies), a sticky `invalid` flag with the run's first diagnostic, and `unsolicited`: a Start of a role other than Monitor that no
publication woke. A record that breaks these checks, or carries values a
domain property rejects, is still exported. Which evidence a publication must
cite and which publications an outcome requires are the application's own
checks, written in its specification.

`generate_rpio_declarations observation_schema.json --output declarations.dsrv`
translates an application's observation schema into the DSRV a
specification imports: one open struct per Knowledge key, the `Knowledge`
union, the `Message` and `Trigger` structs of sent and received messages, and
an accessor per key. Field types are DSRV type syntax, copied verbatim. It
infers nothing from traffic and generates no domain predicate.

`rpio::journal::view::maplek::observe(observation, journal, slots)` reads the coordinator's
output to give the publication-driven view: sensing and adaptation progress
from recorded publications, the accepted and executed plans paired on an
execution publication, and callback outcomes separately. `publications` and
`callback_outcomes` are its components. The paper application roots use this
view after adapting native callback records. The separate classic `rpio::maplek` API remains available for direct
native models; its record shapes and selectors differ from the view.

`std::timing::deadline(open, close, cancel, budget, clock)` is a response
deadline for each opened key. It reports a late closing response as well as
a silence breach, each once, and counts the waits still open; `clock` holds
"now" and this tick's record instant. The general
[`bounded_response`](../reference/dsrv-syntax.md#bounded-response) also
reports which keys breached and which closed.

`examples/screw-detection/` is a complete application of these pieces:
declarations generated from the ScrewDetection observation schema, a typed
`Tick` projection with the application's protocol checks (`evidence.dsrv`),
Sahar's P1–P15 properties (`sahar.dsrv`), the strengthened properties with
their responsibility budgets (`strengthened.dsrv`), and the publication view
composed (`pv-a.dsrv`) and from separate components (`pv-b.dsrv`), which give
identical outputs.

`rpio::journal::view::timing` distinguishes three instants:

| Query | Meaning on this tick |
|---|---|
| `record_time(observation)` | The current record's own instant, including a Knowledge record's; absent on a clock-only tick. |
| `phase_time(observation)` | The current Phase record's instant; absent for other records and clock-only ticks. |
| `now(observation)` | The nonregressing record/clock instant; present on every tick with a record or sample. |

`tick.record_time` matches the first query. The wire field remains
`Arrival.journal_time`. `previous_phase_time` retains phase history.

`knowledge::value(evidence)` and `knowledge::version(evidence)` project an
optional resolved value or version. Bind a projected value once when several
properties reuse it. MAPLE-K's `generated_plan`, `validation_result` and
`applied_effect` select evidence from this tick's step; `pending_plan`,
`accepted_plan` and `accepted_validation` inspect the supplied episode state.
An absent matching transition or state returns `None`.

`std::stream::hold_when(initial, update, value)` retains a value until an
update. `count_when(reset, increment)` returns the count after the tick;
reset wins. `recent::<capacity>(sample, reset)` returns `before` and `after`
lists, newest first, retaining repeated occurrences. Reset wins over a sample;
nonpositive capacity retains no entries. These helpers add no domain resets.

For a Boolean deadline property, `std::timing::response_overdue` accepts the
same six arguments as `bounded_response` and reports each breached obligation
once. Use the rich result when pending or closing evidence is needed.
