# ScrewDetection observation properties

Runnable paper specifications for the ScrewDetection MAPLE-K loop, over the
native reference journal: callback records name immutable versions held in a separate Knowledge history.

| File | Content |
|---|---|
| `observation_schema.json` | The application's observation schema: each Knowledge key's fields, and the fields of the messages its callbacks send and receive, in DSRV type syntax. |
| `declarations.dsrv` | Generated from `observation_schema.json`; do not edit. The Knowledge types and union, `Message`, `Trigger` and one accessor per key. |
| `v2_adapter.dsrv`, `observation_clock.dsrv` | Convert native callback facts to the shared property view, with named application evidence slots and the capture’s physical clock label. |
| `schema.dsrv` | Property-facing identities: `JournalTime`, activation `Owner` and timing `Key`. |
| `evidence.dsrv` | The typed `Tick` the roots read, projected from the one journal coordinator (`rpio::journal::state`) and the shared MAPLE-K publication machines, with the application's protocol checks: the evidence slots each publication must cite and the publications each End outcome requires. Facts are values, never verdicts. |
| `sahar.dsrv` | Sahar's P1–P15 properties. |
| `strengthened.dsrv` | The strengthened properties, including the ten MF-03 responsibility budgets. |
| `pv-a.dsrv`, `pv-b.dsrv` | The journal's shared premises and the publication view, composed (`rpio::journal::view::maplek::observe`) and from separate components. Both give identical outputs. |

The runnable roots preserve the finding families of Sahar's P1–P15 and the
strengthened service and evidence obligations. Their current boundaries are:

- the request boundary is the application's external request: a Monitor
  Start on topic `sensor_data_received` that no publication woke.
  `Capture.request` exposes the recorded request message, including its
  `detection_type`; comparing it with the frame is a property's choice, and
  nothing here claims the frame is what was asked for;
- a `Detection` is causally matched when its acquired `FrameRef` tuple
  (version, value and writer) is exactly the one the waking observation cited;
- a Start of a role other than Monitor with no waking emission (an
  unsolicited Start, such as an Execute woken by a `plan_validated` no
  Legitimate published) breaks the sensing or adaptation protocol
  (`protocol.sensing_ok` / `adaptation_ok`). It is a deployment protocol
  fact, not a journal-integrity finding;
- a publication without the evidence slots its role and topic require, or an
  End outcome field naming a version of another key than it requires
  (`protocol.missing_evidence`), makes the journal invalid from then on, as a
  coordinator finding does; a successful End whose activation did not publish
  what its outcome requires is `protocol.missing_publication`;
- every response deadline is one `std::timing::deadline` call, which reads
  the Tick's `clock` (the held "now" and the record's own instant).

## Recovery assessment after an execution

Each root keeps one pending assessment of an applied model swap, and every
applied execution starts it afresh. In `strengthened.dsrv`, `plan_executed`
with an effect model opens a recovery that counts fresh samples: the first
cycle completion of an adaptive Analysis whose detection was written after
the execution’s command version, by the deployed model, with entropy in [0, 1]. An outer
Analysis that started before the swap and completes after it does not count.
At the tenth or a later fresh sample, once a full, coherent window written after
the execution is available, the assessment closes: `unresolved_degradation` if that window's
average is still above the threshold, otherwise none. Until then
`recovery_pending` stays true. In `sahar.dsrv`, `assessing` opens at
`plan_executed` and closes at the first full window that follows it, where
`ineffective` or `effect_inconclusive` may be reported.

A second applied execution before the assessment closes replaces it: the
earlier swap then never receives a verdict, and only the new one is assessed.
An applied execution with no effect model clears the pending assessment and
opens no new one.
For example, a swap followed by nine fresh degraded samples and then another
swap before the tenth leaves `recovery_pending` true and reports no
`unresolved_degradation` for the first swap. This is the policy of the
historical strengthened specification and of the accepted preview; keeping
several overlapping assessments would need new bounded state and is not done
here. `tests/screw_detection_roots.rs` checks the tenth-sample closure, the
excluded outer cycle, a coherent degraded window and the retired recovery
boundaries on a fragment of an actual long capture.

## Retained state

Nothing is capped. The coordinator keeps the callbacks in flight, each
publication on a consumed topic (`evidence::consumed`: `observation_recorded`,
`anomaly_detected`, `plan_rejected`, `plan_generated`, `plan_validated`)
until the first Start it wakes, and the latest write of every key; a
publication on any other topic, such as `cycle_completed`, is never kept.
Each deadline keeps its open waits. Other root
state is constant size. `startup_incomplete` reports a trial whose first
workload Start comes before the four loop-state keys and the active model
were written.

## Regenerate the declarations

```sh
cargo run --bin generate_rpio_declarations -- \
  examples/screw-detection/observation_schema.json \
  --output examples/screw-detection/declarations.dsrv
```

`--check` instead fails if the file differs from what would be generated.

## Run over a capture

A native capture uses `rpio-observation-clock-file` even when it has no
independent clock samples. The root input is `journal_input`. Freeze the file’s
SHA-256, physical record count and producer clock label in `capture_checks`:

```json5
{ sources: { journal: {
    kind: "rpio-observation-clock-file", variable: "journal_input",
    journal_clock: "redis.server.time",
    files: ["capture.jsonl"],
    capture_checks: [{ records: 1755, sha256: "<SHA-256 of capture bytes>",
                      journal_clock: "redis.server.time" }],
    clock_samples: [{ time: 1791767048.0, through: 1755 }]
} } }
```

`through` counts physical native records, independently of a Knowledge
version’s `global_write_sequence`. Set `observation_clock.dsrv`’s `label()`
to the same physical clock. Omit `clock_samples` for record-only replay.
Each native record remains a source tick, including immutable Knowledge
commits; additional samples are separate ticks unless explicitly combined.

```sh
cargo run --bin trustworthiness_checker -- examples/screw-detection/sahar.dsrv \
  --input-config trial.json5 --output-stdout --runtime dataflow \
  --execution-policy synchronous
```

The roots use lazy conditionals supported by `dataflow`; `semi-sync` rejects
them. Synchronous execution preserves the outputs already established before
an input error. A failed or terminated run is not a complete trial.

Live input uses `rpio-observation-clock`, with separate `phase_stream` and
`knowledge_stream` names. `clock_period_ms` is optional; omitting it admits
records without synthetic samples. Enabling it samples Redis server time and
requires `journal_clock: "redis.server.time"`. The source drains completed,
covered snapshots on shutdown and reports incomplete fetched evidence as an
error. Redis stream IDs are opaque cursors, so completeness must be checked
against the captured stream boundaries rather than a globally numbered gap.

The integration suite keeps the historical inline-evidence captures unchanged.
Its explicit test adapter emits immutable Knowledge definitions and callback
references, retaining a physical-line-to-archived-fact map for scenario
assertions. Contradictory inline copies become conflicting immutable definitions
and are input errors. Removing or shortening a capture is detected against its
frozen byte hash and record count before evaluation. The source decoder suite
also tests captures written directly by the current producer.
