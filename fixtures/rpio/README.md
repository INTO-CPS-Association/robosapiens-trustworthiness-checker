# RPIO observation journal

Hand-written journals for the wire decoder (`src/io/rpio/wire.rs`), read by
`tests/rpio_wire.rs`. They are the shape a producer writes, not a capture:
nothing generates them, so they are edited by hand and kept small enough to
read.

Both files are JSON Lines: one record per line, in the order a producer would
write them. A journal carries two streams in one file, and each line says
which it belongs to by its own tag — a Knowledge record is tagged
`"Knowledge"`, and every other record is a phase record. The decoder has an
entry point for each stream (`knowledge_record`, `phase_record`), so a reader
of the file dispatches on that tag. Every record carries
`"schema_version": 2` beside its tag: only this version of the value schema
is read.

## `nominal.jsonl`

One complete adaptation of the screw-detection robot, as a single loop
`["robot1", "screw_detection"]` of run `run-2026-09-20-0915` ran it:

- **Monitor** (`a1`) is woken from outside the loop by `SensorDataReceived`,
  reads the entropy window (`EntropyHistory`, write 11), writes the frame it
  observed (`FrameRef`, write 12), publishes `ObservationRecorded` as its
  first emission, and ends `Monitored`.
- **Analysis** (`a2`), woken by that emission, reads both versions, writes the
  running average (`RunningAvgEntropy`, write 13), publishes
  `AnomalyDetected` and ends `Analysed::Anomaly`.
- **Plan** (`a3`) writes a `CandidateModel` (write 14), publishes
  `PlanGenerated` and ends `Planned::Generated`.
- **Legitimate** (`a4`) writes a `LegitResult` (write 15), publishes
  `PlanValidated` and ends `Legitimised::Accepted`.
- **Execute** (`a5`) calls the managed system (`swap_model`, delivered),
  records the robot's acknowledgement as an `ActionCommand` (write 16), and
  ends `Executed::Applied`. It publishes nothing: the cycle is over.

Each Knowledge record follows the phase record that wrote it, so the value
appears where the write happened. Every version a phase record names is
written somewhere in the file, and the file writes nothing the phases do not
name; the test holds the journal to that.

## `rejected.jsonl`

Records that must be refused, one per line, each preceded by a `//` line
giving the error expected and, between backticks, what its message has to
name:

```
// EmptyIdentity `activation_id` — why this record is malformed.
{"$tag":"Phase", …}
```

Each record is read on its own, by a decoder that has seen nothing else, so a
rejection never depends on an earlier line. The reasons covered are an empty
identity, an event whose name and payload tag disagree, an outcome of another
phase, an emission ordinal with a gap before it, an emission named but never
published, an emission published under another activation's identity, a
Knowledge value filed under a key its tag does not match, a role that is not
one of the five, and a journal written to another value schema.

A reason that only shows itself across two records — a Start whose trigger
names a different event from the emission that woke it — belongs in a journal
of its own rather than here, since these records are read one at a time.

## `service-rearm.jsonl`

A focused service-identity story for the shipped timing library. Execute
activation A starts `req-a` and ends before completion; activation B then
starts `req-b`. A late completion for A precedes B's matching completion. The
trace has no Knowledge references because it tests owner retirement, re-arm,
and exact activation/request matching rather than application outcomes.

## `service-pending.jsonl`

A two-record prefix that leaves `swap_model` request `req-a` pending. The
presence-safe observation/clock replay test follows it with clock-only ticks,
proving that a watchdog span advances without replaying the last phase record.

## `handoff-terminals.jsonl`

Two selected Plan-to-Legitimate transfers exercise the non-sequential handoff
terminals. In the first, the causally linked Legitimate activation starts
before Plan ends, producing `Overlap`; the later Plan End must not reopen the
handoff. In the second, the domain adapter supplies explicit withdrawal on the
Plan End, producing `Withdrawn` without fabricating a successor Start.

## Extending these by hand

- Keep a journal readable in one sitting. Extend the nominal file only where
  the extra records say something a reader cannot already see, and start a
  further file for a story of its own — a rejected plan, an abort, two loops.
- The record shapes are declared in
  `trustworthiness_specs/preview/lib/rpio/journal.dsrv`, the application's
  Knowledge keys and events in `.../preview/knowledge.dsrv`, and the wire
  decisions in the journal wire notes beside them. The tests in
  `src/io/rpio/wire.rs` show each shape as JSON.
- A value is tagged by the Knowledge key it is written under, an optional
  field is `Some`/`None` rather than `null`, and tags are CamelCase. An
  absent `caused_by` is `{"$tag":"None"}` and a record naming no version is
  `"knowledge":{"$tag":"Nothing"}`; neither may be left out.
- An event is written twice over: its payload is the application's event
  struct, whose `detail` carries the CamelCase tag, and `name` is that tag in
  snake_case (`ObservationRecorded` and `observation_recorded`). A Start's
  trigger is tagged with the event of the emission that woke it.
- When adding an activation, give it the next `activation_id`, name the
  emission that woke it in `caused_by`, and make sure the activation that
  published that emission appears earlier in the file: the decoder reads a
  journal in order and refuses an emission it has not seen published.
- Number a new Knowledge version with the next `global_write_sequence`, add
  its Knowledge record, and name it from the phase record that wrote it;
  `tests/rpio_wire.rs` fails if a named version is never written.
- Adding a line to `rejected.jsonl` means adding its reason line as well, and
  the expected error's name to the list the test asserts.
