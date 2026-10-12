# Producer reference journal

`nominal.jsonl` is copied unchanged from the RPIO repository
(`tests/fixtures/journal-v4/nominal.jsonl`, branch `journal-simplified`,
commit a001b89): the schema-version-4 journal of one nominal run of the RPIO
producer's memory backend. A setup write (`Settings`), one sensor message
handled by `Monitor` (writes `Reading`, publishes `reading_recorded` citing
the Reading as `reading`, ends `Monitored`), which synchronously wakes
`Analysis` (reads `Reading`, ends `Nominal`). Eight records, epoch
`nominal-epoch`, clock `python:time.time_ns`.

The checker decodes it in `src/io/rpio/wire.rs` (unit tests) to confirm that
the producer's records and this decoder agree on the wire format.
