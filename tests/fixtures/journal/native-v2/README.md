# Native classic fixture lineage

These JSONL files are native schema-2 inputs for `rpio_journal_decoding`.
`manifest.json` records immutable bytes, extent, the configured clock label,
producer/study revisions, and original artifact locations. Source positions
are physical file lines; Knowledge global write sequence is a separate identity.

`clean-local` and `clean-redis` are fresh actual 70-cycle callback captures at
RPIO `952c943` and ScrewDetection `71d0e42`. They preserve physical clock labels
`python:time.time_ns` and `redis.server.time` respectively. The producer source
hashes were frozen around capture.

`latency-nominal` and `latency-uncertainty` are the earlier frozen 65-cycle
three-way pilot fixtures. The latter contains the deliberate uncertainty fault.
Their `classic-journal` replay label and disabled live sampling belong to that
experiment; these inputs are not reruns of the historical performance report.

The manifest also anchors the small hand-written native files in
`fixtures/rpio`. Those are protocol examples, not actual producer captures.

Other files under `tests/fixtures/journal` retain archival schema-4 inline
journals for logical coordinator tests. Their hashes, positions, clock labels
and inline shape remain checked. They are explicitly refused by the native
classic decoder and are not presented as current native inputs.
