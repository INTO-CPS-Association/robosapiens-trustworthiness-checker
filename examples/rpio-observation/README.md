# Records and a later clock sample

Run from the repository root:

```sh
cargo run --bin trustworthiness_checker -- examples/rpio-observation/records-and-clock.dsrv \
  --input-config examples/rpio-observation/records-and-clock.json5 --output-stdout
```

The native schema-2 capture contains an Execute Start and ServiceStarted.
The frozen manifest checks its two records, exact bytes and captured clock.
The later sample is a third, record-free tick. The expected values are:

```text
recorded[0] = true
clocked[0] = false
recorded[1] = true
clocked[1] = false
recorded[2] = false
clocked[2] = true
```

Output column order may differ. The process exits normally after three ticks.
No broker, generated capture or cleanup is required.
