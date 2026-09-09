# Redis knowledge-state input

Use Redis knowledge input when the Trustworthiness Checker needs the current
value of selected Redis keys, such as a current plan, mode, or adaptation state.
Use ordinary Redis Pub/Sub input (`redis`) for transient events such as “Analyse
completed”. The two sources are different:

| Information | Redis mechanism | Checker source kind |
|---|---|---|
| Current mode, plan, anomaly, or adaptation state | Selected key plus keyspace notification | `redis-knowledge` |
| Phase completion or component event | Pub/Sub channel | `redis` |

A keyspace notification is an invalidation, not the new value. The provider
rereads each selected key and emits the decoded value only when it differs from
the last value it emitted. It does not infer a key by changing a variable name;
every knowledge key must be mapped explicitly.

{{#include assets/redis-knowledge-overview.svg}}

## Quickstart: monitor one key

This walkthrough runs the checker on the host and a disposable Redis server in a
Docker container. Run all host commands from the repository root. Docker may
pull `redis:7-alpine`; Podman can replace `docker` in these commands.

### Start and check Redis

```sh
docker run --rm -d \
  --name tc-redis-knowledge \
  -p 6379:6379 \
  redis:7-alpine \
  redis-server --notify-keyspace-events KEA
```

The `KEA` setting enables the keyspace notifications used by this provider; the
checker does not set it on the server. Check the Redis service and setting:

```sh
docker exec tc-redis-knowledge redis-cli ping
docker exec tc-redis-knowledge \
  redis-cli CONFIG GET notify-keyspace-events
```

The first command should return:

```text
PONG
```

`PONG` proves that Redis is reachable. It does not prove that the
Trustworthiness Checker is running, subscribed, or ready to evaluate input.

### Seed and run the checker

This DSRV program exposes the selected state without changing it:

```dsrv
in robot_mode
out observed_mode
observed_mode = robot_mode
```

The repository stores this program as `examples/redis-knowledge/robot-mode.dsrv`. Store a JSON string in Redis database 2, the knowledge-source default:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"idle"'
```

From the repository root, start the foreground checker:

```sh
cargo run --quiet -- examples/redis-knowledge/robot-mode.dsrv \
  --redis-knowledge-input \
  --redis-knowledge-key robot_mode=robot:mode \
  --redis-knowledge-database 2 \
  --redis-port 6379 \
  --output-stdout
```

`publish_initial` defaults to `true`, so the provider reads the existing key at
startup. The stdout sink's exact framing is:

```text
<output-variable>[<zero-based-output-index>] = <Debug-style-value>
```

For the sequence in this walkthrough, representative stdout is:

```text
observed_mode[0] = Str("idle")
```

The first expected line is the useful readiness observation for the checker: it
shows that the source connection, initial snapshot, model evaluation, and local
stdout admission have progressed. A running checker process alone proves only
liveness.

### Change, repeat, and delete the key

In another terminal, still using the repository's Docker container, write a new
value:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"active"'
```

The checker should emit a second line. With no other visible updates, the
representative output is:

```text
observed_mode[1] = Str("active")
```

Writing the same decoded value again sends a Redis notification but does not
produce another checker input:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"active"'
```

Delete the key:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 DEL robot:mode
```

A missing selected key becomes `Value::NoVal`. The stdout sink suppresses
`NoVal`, so deletion produces no replacement line. API consumers can still
observe the emitted `NoVal` state. The checker remains running after each
change; it waits for another notification until you stop it.


## MAPLE-K: current Knowledge plus phase events

The one-key run tests current state. A MAPLE-K flow commonly combines that state
with transient phase events: in this representative layout, Analyse and
Legitimate completion use Pub/Sub, while the selected plan uses a database-2
knowledge key. The names are deployment choices, not canonical Redis names.

{{#include assets/redis-knowledge-maple-example.svg}}

| MAPLE-K information | Redis kind | Representative name | Checker input |
|---|---|---|---|
| Analyse completed | Pub/Sub channel | `maple:analyse:completed` | `analyse_completed` |
| Current selected plan | Key in database 2 | `maple:plan:current` | `current_plan` |
| Legitimate completed | Pub/Sub channel | `maple:legitimate:completed` | `legitimate_completed` |

{{#include assets/redis-knowledge-maple-example.svg}}

The MAPLE-K example exposes the current plan only after both phase-completion events are true:

```dsrv
in analyse_completed: Bool
in current_plan: Str
in legitimate_completed: Bool

out legitimate_plan: Str
legitimate_plan = if analyse_completed && legitimate_completed then current_plan else "plan-not-legitimate"
```

The repository stores this program as `examples/redis-knowledge/maple-plan.dsrv`. Its mixed-source configuration, `examples/redis-knowledge/maple-inputs.json5`, connects the two event channels and current-plan key:

```json5
{
  sources: {
    "maple-events": {
      kind: "redis",
      host: "localhost",
      routes: {
        analyse_completed: "maple:analyse:completed",
        legitimate_completed: "maple:legitimate:completed",
      },
    },
    "maple-knowledge": {
      kind: "redis-knowledge",
      host: "localhost",
      database: 2,
      publish_initial: true,
      keys: {
        current_plan: "maple:plan:current",
      },
    },
  },
}
```

Stop the one-key checker first if it is still running, then seed and start the
mixed-input checker from the repository root:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET maple:plan:current '"inspect-area"'

cargo run --quiet -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --output-stdout
```

Publish the two phase events from another terminal:

```sh
docker exec tc-redis-knowledge \
  redis-cli PUBLISH maple:analyse:completed true

docker exec tc-redis-knowledge \
  redis-cli PUBLISH maple:legitimate:completed true
```

After both `true` values have been observed, stdout should contain a line framed
like:

```text
legitimate_plan[<zero-based-output-index>] = Str("inspect-area")
```

The exact index is representative rather than fixed because it depends on which
input ticks produce a visible output. Change the selected plan:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET maple:plan:current '"return-to-base"'
```

The latest phase values remain retained for the sparse inputs, so the later key
update can produce a line framed like:

```text
legitimate_plan[<later-zero-based-index>] = Str("return-to-base")
```

Without an input window, the key snapshot and each Pub/Sub event are independent
logical ticks. A later plan update does not implicitly start a new MAPLE-K cycle;
it is evaluated using the latest retained input values.

### The atomic-step window is a timing reduction, not a transaction

A count-bounded variant is:

```sh
cargo run --quiet -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --input-window-mode atomic-step \
  --input-window-update-limit 3 \
  --output-stdout
```

A time-bounded variant is:

```sh
cargo run --quiet -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --input-window-mode atomic-step \
  --input-window-ms 10 \
  --output-stdout
```

`atomic-step` is applied after the sources have been composed. The `10 ms`
option is a local timing window: updates observed within that window are reduced
with last-update-wins per variable. Events near the boundary can fall into
different windows. The window does not provide cross-source or global atomicity,
does not make Redis writes transactional, and does not guarantee that all events
of one MAPLE-K iteration are grouped together. An update limit is likewise a
flush threshold; a logical tick is never split merely to satisfy the limit.

## Values, options, and limits

Knowledge keys are decoded as JSON5 first, then as plain UTF-8 text:

| Stored value | Checker value |
|---|---|
| `true` | Boolean |
| `42` | Integer |
| `3.5` | Floating-point value |
| `"active"` | String |
| `{mode: "active"}` | Object/map |
| `[1, 2, 3]` | List |
| `inspection robot` | Plain string fallback |
| Missing key | `NoVal` |

Python pickle and arbitrary binary values are not supported. A stored JSON5
`null` is a decoded value and is distinct from a missing key.

| Option | Effect |
|---|---|
| `--redis-knowledge-input` | Select knowledge keys as the single input source |
| `--redis-knowledge-key INPUT=KEY` | Explicitly map a checker input to a key; repeat it |
| `--redis-knowledge-database N` | Select the database; default is 2 |
| `--redis-knowledge-publish-initial true\|false` | Override startup snapshot emission |
| `--redis-knowledge-no-initial` | Disable the startup snapshot |
| `--input-config PATH` | Combine named `redis-knowledge` and other sources |
| `--redis-port PORT` | Fallback port when a source has no port |

With `--redis-knowledge-no-initial`, no startup output is expected from the
knowledge snapshot; the first visible result must follow a changed-key
notification. The provider subscribes before taking a snapshot and refetches
dirty keys, but keyspace notifications are not a durable change log. Rapid writes
may be coalesced, so every transient state is not guaranteed to be observed.

Transport failures retry indefinitely by default, starting at 250 ms and backing
off to a maximum of 5 seconds. `--redis-knowledge-retry-max-attempts N`,
`--redis-knowledge-retry-forever`,
`--redis-knowledge-retry-initial-delay-ms N`, and
`--redis-knowledge-retry-max-delay-ms N` adjust that policy. A knowledge source
carries data only; it cannot declare the reconfiguration control route.

Redis knowledge is incompatible with MSTLO. `--language mstlo` uses a dedicated
MSTLO runtime, while this provider produces ordinary `Value` input; the checker
rejects the combination with:

```text
Redis knowledge input produces ordinary `Value` input and is unsupported for MSTLO
```

Use an MSTLO-compatible source and codec instead of a Redis knowledge key when
monitoring an MSTLO model.

## Troubleshooting

### No update appears after `SET`

Check the notification setting and the exact database/key used by the checker:

```sh
docker exec tc-redis-knowledge \
  redis-cli CONFIG GET notify-keyspace-events
docker exec tc-redis-knowledge \
  redis-cli -n 2 GET robot:mode
```

`KEA` must be enabled for this provider, and a knowledge mapping must name the
same key explicitly.

### The initial value is missing

Check whether `--redis-knowledge-no-initial` was supplied. If it was not, verify
the selected database and key, then distinguish `PONG` (Redis reachability) from
the first checker stdout line (source snapshot and evaluation progress).

### Pub/Sub works but knowledge does not

Check that the event is under a source's `routes`, the current value is under a
`redis-knowledge` source's `keys`, and the key was written in that source's
database. Pub/Sub channels do not replace knowledge-key mappings.

## Stop and clean up

Stop the foreground Trustworthiness Checker with `Ctrl-C` before stopping Redis.
Then remove the disposable container:

```sh
docker stop tc-redis-knowledge
```

Because it was started with `--rm`, Docker removes the container after it stops.
If an interrupted run leaves the name occupied, use the explicit cleanup:

```sh
docker rm -f tc-redis-knowledge
```
