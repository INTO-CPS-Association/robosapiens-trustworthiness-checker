# Redis knowledge-state input

The Redis knowledge-state provider lets the Trustworthiness Checker monitor
current values stored in the Knowledge part of a MAPLE-K adaptation loop. In a
typical RoboSAPIENS Adaptive Platform deployment, components publish transient
phase events through Redis Pub/Sub channels and store current shared knowledge,
such as the selected plan or adaptation state, in Redis keys.

The Trustworthiness Checker can use both kinds of input:

![MAPLE-K phase events and current Knowledge state entering the Trustworthiness Checker through Redis](assets/redis-knowledge-overview.svg)

| Information | Redis mechanism | Input source kind |
|---|---|---|
| Current robot mode, anomaly, plan, or adaptation state | Key | `redis-knowledge` |
| Phase completion or component event | Pub/Sub channel | `redis` |

A keyspace notification tells the Trustworthiness Checker that a selected key
may have changed. The provider reads the key's current value and emits it only
when the decoded value differs from the last value it emitted. Use Redis
Pub/Sub for events such as "Analyse completed" and Redis knowledge input for
state such as "the currently selected plan is return-to-base".

The Redis names on this page are representative, and should be configured to match a particular MAPLE-K and RoboSAPIENS Adaptive Platform configuration.

## Quickstart: monitor one Redis key

This walkthrough runs the Trustworthiness Checker on the host and Redis in a
Docker container. It uses Redis database 2, which is also the default database
for Redis knowledge input.

### Start Redis

Start a disposable Redis server with the required keyspace notifications:

```sh
docker run --rm -d \
  --name tc-redis-knowledge \
  -p 6379:6379 \
  redis:7-alpine \
  redis-server --notify-keyspace-events KEA
```

Redis is now available to the host at `localhost:6379`. The `KEA` setting
enables the keyspace notifications used by this provider. The Trustworthiness
Checker does not change this server-wide setting itself.

Check that Redis is ready:

```sh
docker exec tc-redis-knowledge redis-cli ping
```

The result should be:

```text
PONG
```

You can also inspect the notification setting:

```sh
docker exec tc-redis-knowledge \
  redis-cli CONFIG GET notify-keyspace-events
```

Podman is supported as well. For this walkthrough, replace `docker` with
`podman` in the commands above and below. Docker Compose is not required.

### Use the example model

From the repository root, use the checked-in DSRV specification at
`examples/redis-knowledge/robot-mode.dsrv`:

```dsrv
in robot_mode
out observed_mode

observed_mode = robot_mode
```

The model passes the selected state through directly. A missing Redis key is
emitted as `Value::NoVal`.

### Seed the selected key

Store a JSON string in database 2:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"idle"'
```

The example maps this Redis layout to the checker:

| Database | Redis key | Initial value | Checker input |
|---:|---|---|---|
| 2 | `robot:mode` | `"idle"` | `robot_mode` |

### Run the Trustworthiness Checker

From the repository root, run the checked-in example with Cargo:

```sh
cargo run -- examples/redis-knowledge/robot-mode.dsrv \
  --redis-knowledge-input \
  --redis-knowledge-key robot_mode=robot:mode \
  --redis-knowledge-database 2 \
  --redis-port 6379 \
  --output-stdout
```

The provider reads the current value at startup, so stdout should report an
`observed_mode` value of `idle` without waiting for another `SET` command.

### Change the value

In a second terminal, update the key:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"active"'
```

The Trustworthiness Checker should report the new `observed_mode` on stdout.

Write the same decoded value again:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET robot:mode '"active"'
```

Redis sends another notification, but the current value is unchanged. The
provider therefore does not send another `robot_mode` input to the checker.

Delete the key:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 DEL robot:mode
```

A missing selected key is represented by `Value::NoVal`. The provider emits
that state, but the stdout output handler intentionally suppresses `NoVal`.
Therefore the visible output is an `observed_mode` line for `idle`, then one for
`active`, followed by no line for the `DEL`; neither `NoVal` nor a replacement
string is printed. API consumers can inspect the emitted `Value::NoVal`
directly.

## Representative MAPLE-K example

The one-key walkthrough tests the knowledge provider in isolation. This example
combines current Knowledge with phase events in a small MAPLE-K flow.

MAPLE-K extends the conventional MAPE-K loop with a **Legitimate** phase added
by the RoboSAPIENS research project. In this example, both Analyse completion and
Legitimate completion arrive as Redis Pub/Sub events, while the selected plan is
current state stored in a Redis key.

![Analyse and Legitimate events combined with the current selected plan](assets/redis-knowledge-maple-example.svg)

The representative Redis layout is:

| MAPLE-K information | Redis kind | Representative Redis name | Checker input |
|---|---|---|---|
| Analyse completed | Pub/Sub channel | `maple:analyse:completed` | `analyse_completed` |
| Current selected plan | Key in database 2 | `maple:plan:current` | `current_plan` |
| Legitimate completed | Pub/Sub channel | `maple:legitimate:completed` | `legitimate_completed` |

### Create the MAPLE-K model

Use the checked-in DSRV specification at
`examples/redis-knowledge/maple-plan.dsrv`:

```dsrv
in analyse_completed: Bool
in current_plan: Str
in legitimate_completed: Bool

out legitimate_plan: Str

legitimate_plan = if analyse_completed && legitimate_completed then current_plan else "plan-not-legitimate"
```

Without an input window, the selected plan snapshot and each phase-completion
event remain independent singleton ticks. The runtime retains the latest value
of each sparse input. Once both completion inputs have been observed as `true`,
a later selected-plan update can therefore be evaluated immediately using those
retained phase values; another pair of phase events is not required and does not
implicitly start a new MAPLE-K cycle.

### Create the input configuration

Use the checked-in source configuration at
`examples/redis-knowledge/maple-inputs.json5`:

```json5
{
  // These names are representative, not canonical MAPLE-K Redis names.
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

The `redis` source maps checker inputs to transient Pub/Sub channels. The
`redis-knowledge` source maps checker inputs to keys whose current values are
read from database 2. The per-source port is omitted so `--redis-port 6379`
applies to both sources.

### Seed the selected plan

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET maple:plan:current '"inspect-area"'
```

### Run the mixed-input checker

From the repository root, run the checked-in mixed-input example with Cargo:

```sh
cargo run -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --output-stdout
```

This initial MAPLE-K command deliberately does not configure an input window.
The selected plan snapshot and each phase event remain independent logical ticks.

### Publish the phase events

In another terminal, publish the representative Analyse and Legitimate events:

```sh
docker exec tc-redis-knowledge \
  redis-cli PUBLISH maple:analyse:completed true

docker exec tc-redis-knowledge \
  redis-cli PUBLISH maple:legitimate:completed true
```

Stdout should report `inspect-area` as the legitimate plan after the phase
inputs have been observed.

Change the selected plan:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 SET maple:plan:current '"return-to-base"'
```

Because the runtime retains the latest input values with most-recent-value semantics, both completion
inputs are still `true` from the preceding events. The Redis knowledge update
therefore causes the Trustworthiness Checker to report `return-to-base` as soon
as the provider reads the changed key.

The observable flow is:

1. The provider publishes the initial selected-plan state.
2. Analyse and Legitimate each publish an independent completion event.
3. Once both retained phase values are `true`, the Trustworthiness Checker
   reports the selected plan.
4. A later selected-plan update is evaluated against those retained phase values
   and can produce another result immediately.

### Optional atomic-step window

If an application needs a complete MAPEL-K iteration to be treated as a coherent composite state update, configure a post-composition atomic-step
window. This changes the default independent event-based updates into composite row-based update. This can be done using a fixed step size:

```sh
cargo run -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --input-window-mode atomic-step \
  --input-window-update-limit 3 \
  --output-stdout
```
or alternatively, with a timeout window, which treats all updates within 10ms as a simultaneous batch update:
```sh
cargo run -- examples/redis-knowledge/maple-plan.dsrv \
  --input-config examples/redis-knowledge/maple-inputs.json5 \
  --redis-port 6379 \
  --input-window-mode atomic-step \
  --input-window-ms 10 \
  --output-stdout
```

The window is applied after Redis Pub/Sub and Redis knowledge sources are
composed. It intentionally changes the independent updates into one simultaneous
tick with last-update-wins reduction.

## Applying the pattern across MAPLE-K

The appropriate Redis mechanism depends on whether a value is current state or
a transient event:

| MAPLE-K phase or store | Example information | Suggested Redis mechanism |
|---|---|---|
| Monitor | New observation available | Pub/Sub event |
| Analyse | Analysis completed or anomaly detected | Pub/Sub event |
| Plan | Current selected plan | Knowledge key |
| Legitimate | Legitimacy check completed | Pub/Sub event |
| Execute | Execution completed | Pub/Sub event |
| Knowledge | Current anomaly, plan, mode, or adaptation state | Knowledge key |

The Trustworthiness Checker does not prescribe how MAPLE-K components implement
their communication or name their Redis resources. Its input configuration maps
the deployment's chosen names to checker input variables.

For example:

```json5
{
  sources: {
    events: {
      kind: "redis",
      host: "redis",
      routes: {
        analyse_completed: "<Analyse completion channel>",
        legitimate_completed: "<Legitimate completion channel>",
        execute_completed: "<Execute completion channel>",
      },
    },
    knowledge: {
      kind: "redis-knowledge",
      host: "redis",
      database: 2,
      keys: {
        current_plan: "<selected plan key>",
        adaptation_state: "<adaptation state key>",
      },
    },
  },
}
```

`host: "localhost"` is appropriate for the walkthrough because the
Trustworthiness Checker runs on the host. A deployment where Redis and the
checker share a container network can instead use Redis's service name, such as
`host: "redis"`.

## Supported values

Present values are decoded in this order (standard JSON is valid JSON5):

```text
JSON5 → plain UTF-8 string
```

| Stored Redis value | Checker value |
|---|---|
| `true` | Boolean |
| `42` | Integer |
| `3.5` | Floating-point value |
| `"active"` | String |
| `{mode: "active"}` | Object/map |
| `[1, 2, 3]` | List |
| `inspection robot` | Plain string fallback |
| Missing key | `NoVal` |

Values such as `true`, `42`, and `null` are decoded as typed values before the
plain-text fallback is considered. A stored JSON5 `null` follows normal checker
value decoding and is distinct from a missing key. Python pickle and arbitrary
binary values are not supported.

## Configuration reference

### Simple source options

| Option | Purpose |
|---|---|
| `--redis-knowledge-input` | Select Redis knowledge as the single input source |
| `--redis-knowledge-key INPUT=KEY` | Map a checker input to a Redis key; repeat for multiple keys |
| `--redis-knowledge-database N` | Select the Redis database; default is 2 |
| `--redis-knowledge-publish-initial true\|false` | Control whether current key values are emitted at startup |
| `--redis-knowledge-no-initial` | Disable the startup snapshot |
| `--redis-port PORT` | Use this port when the source configuration does not specify one |

### Named source configuration

Use `--input-config` when a checker combines Redis knowledge with Pub/Sub or
other input sources. A `redis-knowledge` source accepts:

| Field | Purpose |
|---|---|
| `host` | Redis hostname or address |
| `port` | Optional per-source port |
| `database` | Redis database; default is 2 |
| `publish_initial` | Emit current selected values at startup; default is `true` |
| `keys` | Map checker input variables to exact Redis keys |
| `retry` | Configure connection retry behaviour |

For example:

```json5
{
  sources: {
    knowledge: {
      kind: "redis-knowledge",
      host: "redis",
      database: 2,
      publish_initial: true,
      keys: {
        current_plan: "maple:plan:current",
      },
      retry: {
        max_attempts: null,
        initial_delay_ms: 250,
        max_delay_ms: 5000,
      },
    },
  },
}
```

A per-source port takes precedence over the global `--redis-port` fallback.
Focused CLI knowledge options override the selected configured knowledge source.
When several knowledge sources are configured, use
`--redis-knowledge-source SOURCE_ID` to select the one to override. A knowledge
source cannot declare `reconfiguration_route`; reconfiguration remains on a
separate control-capable source. Extra catalog entries may remain configured for
later model generations.

### Long-running deployments

By default, the provider retries transport failures indefinitely, beginning at
250 milliseconds and increasing to a maximum of 5 seconds. Configure this with:

- `--redis-knowledge-retry-max-attempts N` for a finite number of attempts.
- `--redis-knowledge-retry-forever` for unbounded retries.
- `--redis-knowledge-retry-initial-delay-ms N` for the initial delay.
- `--redis-knowledge-retry-max-delay-ms N` for the maximum delay.

`max_attempts` includes the first connection attempt. Invalid local source and
retry configuration is reported before Redis is opened.

## Behaviour and limitations

- Current values are read at startup unless initial emission is disabled.
- Startup and reconnect use subscribe-before-snapshot ordering; notifications
  during `MGET` remain dirty and are refetched.
- Missing selected keys produce `NoVal` (`Value::NoVal`).
- Writing an equivalent decoded value does not produce another checker input.
- Notifications are invalidations, not values; the provider reads the key's
  current state after a notification.
- Rapid writes may be coalesced. The latest observed state is exposed, but every
  transient state is not guaranteed to be seen.
- Keyspace notifications are not a durable change log.
- After reconnecting, the provider compares current selected values with the
  values it emitted previously.
- Each changed key is an independent logical tick. `InputStage::Batch` preserves
  those ticks; `WindowToStep` with `LastUpdateWins` intentionally makes the
  configured window one simultaneous tick after source composition.
- A read or decoding error is reported through the input stream and terminates
  the configured source through the normal `InputSource` error path.
- A Redis knowledge key is data, not a checker reconfiguration signal; the
  private control barrier must come from a separate control-capable source.

## Troubleshooting

### No update appears after `SET`

Check that keyspace notifications are enabled:

```sh
docker exec tc-redis-knowledge \
  redis-cli CONFIG GET notify-keyspace-events
```

### The initial value is missing

Check the configured database and key:

```sh
docker exec tc-redis-knowledge \
  redis-cli -n 2 GET robot:mode
```

### Pub/Sub works but knowledge updates do not

Pub/Sub channels and knowledge keys are separate. Check that an event is listed
under `routes`, a knowledge value is listed under `keys`, and the knowledge
source uses the database in which the key was written.

You can inspect subscribers for the representative Analyse channel with:

```sh
docker exec tc-redis-knowledge \
  redis-cli PUBSUB NUMSUB maple:analyse:completed
```

### The example container name is already in use

```sh
docker rm -f tc-redis-knowledge
```

## Stop the example

Stop and remove the disposable Redis container:

```sh
docker stop tc-redis-knowledge
```
