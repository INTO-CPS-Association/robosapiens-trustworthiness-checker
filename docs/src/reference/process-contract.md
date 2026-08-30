# Process contract

The checker is a foreground process. It reads a model path and CLI options, opens the selected input/output resources, evaluates observations, and either exits after finite input or remains available for live input.

## Start and inputs

From a repository checkout:

```sh
cargo run -- MODEL INPUT_SELECTION [OUTPUT_SELECTION]
```

An installed binary has the same argument boundary without Cargo. `MODEL` is required and exactly one input selection is required. A finite `--input-file` run ends when the file is consumed. MQTT, Redis, Redis knowledge, ROS, and named live sources keep the process waiting for source items.

The current process has no separate readiness endpoint or healthcheck. Process existence proves neither that all routes opened nor that the first evaluation succeeded. For a supervisor, use an application-specific observation such as the first expected result or a selected log event.

## Result and diagnostic channels

- stdout carries stdout-destination results in the form `<variable>[<zero-based row>] = <Debug-formatted value>`.
- MQTT, Redis, ROS, and configured outputs use their transport-specific routes instead of stdout for those values.
- tracing diagnostics default to stderr; `--log-file PATH` appends them to the specified file.
- auxiliary and `NoVal` outputs are suppressed by the stdout sink.

Keep result stdout separate from diagnostics when scripting. A live transport output is locally admitted or flushed according to the selected writer/backend; that does not prove broker acknowledgement, remote observation, or persistence unless the external system is observed separately.

## Startup and failure boundaries

Argument parsing, CLI validation, model/configuration parsing, resource opening, evaluation, and output operations are separate failure points. An error at startup or during the run is returned as a process error; do not assign undocumented POSIX meanings to the resulting status. The exact diagnostic is written through the normal error/log path.

If a live source cannot be opened, first distinguish a parser/validation error from a transport connection error. If the process stays alive but no result appears, the useful next observation is the source route and payload, followed by the first complete input tick—not a process-alive check.

## Stop and restart

The checker has no explicit signal handler or checker-specific graceful-shutdown protocol in the current implementation. Treat a foreground stop or container stop as process termination, not as a verified flush or remote-persistence operation. A restart creates a new process and does not imply restoration of in-memory monitor history or queued output.

For Docker-specific mounts, networking, and entrypoint behavior, see [Run the checker as a Docker service](../tutorials/docker-service.md). For complete option facts, see the [generated CLI reference](cli.md); for output completion and failure details, see [outputs](../features/outputs.md) and [output architecture](../output.md).
