# Runtime capabilities

A DSRV source file must enable the [dialect or experiment](dsrv-language-settings.md)
for a construct it uses. The selected runtime must also be able to evaluate
that construct. If a specification contains an unsupported construct, the
runtime names it and refuses the specification before evaluating any stream.

| Capability | Constructs that require it |
|---|---|
| Distribution | `dist`, `monitored_at` (also require `language distributed`) |
| Tagged unions | Union values and constructors |
| Pattern matching | `match`, `matches` |
| Lazy `if` | An `if` under the `lazy_if` experiment; ordinary `if` does not require it |
| Stream function instances | A call of a `def` that introduces local streams under `local_streams`; declaring an unused function does not require instance support |

The matrix shows each runtime's evaluator capabilities. `yes` means the
runtime admits the construct; `no` means it refuses it. The `distributed`
runtime accepts only `untimed` semantics and needs distribution settings.
Reconfigurable runtimes need a live input pipeline. They refuse stream function
instances in both their initial model and every replacement, because carrying
stream history across a replacement does not show that an instance's private
state still means the same.

Live replacement messages use the low-level single-file DSRV preparation path;
they do not run the module/local-stream expander. Stream-function call syntax in such a
message is therefore rejected during preparation before a model switch. The
runtime capability check additionally rejects any already-expanded replacement
artifact whose provenance records a stream function instance.

<!-- runtime-capabilities:start -->
| Runtime | distribution | tagged unions | pattern matching | lazy if | stream function instances |
|---|---|---|---|---|---|
| `async` | no | yes | yes | no | no |
| `dataflow` | no | yes | yes | yes | yes |
| `distributed` | yes | yes | no | no | no |
| `semi-sync` | no | yes | yes | no | yes |
| `reconf-semi-sync` | no | yes | yes | no | no |
| `reconf-dataflow` | no | yes | yes | yes | no |
<!-- runtime-capabilities:end -->

The `async`, `dataflow`, and `semi-sync` rows are also exercised with file
input under each DSRV semantics. The other rows reflect the evaluators selected
by their runtime builders; those runtimes need additional setup to start.

A runtime expression accepted by `dynamic` or `defer` is checked when it is
accepted. An unsupported construct in it therefore fails during the run,
rather than at initial admission.
