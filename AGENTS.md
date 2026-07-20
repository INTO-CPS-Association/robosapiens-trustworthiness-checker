# Normal development

- Prefer `--profile dev-fast` for routine builds, checks, and tests.
- Do not use `dev-fast` for debugging or profiling; use the profile appropriate to the debugger or profiling task.

# Benchmarking

- Once benchmarking has started, consistently use `--profile bench-fast` for subsequent builds, checks, and benchmark runs so the benchmark cache stays warm.
- Always use the `bench-fast` profile: `cargo bench --profile bench-fast ...`.
- Compile normally so Rust can use all cores: `cargo bench --profile bench-fast --bench <name> --no-run`.
- Pin only the resulting benchmark executable to a P-core. Identify P-cores with `lscpu -e=CPU,CORE,MAXMHZ`; choose an otherwise-idle CPU with the highest `MAXMHZ`, then run `taskset -c <P_CORE> target/bench-fast/deps/<benchmark-binary> '<filter>' --bench`. Never pin `cargo` or `rustc` to one core.
- For comparisons with `main`, create once and reuse the named worktree `../robosapiens-trustworthiness-checker-main-bench` (for example, `git worktree add ../robosapiens-trustworthiness-checker-main-bench main`). Build and benchmark `main` there so its target cache remains warm; do not repeatedly switch the working branch or recreate the worktree.
- Compare equivalent benchmark binaries with the same P-core, filters, profile, and machine load. Avoid running benchmarks during backups or other sustained CPU activity.
