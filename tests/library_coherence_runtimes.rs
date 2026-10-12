//! Standard stream helpers preserve flat recurrences on supported runtimes.
use std::path::PathBuf;
use std::process::Command;

static NEXT_PROGRAM: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct Program(PathBuf);
impl Program {
    fn write() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "tc-library-coherence-parity-{}-{}",
            std::process::id(),
            NEXT_PROGRAM.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let header = "use experimental::{modules, functions, generics, local_streams, tagged_unions, pattern_matching}\nuse std::option::{Option}\nin x: Int\nin hit: Bool\nin reset: Bool\n";
        let local = format!(
            "{header}use std::stream\naux h = std::stream::hold_when(7,hit,x)\naux c = std::stream::count_when(reset,hit)\naux sample: Option<Int> = if hit then Some(x) else None\naux history = std::stream::recent::<2>(sample,reset)\nout held: Int = h\nout count: Int = c\nout before: List<Int> = history.before\nout after: List<Int> = history.after"
        );
        let flat = format!(
            "{header}aux previous_h: Int = default(h[1],7)\naux h: Int = if hit then x else previous_h\naux previous_c: Int = default(c[1],0)\naux c: Int = if reset then 0 else if hit then previous_c+1 else previous_c\naux last: Option<Int> = if reset then None else if hit then Some(x) else previous_last\naux previous_last: Option<Int> = default(last[1],None)\naux prior: List<Int> = default(current[1],[])\naux current: List<Int> = if reset then [] else if hit then match(previous_last){{Some(a)->[x,a],None->[x]}} else prior\nout held: Int = h\nout count: Int = c\nout before: List<Int> = prior\nout after: List<Int> = current"
        );
        for (name, source) in [
            ("local.dsrv", local.as_str()),
            ("flat.dsrv", flat.as_str()),
            (
                "input.trace",
                "0: x = 2\n   hit = true\n   reset = false\n1: x = 2\n   hit = true\n   reset = false\n2: x = 3\n   hit = true\n   reset = false\n3: x = 9\n   hit = false\n   reset = false\n4: x = 4\n   hit = true\n   reset = true\n",
            ),
        ] {
            std::fs::write(dir.join(name), source).unwrap();
        }
        Self(dir)
    }
    fn run(&self, source: &str, runtime: &str, semantics: &str) -> Vec<String> {
        let result = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .arg(self.0.join(source))
            .arg("--input-file")
            .arg(self.0.join("input.trace"))
            .args([
                "--output-stdout",
                "--runtime",
                runtime,
                "--semantics",
                semantics,
            ])
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{source}/{runtime}/{semantics}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let mut lines = String::from_utf8_lossy(&result.stdout)
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines.sort();
        lines
    }
}
impl Drop for Program {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn standard_stream_helpers_match_flat_histories_on_supported_runtimes() {
    let program = Program::write();
    for semantics in ["untimed", "typed-untimed", "gradual-typed-untimed"] {
        for runtime in ["dataflow", "semi-sync"] {
            let local = program.run("local.dsrv", runtime, semantics);
            assert_eq!(
                local,
                program.run("flat.dsrv", runtime, semantics),
                "{runtime}/{semantics}"
            );
            assert_eq!(
                local.len(),
                20,
                "{runtime}/{semantics} missed an output tick"
            );
            for expected in ["held[0] = 2", "count[2] = 3", "count[4] = 0"] {
                assert!(
                    local.iter().any(|line| line == expected),
                    "{runtime}/{semantics} missing {expected}: {local:?}"
                );
            }
        }
    }
}

#[test]
fn async_refuses_standard_stream_function_instances() {
    let program = Program::write();
    for semantics in ["untimed", "typed-untimed", "gradual-typed-untimed"] {
        let result = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
            .arg(program.0.join("local.dsrv"))
            .arg("--input-file")
            .arg(program.0.join("input.trace"))
            .args([
                "--runtime",
                "async",
                "--semantics",
                semantics,
                "--output-stdout",
            ])
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert!(!result.status.success());
        let message = String::from_utf8_lossy(&result.stderr);
        assert!(
            message.contains("cannot run on the async runtime")
                && message.contains("does not support stream function instances"),
            "{message}"
        );
        assert!(result.stdout.is_empty());
    }
}
