//! Equation-local auxiliaries preserve explicit flat timelines on static runtimes.
use std::path::PathBuf;
use std::process::Command;

struct Program(PathBuf);
impl Program {
    fn write() -> Self {
        let dir =
            std::env::temp_dir().join(format!("tc-equation-where-parity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let local = "use experimental::local_streams\nin x: Int\nin hit: Bool\nout total: Int = count where { count: Int = default(count[1], 0) + x }\nout double: Int = count where { count: Int = default(count[1], 0) + x + x }\nout conditional: Int = if hit then ticks else 0 where { ticks: Int = default(ticks[1], 0) + 1 }";
        let flat = "in x: Int\nin hit: Bool\naux count: Int = default(count[1], 0) + x\naux doubled: Int = default(doubled[1], 0) + x + x\naux ticks: Int = default(ticks[1], 0) + 1\nout total: Int = count\nout double: Int = doubled\nout conditional: Int = if hit then ticks else 0";
        for (name, source) in [
            ("local.dsrv", local),
            ("flat.dsrv", flat),
            (
                "input.trace",
                "0: x = 2\n   hit = true\n1: x = -1\n   hit = false\n2: x = 3\n   hit = true\n",
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
fn equation_locals_match_flat_histories_on_supported_static_runtimes() {
    let program = Program::write();
    for semantics in ["untimed", "typed-untimed", "gradual-typed-untimed"] {
        for runtime in ["dataflow", "semi-sync", "async"] {
            let local = program.run("local.dsrv", runtime, semantics);
            assert_eq!(
                local,
                program.run("flat.dsrv", runtime, semantics),
                "{runtime}/{semantics}"
            );
            assert_eq!(
                local.len(),
                9,
                "{runtime}/{semantics} missed an output tick"
            );
            for expected in [
                "total[0] = 2",
                "total[1] = 1",
                "total[2] = 4",
                "double[2] = 8",
                "conditional[0] = 1",
                "conditional[1] = 0",
                "conditional[2] = 3",
            ] {
                assert!(
                    local.iter().any(|line| line == expected),
                    "{runtime}/{semantics} missing {expected}: {local:?}"
                );
            }
        }
    }
}
