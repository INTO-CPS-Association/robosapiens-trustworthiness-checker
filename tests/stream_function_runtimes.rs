//! Lexical match calls agree across typed runtimes.

use std::path::Path;
use std::process::Command;

#[test]
fn semi_sync_matches_dataflow_for_lexical_match_calls() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for semantics in ["typed-untimed", "gradual-typed-untimed"] {
        let run = |runtime: &str| {
            let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
                .args([
                    "tests/fixtures/lexical_match_calls.dsrv",
                    "--input-file",
                    "tests/fixtures/lexical_match_calls.input",
                    "--output-stdout",
                    "--runtime",
                    runtime,
                    "--semantics",
                    semantics,
                ])
                .env("RUST_LOG", "error")
                .current_dir(root)
                .output()
                .expect("the checker runs");
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{runtime}/{semantics}: {error}");
            let mut lines = String::from_utf8(output.stdout)
                .expect("UTF-8 output")
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            lines.sort_unstable();
            lines
        };
        assert_eq!(run("semi-sync"), run("dataflow"), "{semantics}");
    }
}

