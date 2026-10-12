//! A def's parameter shadows a stream of the same name inside the def body,
//! on every runtime.

use std::path::Path;
use std::process::Command;

const INPUT: &str = "0: x = 3\n1: x = 4\n";

/// Run the checker on `spec` and return its sorted output lines.
fn run(name: &str, spec: &str, runtime: &str) -> Vec<String> {
    let directory = std::env::temp_dir().join(format!(
        "tc-def-shadow-{name}-{runtime}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("a scratch directory");
    let spec_path = directory.join("spec.dsrv");
    let input_path = directory.join("spec.input");
    std::fs::write(&spec_path, spec).expect("a scratch file");
    std::fs::write(&input_path, INPUT).expect("a scratch file");
    let output = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(&spec_path)
        .args(["--input-file"])
        .arg(&input_path)
        .args(["--output-stdout", "--runtime", runtime])
        .env("RUST_LOG", "error")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("the checker runs");
    let _ = std::fs::remove_dir_all(&directory);
    assert!(
        output.status.success(),
        "{runtime} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut lines: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

fn check_both(name: &str, spec: &str, expected: &[&str]) {
    for runtime in ["dataflow", "semi-sync"] {
        assert_eq!(run(name, spec, runtime), expected, "{runtime}");
    }
}

#[test]
fn parameter_named_like_an_input_shadows_it() {
    check_both(
        "input",
        "use experimental::functions\n\
         def twice(x: Int) -> Int = x * 2\n\
         in x: Int\n\
         out y: Int = twice(x + 1)\n",
        &["y[0] = 8", "y[1] = 10"],
    );
}

#[test]
fn parameter_named_like_the_output_being_defined_shadows_it() {
    check_both(
        "output",
        "use experimental::functions\n\
         def twice(y: Int) -> Int = y * 2\n\
         in x: Int\n\
         out y: Int = twice(x + 1)\n",
        &["y[0] = 8", "y[1] = 10"],
    );
}

#[test]
fn parameter_named_like_a_self_referring_output_shadows_it() {
    check_both(
        "recursive",
        "use experimental::functions\n\
         def double(y: Int) -> Int = y * 2\n\
         in x: Int\n\
         out y: Int = default(y[1], 0) + double(x)\n",
        &["y[0] = 6", "y[1] = 14"],
    );
}
