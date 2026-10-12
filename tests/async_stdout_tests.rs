//! CLI coverage for asynchronous producer-position labels.
use std::process::Command;

#[test]
fn async_stdout_labels_outputs_by_stream_position_after_no_val() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let spec = root.join("tests/fixtures/async_positions.dsrv");
    let input = root.join("tests/fixtures/async_positions.input");
    let result = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .arg(&spec)
        .args(["--input-file"])
        .arg(&input)
        .args([
            "--output-stdout",
            "--runtime",
            "async",
            "--semantics",
            "typed-untimed",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stdout = String::from_utf8(result.stdout).unwrap();
    let mut rows = stdout.lines().collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "w[0] = 10",
            "w[1] = 20",
            "w[2] = 30",
            "z[0] = 1",
            "z[2] = 3"
        ],
        "{stdout}"
    );
}
