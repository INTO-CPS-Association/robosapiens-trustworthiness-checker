use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn model(source: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "tc-expand-process-{}-{}.dsrv",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::write(&path, source).unwrap();
    path
}

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tc-expand"))
}

#[test]
fn help_succeeds_without_starting_a_service() {
    let output = command().arg("--help").output().unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Usage: tc-expand")
    );
}

#[test]
fn version_succeeds_without_a_model() {
    let output = command().arg("--version").output().unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with("tc-expand ")
    );
}

#[test]
fn invalid_and_monitoring_arguments_are_rejected_by_clap() {
    for arguments in [
        vec!["--check-mode", "eventual"],
        vec!["model.dsrv", "--input-file", "values.csv"],
        vec![],
    ] {
        let output = command().args(arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn plain_mode_needs_no_runtime_input_or_output_configuration() {
    let path = model("out y: Int\ny = 1");
    let output = command().arg(&path).output().unwrap();
    fs::remove_file(path).unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("expanded, unchecked")
    );
}

#[test]
fn semantic_failure_is_status_one_with_empty_stdout() {
    let path = model("out y: Bool\ny = 1");
    let output = command()
        .args(["--check-mode", "strict"])
        .arg(&path)
        .output()
        .unwrap();
    fs::remove_file(path).unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr).unwrap().contains("error:"));
}

#[test]
fn global_view_loads_real_filesystem_and_embedded_modules_deterministically() {
    let directory = std::env::temp_dir().join(format!(
        "tc-expand-modules-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let root = directory.join("root.dsrv");
    fs::write(
        &root,
        "language distributed\n\
         use experimental::{modules, functions, generics, tagged_unions}\n\
         mod local\nuse local::*\nuse std::option\n\
         in x: Int\nout doubled: Int\nout placement: Bool\n\
         doubled = twice(x)\nplacement = dist(A, B) == 1\n",
    )
    .unwrap();
    fs::write(
        directory.join("local.dsrv"),
        "use experimental::functions\ndef twice(n: Int) -> Int = n * 2\n",
    )
    .unwrap();

    let first = command().arg(&root).output().unwrap();
    let second = command().arg(&root).output().unwrap();
    fs::remove_dir_all(directory).unwrap();
    assert!(first.status.success(), "{first:?}");
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());
    let report = String::from_utf8(first.stdout).unwrap();
    assert!(report.contains("# language: Distributed DSRV"));
    assert!(report.contains("local [filesystem:"));
    assert!(report.contains("std::option [embedded catalogue]"));
    assert!(report.find("local [filesystem:").unwrap() < report.find("std::option").unwrap());
    assert!(report.contains("doubled ="));
    assert!(report.contains("placement ="));
    assert!(report.contains("dist(A, B)"));
}
