//! What each runtime does with each capability, observed by running it.
//!
//! Runtimes declare their capabilities where they are implemented; nothing
//! lists them centrally. This test runs the checker for every `--runtime`
//! value and every capability fixture, and requires one of two outcomes: the
//! specification is admitted and runs to completion, or it is refused with the
//! admission error. A panic, a timeout or any other failure fails the test,
//! so a runtime that declares a capability it cannot deliver is caught.
//!
//! The observed outcomes are rendered as the table in
//! `docs/src/reference/runtime-capabilities.md`. Regenerate it with
//! `CAPABILITY_TABLE=overwrite cargo test --test runtime_capabilities`.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use clap::ValueEnum;
use trustworthiness_checker::cli::args::RuntimeKind;
use trustworthiness_checker::core::Capability;

/// Runtimes that cannot be started from a specification and an input file,
/// with the reason. Every other runtime is run.
const NOT_RUN: &[(&str, &str)] = &[
    ("distributed", "needs distribution settings"),
    ("reconf-semi-sync", "needs an input pipeline"),
    ("reconf-dataflow", "needs an input pipeline"),
];

const SEMANTICS: &[&str] = &["untimed", "typed-untimed", "gradual-typed-untimed"];

/// A specification and input that use exactly one capability.
fn fixture(capability: Capability) -> (&'static str, &'static str) {
    match capability {
        Capability::Distribution => ("distribution.dsrv", "distribution.input"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Admitted,
    Refused,
}

fn run(runtime: &str, semantics: &str, capability: Capability) -> Outcome {
    let (specification, input) = fixture(capability);
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/");
    let mut child = Command::new(env!("CARGO_BIN_EXE_trustworthiness_checker"))
        .args([
            &format!("{fixtures}{specification}"),
            "--input-file",
            &format!("{fixtures}{input}"),
            "--output-stdout",
            "--runtime",
            runtime,
            "--semantics",
            semantics,
        ])
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the checker starts");
    let deadline = Instant::now() + Duration::from_secs(30);
    while child
        .try_wait()
        .expect("the checker can be polled")
        .is_none()
    {
        if Instant::now() > deadline {
            child.kill().ok();
            panic!("{runtime}/{semantics} did not finish with {capability}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().expect("the checker's output");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let refusal =
        format!("cannot run on the {runtime} runtime, which does not support {capability}");
    match (output.status.success(), stderr.contains(&refusal)) {
        (true, false) if !stderr.contains("panicked") => Outcome::Admitted,
        (false, true) if !stderr.contains("panicked") => Outcome::Refused,
        _ => {
            panic!("{runtime}/{semantics} neither ran nor refused {capability} cleanly:\n{stderr}")
        }
    }
}

fn runtimes() -> Vec<String> {
    RuntimeKind::value_variants()
        .iter()
        .map(|runtime| runtime.to_possible_value().unwrap().get_name().to_owned())
        .collect()
}

fn table() -> String {
    let mut table = String::from("| Runtime |");
    for capability in Capability::ALL {
        table.push_str(&format!(" {capability} |"));
    }
    table.push_str("\n|---|");
    for _ in Capability::ALL {
        table.push_str("---|");
    }
    table.push('\n');
    for runtime in runtimes() {
        table.push_str(&format!("| `{runtime}` |"));
        let skipped = NOT_RUN.iter().find(|(name, _)| *name == runtime);
        for capability in Capability::ALL {
            let cell = match skipped {
                Some((_, reason)) => format!("not checked automatically ({reason})"),
                None => {
                    let outcomes: Vec<Outcome> = SEMANTICS
                        .iter()
                        .map(|semantics| run(&runtime, semantics, *capability))
                        .collect();
                    assert!(
                        outcomes.windows(2).all(|pair| pair[0] == pair[1]),
                        "{runtime} treats {capability} differently across semantics: {outcomes:?}"
                    );
                    match outcomes[0] {
                        Outcome::Admitted => "yes".to_owned(),
                        Outcome::Refused => "no".to_owned(),
                    }
                }
            };
            table.push_str(&format!(" {cell} |"));
        }
        table.push('\n');
    }
    table
}

#[test]
fn every_runtime_is_run_or_named_with_a_reason() {
    let runtimes = runtimes();
    for (name, _) in NOT_RUN {
        assert!(
            runtimes.iter().any(|runtime| runtime == name),
            "{name} is not a runtime"
        );
    }
    assert!(runtimes.len() > NOT_RUN.len(), "some runtime must be run");
}

#[test]
fn the_documented_table_matches_what_the_runtimes_do() {
    const START: &str = "<!-- runtime-capabilities:start -->\n";
    const END: &str = "<!-- runtime-capabilities:end -->";
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/src/reference/runtime-capabilities.md"
    );
    let page = std::fs::read_to_string(path).expect("the runtime capabilities page");
    let start = page.find(START).expect("start marker") + START.len();
    let end = page.find(END).expect("end marker");
    let observed = table();
    if page[start..end] != observed
        && std::env::var_os("CAPABILITY_TABLE").is_some_and(|value| value == "overwrite")
    {
        let updated = format!("{}{observed}{}", &page[..start], &page[end..]);
        std::fs::write(path, updated).expect("rewrite the runtime capabilities page");
        return;
    }
    assert_eq!(
        &page[start..end],
        observed,
        "the runtimes' behaviour changed; regenerate with \
         CAPABILITY_TABLE=overwrite cargo test --test runtime_capabilities"
    );
}
