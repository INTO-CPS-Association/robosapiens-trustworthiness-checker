//! Checker-owned suites, including relation laws and generated specifications.

mod checking;
mod concrete;
mod contextual;
mod diagnostics;
mod properties;
mod relations;
mod runtime_checks;

const TYPECHECK_PROPTEST_CASES: u32 = core::cfg_select! {
    feature = "extended-proptests" => 10_000,
    _ => 256,
};

fn property_config() -> proptest::test_runner::Config {
    use proptest::test_runner::{Config, FileFailurePersistence};

    // Both suites replay the seeds recorded before the property suite split.
    Config {
        cases: TYPECHECK_PROPTEST_CASES,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/lang/dsrv/type_checker/property_tests.txt"
        )))),
        ..Config::default()
    }
}
