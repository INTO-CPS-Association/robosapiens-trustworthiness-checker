//! The lab journals in `tests/fixtures/journal/lab` are exactly what the
//! recipes in `tc_testutils::journal::app::lab` write. Set
//! `TC_REGENERATE_FIXTURES=1` to rewrite them.

use std::path::PathBuf;

use tc_testutils::journal::app::lab;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/journal/lab")
}

#[test]
fn lab_journals_match_their_recipes() {
    let regenerate = std::env::var_os("TC_REGENERATE_FIXTURES").is_some();
    for (stem, trace) in lab::all() {
        let path = dir().join(format!("{stem}.jsonl"));
        let expected = trace.jsonl();
        if regenerate {
            std::fs::write(&path, &expected).unwrap();
        }
        let found = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        assert_eq!(
            found,
            expected,
            "{} drifted from its recipe",
            path.display()
        );
        // Positions are contiguous from 1 and the record on line k is at k.
        for (index, record) in trace.records.iter().enumerate() {
            assert_eq!(record["position"]["seq"], index as i64 + 1);
            assert_eq!(record["schema_version"], 4);
        }
    }
}
