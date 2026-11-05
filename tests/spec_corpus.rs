//! Every specification shipped with the repository parses, expands, passes
//! concrete type checking and elaborates.
//!
//! Every runtime checks and elaborates a specification before running it,
//! under their declared concrete types, so a shipped specification that does
//! not check is one no runtime accepts. Fragments that a harness assembles
//! into specifications are named `.dsrv.in` and are not part of the corpus.

use std::fs;
use std::path::{Path, PathBuf};

use trustworthiness_checker::lang::dsrv::Dialect;
use trustworthiness_checker::lang::dsrv::parser::parse_str;

fn specifications_under(root: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            specifications_under(&path, found);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "dsrv")
        {
            found.push(path);
        }
    }
}

#[test]
fn every_shipped_specification_parses_checks_and_elaborates() {
    let mut paths = Vec::new();
    specifications_under(Path::new("examples"), &mut paths);
    specifications_under(Path::new("tests/fixtures"), &mut paths);
    paths.sort();
    assert!(
        paths.len() > 20,
        "expected the shipped corpus, found {} files",
        paths.len()
    );

    // The constraints fragment is not a specification; the literal fixture is deliberately invalid.
    const NOT_ACCEPTED: [&str; 2] = [
        "examples/simple_add_distributable_dist_constraints.dsrv",
        "tests/fixtures/dsrv_syntax_revision_invalid_literal.dsrv",
    ];

    // Fixtures that are ill-typed on purpose, to test that checking rejects
    // them.
    const ILL_TYPED: [&str; 4] = [
        "examples/recursive_types_illtyped.dsrv",
        "examples/simple_add_illtyped.dsrv",
        "tests/fixtures/redundant_cast_with_error.dsrv",
        "tests/fixtures/invalid_typed_model.dsrv",
    ];

    let mut unexpected = Vec::new();
    for path in &paths {
        let name = path.to_string_lossy().replace('\\', "/");
        let source = fs::read_to_string(path).expect("a readable specification");
        let parsed = parse_str(&source);
        if let Ok(specification) = &parsed {
            // A file declares Distributed DSRV exactly when it uses the
            // distribution primitives; everything else is Full DSRV
            // with no experiments.
            let language = specification.source_context().language();
            let expected_dialect = if source.contains("monitored_at(") || source.contains("dist(") {
                Dialect::Distributed
            } else {
                Dialect::Full
            };
            // A shipped specification is read under the defaults, except for
            // the dialect its constructs need and the experiments it declares.
            let declares_experiments = source.contains("use experimental::");
            if language.dialect() != expected_dialect
                || language.experiment_names().next().is_some() != declares_experiments
            {
                unexpected.push(format!("{name} resolves to {language}"));
            }
            let checks = specification.clone().check_and_elaborate().result().is_ok();
            match (checks, ILL_TYPED.contains(&name.as_str())) {
                (true, false) | (false, true) => {}
                (false, false) => unexpected.push(format!("{name} fails concrete checking")),
                (true, true) => unexpected.push(format!("{name} now checks; update the list")),
            }
        }
        let accepted = parsed.is_ok();
        match (accepted, NOT_ACCEPTED.contains(&name.as_str())) {
            (true, false) | (false, true) => {}
            (false, false) => match parse_str(&source) {
                Err(error) => unexpected.push(format!("{name} no longer parses: {error}")),
                Ok(_) => unreachable!(),
            },
            (true, true) => unexpected.push(format!("{name} now parses; update the list")),
        }
    }

    assert!(unexpected.is_empty(), "{}", unexpected.join("\n"));
}

#[test]
fn shipped_reconfiguration_commands_parse_check_and_elaborate() {
    for entry in fs::read_dir("examples/reconf_cmds").expect("reconfiguration example directory") {
        let path = entry.expect("a reconfiguration example entry").path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("a readable reconfiguration command");
        let command: serde_json::Value = serde_json::from_str(&contents)
            .unwrap_or_else(|error| panic!("{} is not valid JSON: {error}", path.display()));
        let source = command["specification"]
            .as_str()
            .unwrap_or_else(|| panic!("{} has no specification string", path.display()));
        let specification = parse_str(source)
            .unwrap_or_else(|error| panic!("{} does not parse: {error}", path.display()));
        assert!(
            specification.check_and_elaborate().result().is_ok(),
            "{} fails concrete checking or elaboration",
            path.display()
        );
    }
}
