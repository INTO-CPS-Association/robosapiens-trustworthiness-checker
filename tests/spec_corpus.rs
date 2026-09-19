//! Every specification shipped with the repository parses and expands.
//!
//! The frontend runs in two stages, syntax then expansion, and this guards
//! the corpus against a regression in either of them. Type checking is not
//! attempted here: some fixtures are deliberately ill-typed.

use std::fs;
use std::path::{Path, PathBuf};

use trustworthiness_checker::lang::dsrv::LanguageConfig;
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
fn every_shipped_specification_parses_and_expands() {
    let mut paths = Vec::new();
    specifications_under(Path::new("examples"), &mut paths);
    specifications_under(Path::new("tests/fixtures"), &mut paths);
    paths.sort();
    assert!(
        paths.len() > 20,
        "expected the shipped corpus, found {} files",
        paths.len()
    );

    // Two shipped files are not specifications this entry point accepts:
    // one carries distribution constraints, parsed elsewhere, and one is a
    // fixture whose literal is deliberately invalid.
    const NOT_ACCEPTED: [&str; 2] = [
        "examples/simple_add_distributable_dist_constraints.dsrv",
        "tests/fixtures/dsrv_syntax_revision_invalid_literal.dsrv",
    ];

    let mut unexpected = Vec::new();
    for path in &paths {
        let name = path.to_string_lossy().replace('\\', "/");
        let source = fs::read_to_string(path).expect("a readable specification");
        let parsed = parse_str(&source);
        if let Ok(specification) = &parsed {
            // No shipped file declares language settings yet.
            if specification.source_context().language() != &LanguageConfig::default() {
                unexpected.push(format!("{name} does not resolve to the default language"));
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
