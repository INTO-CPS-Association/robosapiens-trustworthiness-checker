//! Structural and lexical invariants checked independently of type inference.

use crate::lang::dsrv::validation::validate_specification;
use test_log::test;

#[test]
fn validation_respects_lambda_bindings() {
    let specification = "out z: Int\nz = (\\x: Int -> x)(1)".parse().unwrap();

    validate_specification(&specification).expect("lambda parameter should be in scope");
}
