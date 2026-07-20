#[test]
fn malformed_schemas_report_at_the_declaration() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
