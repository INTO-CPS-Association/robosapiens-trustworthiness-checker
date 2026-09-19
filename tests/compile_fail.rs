#[test]
fn frontend_privacy_contracts() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/frontend_privacy/*.rs");
}
