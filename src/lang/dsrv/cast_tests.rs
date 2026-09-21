use crate::{DsrvSpecification, TypeCheckOptions, VarName};

#[test]
fn casts_require_the_experiment() {
    let error = "out y: Float = 1 as Float"
        .parse::<DsrvSpecification>()
        .expect_err("a cast without the experiment must be rejected");
    assert!(error.to_string().contains("casts"), "{error}");
}

#[test]
fn a_cast_target_is_one_type_name() {
    let error = "use experimental::{casts}\nout y: Str = 1 as List<Int>"
        .parse::<DsrvSpecification>()
        .expect_err("a parameterised cast target must not parse");
    assert!(error.to_string().contains("parse"), "{error}");
}

#[test]
fn a_named_cast_target_resolves_through_the_type_environment() {
    let report = "use experimental::{casts}\n\
                  type Number = Float\n\
                  out y: Number = 1 as Number"
        .parse::<DsrvSpecification>()
        .expect("the named target parses")
        .check(TypeCheckOptions::STRICT);
    assert!(report.warnings().is_empty());
    report
        .discard_warnings()
        .expect("the named target resolves to Float");
}

#[test]
fn casts_bind_below_unary_and_above_multiplication_and_associate_left() {
    let specification = "use experimental::{casts}\n\
                         in a: Int\nin b: Int\n\
                         out divided: Float = -a / b as Float\n\
                         out chained: Str = 1 as Float as Str"
        .parse::<DsrvSpecification>()
        .expect("the precedence examples parse");

    assert_eq!(
        specification
            .var_expr_ref(&VarName::new("divided"))
            .expect("divided is defined")
            .to_string(),
        "(-a / (b as Float))"
    );
    assert_eq!(
        specification
            .var_expr_ref(&VarName::new("chained"))
            .expect("chained is defined")
            .to_string(),
        "((1 as Float) as Str)"
    );
}
