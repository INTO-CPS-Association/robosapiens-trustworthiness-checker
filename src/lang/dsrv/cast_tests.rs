use crate::{DsrvSpecification, TypeCheckOptions, VarName};

#[test]
fn casts_and_rounding_do_not_require_an_experiment_in_any_dialect() {
    for dialect in ["", "language core\n", "language distributed\n"] {
        for expression in [
            "out y: Float = 1 as Float",
            "out y: Str = 1 as Str",
            "out y: Int = trunc(1.5)",
            "out y: Int = floor(1.5)",
            "out y: Int = ceil(1.5)",
            "out y: Int = round(1.5)",
        ] {
            let source = format!("{dialect}{expression}");
            let specification = source
                .parse::<DsrvSpecification>()
                .unwrap_or_else(|error| panic!("{source}: {error}"));
            assert!(
                specification
                    .source_context()
                    .language()
                    .experiment_names()
                    .next()
                    .is_none(),
                "{source} enabled an experiment"
            );
            specification
                .check(TypeCheckOptions::STRICT)
                .discard_warnings()
                .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
        }
    }
}

#[test]
fn a_cast_target_is_one_type_name() {
    let error = "out y: Str = 1 as List<Int>"
        .parse::<DsrvSpecification>()
        .expect_err("a parameterised cast target must not parse");
    assert!(error.to_string().contains("parse"), "{error}");
}

#[test]
fn a_named_cast_target_resolves_through_the_type_environment() {
    let report = "type Number = Float\n\
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
    let specification = "in a: Int\nin b: Int\n\
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
