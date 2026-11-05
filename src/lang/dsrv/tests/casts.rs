use crate::{DsrvSpecification, VarName};

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
                .check()
                .discard_warnings()
                .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
        }
    }
}

#[test]
fn structural_cast_targets_remain_restricted() {
    for target in ["Struct<a: Int>", "(Int, Int)"] {
        let error = format!("out y: Str = 1 as {target}")
            .parse::<DsrvSpecification>()
            .expect_err("a structural cast target other than a collection must not parse");
        assert!(error.to_string().contains("parse"), "{target}: {error}");
    }
    let error = check_cast("out y = 1 as List<Int>").expect_err("an Int does not cast to a list");
    assert!(error.contains("cannot cast Int to List<Int>"), "{error}");
}

#[test]
fn a_named_cast_target_resolves_through_the_type_environment() {
    let report = "type Number = Float\n\
                  out y: Number = 1 as Number"
        .parse::<DsrvSpecification>()
        .expect("the named target parses")
        .check();
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

fn check_cast(source: &str) -> Result<Vec<String>, String> {
    let (result, warnings) = source
        .parse::<DsrvSpecification>()
        .expect("the cast parses")
        .check()
        .into_parts();
    result
        .map(|_| warnings.iter().map(|w| w.message().to_owned()).collect())
        .map_err(|errors| format!("{errors:?}"))
}

// A source whose type holds an integer literal's variable is not known until
// it settles, however deep the variable sits: until then it is neither the
// target nor anything else.
#[test]
fn a_cast_waits_for_literals_inside_its_source_to_settle() {
    for (source, cast) in [
        (
            "type Ints = List<Int>\nout y: Ints = [1, 2] as Ints",
            "cast from List<Int> to List<Int> is redundant",
        ),
        (
            "type Pair = (Int, Int)\n\
                 out y: Pair = Tuple(1, 2) as Pair",
            "cast from (Int, Int) to (Int, Int) is redundant",
        ),
    ] {
        assert_eq!(check_cast(source), Ok(vec![cast.to_owned()]), "{source}");
    }
}

// A literal settling as an `Int` is no more a `Float` inside a list than a
// variable bound to `Int` would be: the cast is refused, and says so in the
// types it settled as.
#[test]
fn a_cast_refuses_what_its_settled_source_cannot_become() {
    let error = check_cast("type Floats = List<Float>\nout y: Floats = [1] as Floats")
        .expect_err("a list of Ints does not cast to a list of Floats");
    assert!(
        error.contains("cannot cast List<Int> to List<Float>"),
        "{error}"
    );
}

// A cast gives an empty collection its element type, which nothing else in the
// expression says. It is the type's only statement, so it is not redundant.
#[test]
fn a_cast_gives_an_empty_collection_its_element_type() {
    for source in [
        "out y: List<Int> = [] as List<Int>",
        "out y: List<Int> = List() as List<Int>",
        "type Ints = List<Int>\nout y: Ints = [] as Ints",
        "type Ints = List<Int>\nout y: List<Int> = [] as Ints",
        "out y: Int = List.len([] as List<Int>)",
        "type Ints = List<Int>\nout y: Int = List.len([] as Ints)",
        "out y: List<Int> = List.append([] as List<Int>, 1)",
        "out y: List<List<Int>> = [] as List<List<Int>>",
        "out y: List<Int> = ([] as List<Int>) as List<Int>",
        "out y: Map<Int> = Map() as Map<Int>",
        "out y: Map<Int> = {} as Map<Int>",
        "type Counts = Map<Int>\nout y: Counts = Map() as Counts",
        "out y: Int = Map.get(Map() as Map<Int>, \"a\")",
    ] {
        let warnings =
            check_cast(&source).unwrap_or_else(|error| panic!("{source} should check: {error}"));
        let expected: &[&str] = if source.contains("([] as List<Int>) as List<Int>") {
            &["cast from List<Int> to List<Int> is redundant"]
        } else {
            &[]
        };
        assert_eq!(warnings, expected, "{source}");
    }
}

// The element type the cast gave is the list's type from then on.
#[test]
fn an_empty_collection_keeps_the_element_type_its_cast_gave_it() {
    for source in [
        "out y: List<Str> = [] as List<Int>",
        "out y: List<Int> = List.append([] as List<Int>, \"a\")",
        "out y: Map<Str> = Map() as Map<Int>",
    ] {
        assert!(check_cast(&source).is_err(), "{source}");
    }
}

#[test]
fn a_cast_is_refused_where_its_source_is_no_empty_collection_of_the_target() {
    for (source, message) in [
        ("[1] as List<Str>", "cannot cast List<Int> to List<Str>"),
        ("[1.5] as List<Int>", "cannot cast List<Float> to List<Int>"),
        ("[\"a\"] as List<Int>", "cannot cast List<Str> to List<Int>"),
        (
            "Map(\"a\": 1) as Map<Str>",
            "cannot cast Map<Int> to Map<Str>",
        ),
        ("([] as List<Int>) as Int", "cannot cast"),
        ("([] as List<Int>) as Str", "cannot cast"),
        ("([] as List<Int>) as Map<Int>", "cannot cast"),
        ("(Map() as Map<Int>) as List<Int>", "cannot cast"),
    ] {
        let source = format!("out y = {source}");
        let error = check_cast(&source).expect_err(&source);
        assert!(error.contains(message), "{source}: {error}");
    }

    assert_eq!(
        check_cast("out y: List<Int> = [1] as List<Int>",),
        Ok(vec![
            "cast from List<Int> to List<Int> is redundant".to_owned()
        ])
    );
}

#[test]
fn invalid_concrete_casts_keep_the_conversion_diagnostic_and_location() {
    for (source, message) in [
        (
            "in x:Float\nout y:Int=x as Int",
            "cannot cast Float to Int; use trunc, floor, ceil, or round instead",
        ),
        (
            "in x:Str\nout y:Float=x as Float",
            "cannot cast Str to Float",
        ),
        (
            "in x:List<Int>\nout y:Str=x as Str",
            "cannot cast List<Int> to Str",
        ),
    ] {
        let errors = source
            .parse::<DsrvSpecification>()
            .unwrap()
            .check()
            .discard_warnings()
            .expect_err("unsupported concrete conversion");
        let error = errors
            .iter()
            .find_map(|error| match error {
                crate::lang::dsrv::diagnostics::SemanticError::TypeError(error) => Some(error),
                _ => None,
            })
            .expect("concrete cast diagnostic");
        assert_eq!(error.message(), message);
        assert_eq!(
            error.location().primary().and_then(|site| site.snippet()),
            source.rsplit('=').next()
        );
    }
}

#[test]
fn expression_source_annotations_accept_literals_and_string_expressions() {
    for source in [
        "out y = eval(\"-42\" as Expr<Int>)",
        "in source: Str\nout y = eval(source as Expr<Int>)",
        "type Code = Expr<Int>\nin source: Str\nout y = eval(source as Code)",
        "in e: Expr<Int>\nout y = eval(default(e, \"-42\" as Expr<Int>))",
    ] {
        assert!(check_cast(source).unwrap().is_empty(), "{source}");
    }
    for expression in [
        "1 as Expr<Int>",
        "true as Expr<Int>",
        "(\"-42\" as Expr<Int>) as Expr<Bool>",
    ] {
        assert!(
            check_cast(&format!("out y = {expression}"))
                .unwrap_err()
                .contains("cannot cast")
        );
    }
}
