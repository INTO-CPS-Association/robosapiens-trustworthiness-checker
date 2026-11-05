//! Concrete root inference, contextual shapes, and located failures.

use crate::core::BinaryOperator;
use crate::core::StreamType;
use crate::lang::dsrv::ast::Expr;
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::test_support::type_check;

use crate::{DsrvSpecification, VarName};
use ecow::EcoVec;
use std::collections::{BTreeMap, BTreeSet};
use test_log::test;

#[test]
fn type_check_infers_unary_minus_result_type() {
    let checked = type_check("out z\nz = -1".parse().unwrap()).unwrap();
    assert_eq!(
        checked.type_annotations().get(&VarName::new("z")),
        Some(&StreamType::Int)
    );
}

#[test]
fn type_check_accepts_a_function_typed_annotation() {
    let source = "in x: Int\naux f: (Int -> Int)\nout z: Int\nf = \\v: Int -> v + 1\nz = f(x)";
    let checked = type_check(source.parse().unwrap())
        .unwrap_or_else(|errors| panic!("function annotation should check: {errors:?}"));
    assert_eq!(
        checked.type_annotations().get(&VarName::new("f")),
        Some(&StreamType::Function(
            EcoVec::from([StreamType::Int]),
            Box::new(StreamType::Int)
        ))
    );
}

#[test]
fn typed_expression_sources_infer_runtime_expression_results() {
    for operator in ["dynamic", "defer"] {
        let source = format!("in property: Expr<Int>\nout result\nresult = {operator}(property)");
        let checked = type_check(source.parse().unwrap())
            .unwrap_or_else(|errors| panic!("{operator} should infer from Expr<T>: {errors:?}"));
        assert_eq!(
            checked.type_annotations().get(&VarName::new("result")),
            Some(&StreamType::Int)
        );
    }
}

#[test]
fn checking_keeps_unascribed_string_sources_compatible() {
    let source = "in property: Str\nout result: Int\nresult = dynamic(property)";
    type_check(source.parse().unwrap())
        .expect("concrete checking should continue to accept an unascribed Str source");
}

#[test]
fn test_type_check_infers_unannotated_output() {
    let x: VarName = "x_infer".into();
    let y: VarName = "y_infer".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        y.clone(),
        Expr::BinOp(
            Box::new(Expr::Var(x.clone())),
            Box::new(Expr::Val(1)),
            BinaryOperator::Add,
        ),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(x.clone(), StreamType::Int);
    let spec = DsrvSpecification::new(
        BTreeSet::from([x]),
        BTreeSet::from([y.clone()]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let typed = type_check(spec).expect("concrete type check should infer y");
    assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Int));
    assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &StreamType::Int);
}

#[test]
fn inferred_auxiliary_types_preserve_stream_roles() {
    let x: VarName = "aux_x".into();
    let z: VarName = "aux_z".into();
    let u: VarName = "aux_u".into();
    let exprs = BTreeMap::from([
        (u.clone(), Expr::Var(x.clone())),
        (z.clone(), Expr::Var(u.clone())),
    ]);
    let type_annotations = BTreeMap::from([(x.clone(), StreamType::Int)]);
    let spec = DsrvSpecification::new(
        BTreeSet::from([x]),
        BTreeSet::from([z.clone()]),
        exprs,
        type_annotations,
        BTreeSet::from([u.clone()]),
    );

    let typed = type_check(spec).expect("concrete type check should preserve aux vars");

    assert_eq!(typed.type_annotations().get(&u), Some(&StreamType::Int));
    assert_eq!(typed.type_annotations().get(&z), Some(&StreamType::Int));
    assert_eq!(typed.aux_vars(), &BTreeSet::from([u.clone()]));
    assert_eq!(typed.stream_vars(), &BTreeSet::from([z.clone(), u]));
    assert_eq!(typed.output_vars(), &BTreeSet::from([z]));
}

#[test]
fn a_concrete_output_does_not_require_an_annotation() {
    let y: VarName = "y_strict_missing".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(y.clone(), Expr::Val(1));
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([y]),
        exprs,
        BTreeMap::new(),
        BTreeSet::new(),
    );

    assert!(type_check(spec).is_ok());
}

// --- Concrete typing: inference order, consistency, and casts ---

#[test]
fn test_infers_through_dependency_chain_out_of_order() {
    // `z` is interned first, though it depends on `y`, so `y` must be
    // inferred first and `z` from it.
    let z: VarName = "chain_z".into();
    let y: VarName = "chain_y".into();
    let x: VarName = "chain_x".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        z.clone(),
        Expr::BinOp(
            Box::new(Expr::Var(y.clone())),
            Box::new(Expr::Val(1)),
            BinaryOperator::Add,
        ),
    );
    exprs.insert(
        y.clone(),
        Expr::BinOp(
            Box::new(Expr::Var(x.clone())),
            Box::new(Expr::Val(1)),
            BinaryOperator::Add,
        ),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(x.clone(), StreamType::Int);
    let spec = DsrvSpecification::new(
        BTreeSet::from([x]),
        BTreeSet::from([y.clone(), z.clone()]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let typed = type_check(spec).expect("dependency chain should be inferred");
    assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Int));
    assert_eq!(typed.type_annotations().get(&z), Some(&StreamType::Int));
    assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &StreamType::Int);
    assert_eq!(typed.var_expr_ref(&z).unwrap().typ(), &StreamType::Int);
}

#[test]
fn test_rejects_noval_literal_ast() {
    let x: VarName = "noval_x".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        x.clone(),
        Expr::Val(crate::lang::dsrv::ast::SyntaxLiteral::NoVal),
    );
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([x]),
        exprs,
        BTreeMap::new(),
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("NoVal literal AST should be rejected");
    assert!(matches!(
        errors.as_slice(),
        [SemanticError::UnsupportedLiteral(_, _, _)]
    ));
}

#[test]
fn test_rejects_concrete_annotation_mismatch() {
    // A concrete static mismatch must remain an error rather than being
    // deferred to a runtime cast that can never succeed.
    let y: VarName = "mismatch_y".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(y.clone(), Expr::Val(1));
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(y.clone(), StreamType::Bool);
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([y]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    assert!(type_check(spec).is_err());
}

#[test]
fn test_rejects_inconsistent_container_annotation() {
    let xs: VarName = "bad_xs".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        xs.clone(),
        Expr::List(EcoVec::from(vec![Expr::Val("a").into()])),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(xs.clone(), StreamType::List(Box::new(StreamType::Int)));
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([xs]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    assert!(type_check(spec).is_err());
}

#[test]
fn test_annotated_contradiction_is_error() {
    // 1 + "a" is a contradiction; with an annotation it must hard-fail.
    let z: VarName = "contradiction_typed_z".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        z.clone(),
        Expr::BinOp(
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val("a")),
            BinaryOperator::Add,
        ),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(z.clone(), StreamType::Int);
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([z]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("contradiction should fail");
    assert!(!errors.is_empty());
}

#[test]
fn test_unannotated_contradiction_is_error() {
    // Even without an annotation, a concrete contradiction such as
    // `1 + "a"` can never succeed at runtime and must not be widened.
    let z: VarName = "contradiction_any_z".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        z.clone(),
        Expr::BinOp(
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val("a")),
            BinaryOperator::Add,
        ),
    );
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([z]),
        exprs,
        BTreeMap::new(),
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("contradiction should fail");
    assert!(!errors.is_empty());
}

/// Whichever helper finds a contradiction, an unannotated stream is not
/// widened to `Any` over it: the error is reported where it is.
#[test]
fn an_unannotated_contradiction_is_refused_at_its_site() {
    for (source, message, position, snippet) in [
        (
            "in x: Int\nout z\nz = !1",
            "expected Bool, got Int",
            (3, 6),
            "1",
        ),
        (
            "in x: Int\nout z\nz = !x",
            "expected Bool, got Int",
            (3, 6),
            "x",
        ),
        (
            "in x: Int\nout z\nz = if x then 1 else 2",
            "expected Bool, got Int",
            (3, 8),
            "x",
        ),
        (
            "in x: Str\nout z\nz = sin(x)",
            "expected Float, got Str",
            (3, 9),
            "x",
        ),
        (
            "in x: Str\nout z\nz = trunc(x)",
            "expected Float, got Str",
            (3, 11),
            "x",
        ),
        (
            "in x: Str\nout z\nz = List.get([1], x)",
            "expected Int, got Str",
            (3, 19),
            "x",
        ),
        (
            "in x: Str\nout z\nz = (\\n: Int -> n)(x)",
            "expected Int, got Str",
            (3, 20),
            "x",
        ),
    ] {
        let errors =
            type_check(source.parse().unwrap()).expect_err(&format!("{source} can never succeed"));
        let [SemanticError::TypeError(error)] = errors.as_slice() else {
            panic!("{source}: one type error, got {errors:?}");
        };
        assert!(error.message().contains(message), "{source}: {errors:?}");
        let site = error.location().primary().expect("a located error");
        assert_eq!(site.position(), Some(position), "{source}: {errors:?}");
        assert_eq!(site.snippet(), Some(snippet), "{source}: {errors:?}");
    }
}

#[test]
fn test_heterogeneous_list_rejects_homogeneous_annotation() {
    let xs: VarName = "heterogeneous_list_annotated_xs".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        xs.clone(),
        Expr::List(EcoVec::from(vec![
            Expr::Val(1).into(),
            Expr::Val("a").into(),
        ])),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(xs.clone(), StreamType::List(Box::new(StreamType::Int)));
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([xs]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("homogeneous annotation should fail");
    assert!(!errors.is_empty());
}

#[test]
fn test_heterogeneous_map_rejects_homogeneous_annotation() {
    let m: VarName = "heterogeneous_map_annotated_m".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        m.clone(),
        Expr::Map(BTreeMap::from([
            ("x".into(), Expr::Val(1).into()),
            ("y".into(), Expr::Val("a").into()),
        ])),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(m.clone(), StreamType::Map(Box::new(StreamType::Int)));
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([m]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("homogeneous annotation should fail");
    assert!(!errors.is_empty());
}

#[test]
fn test_infers_unannotated_struct_fields() {
    let robot: VarName = "struct_robot".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        robot.clone(),
        Expr::Struct(BTreeMap::from([
            ("id".into(), Expr::Val(1).into()),
            ("name".into(), Expr::Val("r2d2").into()),
        ])),
    );
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([robot.clone()]),
        exprs,
        BTreeMap::new(),
        BTreeSet::new(),
    );

    let typed = type_check(spec).expect("struct fields should be inferred");
    assert_eq!(
        typed.type_annotations().get(&robot),
        Some(&StreamType::Struct(
            EcoVec::from(vec![
                ("id".into(), StreamType::Int),
                ("name".into(), StreamType::Str),
            ]),
            false,
        ))
    );
    assert_eq!(
        typed.var_expr_ref(&robot).unwrap().typ(),
        &StreamType::Struct(
            EcoVec::from(vec![
                ("id".into(), StreamType::Int),
                ("name".into(), StreamType::Str)
            ]),
            false
        )
    );
}

#[test]
fn test_inferred_struct_can_feed_field_access() {
    let robot: VarName = "struct_a_robot".into();
    let name: VarName = "struct_z_name".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        robot.clone(),
        Expr::Struct(BTreeMap::from([
            ("id".into(), Expr::Val(1).into()),
            ("name".into(), Expr::Val("r2d2").into()),
        ])),
    );
    exprs.insert(
        name.clone(),
        Expr::SGet(Box::new(Expr::Var(robot.clone())), "name".into()),
    );
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([name.clone()]),
        exprs,
        BTreeMap::new(),
        BTreeSet::from([robot]),
    );

    let typed = type_check(spec).expect("field access should use inferred struct type");
    assert_eq!(typed.type_annotations().get(&name), Some(&StreamType::Str));
    assert_eq!(typed.var_expr_ref(&name).unwrap().typ(), &StreamType::Str);
}

fn struct_spec(annotation: StreamType) -> DsrvSpecification {
    let robot: VarName = "struct_robot".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        robot.clone(),
        Expr::Struct(BTreeMap::from([
            ("id".into(), Expr::Val(1).into()),
            ("name".into(), Expr::Val("r2d2").into()),
        ])),
    );
    DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([robot.clone()]),
        exprs,
        BTreeMap::from([(robot, annotation)]),
        BTreeSet::new(),
    )
}

/// `out y: output_type; y = r` where the input `r` has `input_type`.
fn struct_copy_spec(input_type: StreamType, output_type: StreamType) -> DsrvSpecification {
    let (r, y): (VarName, VarName) = ("struct_r".into(), "struct_y".into());
    DsrvSpecification::new(
        BTreeSet::from([r.clone()]),
        BTreeSet::from([y.clone()]),
        BTreeMap::from([(y.clone(), Expr::Var(r.clone()))]),
        BTreeMap::from([(r, input_type), (y, output_type)]),
        BTreeSet::new(),
    )
}

fn struct_type(fields: &[(&str, StreamType)], allow_extra: bool) -> StreamType {
    StreamType::Struct(
        fields
            .iter()
            .map(|(name, typ)| ((*name).into(), typ.clone()))
            .collect(),
        allow_extra,
    )
}

#[test]
fn open_struct_annotation_types_extra_fields() {
    let spec = struct_spec(struct_type(&[("id", StreamType::Int)], true));
    // Checking panics in debug builds if the extra `name` field has no
    // recorded type.
    type_check(spec).expect("an open struct annotation allows extra fields");
}

#[test]
fn test_struct_annotation_matches_fields_by_name() {
    let input = struct_type(&[("id", StreamType::Int), ("name", StreamType::Str)], false);
    let output = struct_type(&[("name", StreamType::Str), ("id", StreamType::Int)], false);
    type_check(struct_copy_spec(input, output)).expect("field order should not matter");
}

#[test]
fn test_open_struct_annotation_accepts_extra_fields() {
    let input = struct_type(&[("id", StreamType::Int), ("name", StreamType::Str)], false);
    let output = struct_type(&[("id", StreamType::Int)], true);
    type_check(struct_copy_spec(input, output))
        .expect("an open struct annotation allows extra fields");
}

#[test]
fn test_closed_struct_annotation_rejects_extra_fields() {
    let input = struct_type(&[("id", StreamType::Int), ("name", StreamType::Str)], false);
    let output = struct_type(&[("id", StreamType::Int)], false);
    type_check(struct_copy_spec(input, output))
        .expect_err("a closed struct annotation rejects extra fields");
}

#[test]
fn test_struct_annotation_rejects_field_mismatch() {
    let robot: VarName = "struct_bad_robot".into();
    let mut exprs = BTreeMap::new();
    exprs.insert(
        robot.clone(),
        Expr::Struct(BTreeMap::from([
            ("id".into(), Expr::Val("not an int").into()),
            ("name".into(), Expr::Val("r2d2").into()),
        ])),
    );
    let mut type_annotations = BTreeMap::new();
    type_annotations.insert(
        robot.clone(),
        StreamType::Struct(
            EcoVec::from(vec![
                ("id".into(), StreamType::Int),
                ("name".into(), StreamType::Str),
            ]),
            false,
        ),
    );
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([robot]),
        exprs,
        type_annotations,
        BTreeSet::new(),
    );

    let errors = type_check(spec).expect_err("field annotation mismatch should fail");
    assert!(!errors.is_empty());
}

#[test]
fn cycles_still_validate_runtime_scopes() {
    let specification = "in source: Str\nout z\n\
                         z = if true then z else dynamic(source: Int, {missing})"
        .parse()
        .unwrap();

    let errors = type_check(specification)
        .expect_err("an unresolved type cycle must not bypass AST validation");

    assert!(errors.iter().any(|error| matches!(
        error,
        SemanticError::InvalidRuntimeScope(message, _, _)
            if message.contains("unknown variable missing")
    )));
}

#[test]
fn forward_dependencies_infer_but_recursive_roots_need_annotations() {
    let checked = type_check("in x:Int\nout a=b+1\naux b=x*2".parse().unwrap()).unwrap();
    for name in ["a", "b"] {
        assert_eq!(
            checked.type_annotations()[&VarName::new(name)],
            StreamType::Int
        );
    }
    assert!(
        type_check(
            "in x:Int\nout c=default(d[1],0)+x\naux d=c"
                .parse()
                .unwrap()
        )
        .is_err()
    );
    type_check(
        "in x:Int\nout c:Int=default(d[1],0)+x\naux d:Int=c"
            .parse()
            .unwrap(),
    )
    .unwrap();
}

#[test]
fn nested_runtime_markers_do_not_manufacture_a_source_type() {
    use crate::lang::dsrv::ast::SyntaxLiteral;
    let y = VarName::new("y");
    let spec = DsrvSpecification::new(
        BTreeSet::new(),
        BTreeSet::from([y.clone()]),
        BTreeMap::from([(
            y.clone(),
            Expr::Val(SyntaxLiteral::Tuple(EcoVec::from([
                SyntaxLiteral::Int(1),
                SyntaxLiteral::NoVal,
            ]))),
        )]),
        BTreeMap::new(),
        BTreeSet::new(),
    );
    let errors = type_check(spec).expect_err("NoVal is runtime state at every literal depth");
    assert!(errors.iter().any(|error| match error {
        SemanticError::UnsupportedLiteral(message, ..) => message.contains("runtime states"),
        SemanticError::UnresolvedType(error) => error.message().contains("runtime markers"),
        _ => false,
    }));
    let checked = type_check("out y=Tuple(1,true)".parse().unwrap()).unwrap();
    assert_eq!(
        checked.var_expr_ref(&y).unwrap().typ(),
        &StreamType::Tuple([StreamType::Int, StreamType::Bool].into_iter().collect())
    );
}
