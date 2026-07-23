//! Gradual type checking: a fixed-point inference driver layered on the
//! expression-level checker, falling back to `Any` for unannotated variables
//! that cannot be given a concrete type.

use super::*;
use crate::DsrvSpecification;
use crate::core::StreamType;
use crate::lang::dsrv::ast::CheckedDsrvSpecification;
use std::collections::{BTreeMap, BTreeSet};

fn gradual_fallback_type(typ: TCType) -> StreamType {
    match typ {
        TCType::EmptyList => StreamType::List(Box::new(StreamType::Any)),
        TCType::EmptyMap => StreamType::Map(Box::new(StreamType::Any)),
        TCType::Unknown | TCType::Any => StreamType::Any,
        TCType::List(inner) => StreamType::List(Box::new(gradual_fallback_type(*inner))),
        TCType::Map(inner) => StreamType::Map(Box::new(gradual_fallback_type(*inner))),
        TCType::Struct(fields, allow_extra) => StreamType::Struct(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), gradual_fallback_type(v.clone())))
                .collect(),
            allow_extra,
        ),
        other => other.to_stream_type().unwrap_or(StreamType::Any),
    }
}

/// Gradual type consistency between a declared stream type and an inferred
/// checker type: `Any` (and unresolved placeholders) are consistent with
/// anything, while concrete types must match structurally. This ensures that
/// concrete static mismatches like `out y: Bool; y = 1` are still rejected
/// instead of being deferred to a doomed runtime cast.
fn gradual_consistent(expected: &StreamType, actual: &TCType) -> bool {
    match (expected, actual) {
        (StreamType::Any, _) | (_, TCType::Any) => true,
        (_, TCType::Unknown) => true,
        (StreamType::List(_), TCType::EmptyList) => true,
        (StreamType::Map(_), TCType::EmptyMap) => true,
        (StreamType::Int, TCType::Int)
        | (StreamType::Float, TCType::Float)
        | (StreamType::Str, TCType::Str)
        | (StreamType::Bool, TCType::Bool)
        | (StreamType::Unit, TCType::Unit) => true,
        (StreamType::List(e), TCType::List(a)) => gradual_consistent(e, a),
        (StreamType::Tuple(elems), TCType::Tuple(actual)) => {
            elems.len() == actual.len()
                && elems
                    .iter()
                    .zip(actual.iter())
                    .all(|(e, a)| gradual_consistent(e, a))
        }
        (StreamType::Map(e), TCType::Map(a)) => gradual_consistent(e, a),
        (StreamType::Struct(ef, _), TCType::Struct(af, _)) => {
            ef.len() == af.len()
                && ef
                    .iter()
                    .zip(af.iter())
                    .all(|((en, et), (an, at))| en == an && gradual_consistent(et, at))
        }
        _ => false,
    }
}

fn can_widen_gradual_error(error: &SemanticError) -> bool {
    match error {
        SemanticError::UndeclaredVariable(_, _) => true,
        SemanticError::TypeError(type_error) => matches!(
            type_error.kind(),
            TypeErrorKind::IfBranchTypeMismatch
                | TypeErrorKind::DefaultTypeMismatch
                | TypeErrorKind::ListElementTypeMismatch
                | TypeErrorKind::MapValueTypeMismatch
                | TypeErrorKind::AnnotationTypeMismatch
        ),
        _ => false,
    }
}

pub fn type_check_gradual(
    mut spec: DsrvSpecification,
    distributed: bool,
) -> SemanticResult<CheckedDsrvSpecification> {
    super::validation::validate_specification(&spec, distributed)?;
    let mut types = spec.type_annotations.clone();
    for input in &spec.input_vars {
        types.entry(input.clone()).or_insert(StreamType::Any);
    }
    let known_vars = spec
        .input_vars
        .iter()
        .chain(spec.exprs.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let assignment_inputs = spec
        .exprs
        .iter()
        .map(|(var, expr)| (var.clone(), expr.free_variables()))
        .collect::<BTreeMap<_, _>>();
    let mut pending = spec.exprs.keys().cloned().collect::<Vec<_>>();
    let mut errors = Vec::new();
    let mut root_types = BTreeMap::new();

    while !pending.is_empty() {
        let mut progressed = false;
        let mut next = Vec::new();
        for var in std::mem::take(&mut pending) {
            let unresolved_dependency = assignment_inputs[&var]
                .iter()
                .filter(|dependency| known_vars.contains(dependency))
                .any(|dependency| !types.contains_key(&dependency));
            if unresolved_dependency {
                next.push(var);
                continue;
            }
            let expr = spec.exprs.get(&var).unwrap();
            let expected_stream = types.get(&var).cloned();
            let expected = expected_stream.as_ref().map(TCType::from_stream_type);
            match super::checker::infer_expression(expr, expected.as_ref(), &types) {
                Ok(actual) => {
                    if let Some(expected) = &expected_stream {
                        root_types.insert(var.clone(), actual.clone());
                        if !gradual_consistent(expected, &actual) {
                            errors.push(SemanticError::type_error_at(
                                TypeErrorKind::AnnotationTypeMismatch,
                                format!(
                                    "Variable {var} has declared type {expected}, but expression has inconsistent type {actual}"
                                ),
                                expr.span(),
                            ));
                        }
                    } else {
                        let inferred = gradual_fallback_type(actual);
                        root_types.insert(var.clone(), TCType::from_stream_type(&inferred));
                        types.insert(var.clone(), inferred);
                    }
                    progressed = true;
                }
                Err(error) if expected_stream.is_none() && can_widen_gradual_error(&error) => {
                    types.insert(var.clone(), StreamType::Any);
                    root_types.insert(var.clone(), TCType::Any);
                    progressed = true;
                }
                Err(error) => errors.push(error),
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        if !progressed {
            for var in next.drain(..) {
                types.entry(var.clone()).or_insert(StreamType::Any);
                root_types.insert(var, TCType::Any);
            }
            break;
        }
        pending = next;
    }

    spec.type_annotations = types.clone();
    let expr_types = super::checker::check_gradual_expr_types(&spec, &root_types, &mut types)?;
    Ok(CheckedDsrvSpecification::new(spec, expr_types))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::BinaryOperator;
    use crate::lang::dsrv::ast::Expr;
    use crate::{Value, VarName};
    use ecow::EcoVec;
    use std::collections::{BTreeMap, BTreeSet};
    use test_log::test;

    #[test]
    fn gradual_type_check_infers_unary_minus_result_type() {
        let checked = type_check_gradual("out z\nz = -1".parse().unwrap(), false).unwrap();
        assert_eq!(
            checked.type_annotations().get(&VarName::new("z")),
            Some(&StreamType::Int)
        );
    }

    #[test]
    fn test_gradual_type_check_infers_unannotated_output() {
        let x: VarName = "x_gradual_infer".into();
        let y: VarName = "y_gradual_infer".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            y.clone(),
            Expr::BinOp(
                Box::new(Expr::Var(x.clone())),
                Box::new(Expr::Val(Value::Int(1))),
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

        let typed = type_check_gradual(spec, false).expect("gradual type check should infer y");
        assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Int));
        assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &TCType::Int);
    }

    #[test]
    fn test_gradual_type_check_casts_untyped_input_to_concrete_use() {
        let x: VarName = "x_gradual_any".into();
        let y: VarName = "y_gradual_any".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            y.clone(),
            Expr::BinOp(
                Box::new(Expr::Var(x.clone())),
                Box::new(Expr::Val(Value::Int(1))),
                BinaryOperator::Add,
            ),
        );
        let spec = DsrvSpecification::new(
            BTreeSet::from([x.clone()]),
            BTreeSet::from([y.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed =
            type_check_gradual(spec, false).expect("gradual type check should accept untyped x");
        assert_eq!(typed.type_annotations().get(&x), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Int));
        assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &TCType::Int);
    }

    #[test]
    fn test_gradual_type_check_preserves_aux_vars() {
        let x: VarName = "gradual_aux_x".into();
        let z: VarName = "gradual_aux_z".into();
        let u: VarName = "gradual_aux_u".into();
        let exprs = BTreeMap::from([
            (u.clone(), Expr::Var(x.clone())),
            (z.clone(), Expr::Var(u.clone())),
        ]);
        let type_annotations = BTreeMap::from([
            (x.clone(), StreamType::Int),
            (u.clone(), StreamType::Int),
            (z.clone(), StreamType::Int),
        ]);
        let spec = DsrvSpecification::new(
            BTreeSet::from([x]),
            BTreeSet::from([z.clone()]),
            exprs,
            type_annotations,
            BTreeSet::from([u.clone()]),
        );

        let typed =
            type_check_gradual(spec, false).expect("gradual type check should preserve aux vars");

        assert_eq!(typed.aux_vars(), &BTreeSet::from([u.clone()]));
        assert_eq!(typed.stream_vars(), &BTreeSet::from([z.clone(), u]));
        assert_eq!(typed.output_vars(), &BTreeSet::from([z]));
    }

    #[test]
    fn test_strict_type_check_still_requires_output_annotation() {
        let y: VarName = "y_strict_missing".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), Expr::Val(Value::Int(1)));
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([y]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        assert!(type_check(spec, false).is_err());
    }

    // --- Gradual typing: inference order, consistency, and casts ---

    #[test]
    fn test_gradual_infers_through_dependency_chain_out_of_order() {
        // `z` is interned first so the checker visits it before `y`, even
        // though it depends on `y`; the fixed-point pass must resolve `y`
        // first and then infer `z` from it.
        let z: VarName = "gradual_chain_z".into();
        let y: VarName = "gradual_chain_y".into();
        let x: VarName = "gradual_chain_x".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            Expr::BinOp(
                Box::new(Expr::Var(y.clone())),
                Box::new(Expr::Val(Value::Int(1))),
                BinaryOperator::Add,
            ),
        );
        exprs.insert(
            y.clone(),
            Expr::BinOp(
                Box::new(Expr::Var(x.clone())),
                Box::new(Expr::Val(Value::Int(1))),
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

        let typed = type_check_gradual(spec, false).expect("dependency chain should be inferred");
        assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Int));
        assert_eq!(typed.type_annotations().get(&z), Some(&StreamType::Int));
        assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &TCType::Int);
        assert_eq!(typed.var_expr_ref(&z).unwrap().typ(), &TCType::Int);
    }

    #[test]
    fn test_gradual_empty_containers_fall_back_to_any_elements() {
        let xs: VarName = "gradual_empty_xs".into();
        let m: VarName = "gradual_empty_m".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(xs.clone(), Expr::List(EcoVec::new()));
        exprs.insert(m.clone(), Expr::Map(BTreeMap::new()));
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([xs.clone(), m.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed =
            type_check_gradual(spec, false).expect("empty containers should fall back to Any");
        assert_eq!(
            typed.type_annotations().get(&xs),
            Some(&StreamType::List(Box::new(StreamType::Any)))
        );
        assert_eq!(
            typed.type_annotations().get(&m),
            Some(&StreamType::Map(Box::new(StreamType::Any)))
        );
        assert_eq!(
            typed.var_expr_ref(&xs).unwrap().typ(),
            &TCType::list(TCType::Any)
        );
        assert_eq!(
            typed.var_expr_ref(&m).unwrap().typ(),
            &TCType::map(TCType::Any)
        );
    }

    #[test]
    fn test_gradual_rejects_noval_literal_ast() {
        let x: VarName = "gradual_noval_x".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(x.clone(), Expr::Val(Value::NoVal));
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([x]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let errors =
            type_check_gradual(spec, false).expect_err("NoVal literal AST should be rejected");
        assert!(matches!(
            errors.as_slice(),
            [SemanticError::UnsupportedLiteral(_, _)]
        ));
    }

    #[test]
    fn test_gradual_rejects_concrete_annotation_mismatch() {
        // A concrete static mismatch must remain an error rather than being
        // deferred to a runtime cast that can never succeed.
        let y: VarName = "gradual_mismatch_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), Expr::Val(Value::Int(1)));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(y.clone(), StreamType::Bool);
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([y]),
            exprs,
            type_annotations,
            BTreeSet::new(),
        );

        assert!(type_check_gradual(spec, false).is_err());
    }

    #[test]
    fn test_gradual_rejects_inconsistent_container_annotation() {
        let xs: VarName = "gradual_bad_xs".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            xs.clone(),
            Expr::List(EcoVec::from(vec![Expr::Val(Value::Str("a".into())).into()])),
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

        assert!(type_check_gradual(spec, false).is_err());
    }

    #[test]
    fn test_gradual_any_annotation_accepts_any_expression() {
        let y: VarName = "gradual_any_annot_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), Expr::Val(Value::Str("hello".into())));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(y.clone(), StreamType::Any);
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([y.clone()]),
            exprs,
            type_annotations,
            BTreeSet::new(),
        );

        let typed =
            type_check_gradual(spec, false).expect("Any annotation should accept any value");
        assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Any));
        // The expression keeps its precise type; only the declaration is Any.
        assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &TCType::Str);
    }

    #[test]
    fn test_gradual_annotated_contradiction_is_error() {
        // 1 + "a" is a contradiction; with an annotation it must hard-fail.
        let z: VarName = "gradual_contradiction_typed_z".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            Expr::BinOp(
                Box::new(Expr::Val(Value::Int(1))),
                Box::new(Expr::Val(Value::Str("a".into()))),
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

        let errors = type_check_gradual(spec, false).expect_err("contradiction should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_unannotated_contradiction_is_error() {
        // Even without an annotation, a concrete contradiction such as
        // `1 + "a"` can never succeed at runtime and must not be widened.
        let z: VarName = "gradual_contradiction_any_z".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            Expr::BinOp(
                Box::new(Expr::Val(Value::Int(1))),
                Box::new(Expr::Val(Value::Str("a".into()))),
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

        let errors = type_check_gradual(spec, false).expect_err("contradiction should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_heterogeneous_list_falls_back_to_any() {
        // Without a homogeneous type annotation, a concrete mixed list is a
        // valid untyped value and may be handled by the gradual Any path.
        let xs: VarName = "gradual_heterogeneous_list_xs".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            xs.clone(),
            Expr::List(EcoVec::from(vec![
                Expr::Val(Value::Int(1)).into(),
                Expr::Val(Value::Str("a".into())).into(),
            ])),
        );
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([xs.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed =
            type_check_gradual(spec, false).expect("heterogeneous list should widen to Any");
        assert_eq!(typed.type_annotations().get(&xs), Some(&StreamType::Any));
        assert_eq!(typed.var_expr_ref(&xs).unwrap().typ(), &TCType::Any);
    }

    #[test]
    fn test_gradual_heterogeneous_list_rejects_homogeneous_annotation() {
        let xs: VarName = "gradual_heterogeneous_list_annotated_xs".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            xs.clone(),
            Expr::List(EcoVec::from(vec![
                Expr::Val(Value::Int(1)).into(),
                Expr::Val(Value::Str("a".into())).into(),
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

        let errors =
            type_check_gradual(spec, false).expect_err("homogeneous annotation should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_heterogeneous_map_falls_back_to_any() {
        // Without a homogeneous type annotation, a concrete mixed map is a
        // valid untyped value and may be handled by the gradual Any path.
        let m: VarName = "gradual_heterogeneous_map_m".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            m.clone(),
            Expr::Map(BTreeMap::from([
                ("x".into(), Expr::Val(Value::Int(1)).into()),
                ("y".into(), Expr::Val(Value::Str("a".into())).into()),
            ])),
        );
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([m.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed = type_check_gradual(spec, false).expect("heterogeneous map should widen to Any");
        assert_eq!(typed.type_annotations().get(&m), Some(&StreamType::Any));
        assert_eq!(typed.var_expr_ref(&m).unwrap().typ(), &TCType::Any);
    }

    #[test]
    fn test_gradual_heterogeneous_map_rejects_homogeneous_annotation() {
        let m: VarName = "gradual_heterogeneous_map_annotated_m".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            m.clone(),
            Expr::Map(BTreeMap::from([
                ("x".into(), Expr::Val(Value::Int(1)).into()),
                ("y".into(), Expr::Val(Value::Str("a".into())).into()),
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

        let errors =
            type_check_gradual(spec, false).expect_err("homogeneous annotation should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_infers_unannotated_struct_fields() {
        let robot: VarName = "gradual_struct_robot".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            Expr::Struct(BTreeMap::from([
                ("id".into(), Expr::Val(Value::Int(1)).into()),
                ("name".into(), Expr::Val(Value::Str("r2d2".into())).into()),
            ])),
        );
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([robot.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed = type_check_gradual(spec, false).expect("struct fields should be inferred");
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
            &TCType::Struct(
                EcoVec::from(vec![
                    ("id".into(), TCType::Int),
                    ("name".into(), TCType::Str)
                ]),
                false
            )
        );
    }

    #[test]
    fn test_gradual_inferred_struct_can_feed_field_access() {
        let robot: VarName = "gradual_struct_a_robot".into();
        let name: VarName = "gradual_struct_z_name".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            Expr::Struct(BTreeMap::from([
                ("id".into(), Expr::Val(Value::Int(1)).into()),
                ("name".into(), Expr::Val(Value::Str("r2d2".into())).into()),
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

        let typed =
            type_check_gradual(spec, false).expect("field access should use inferred struct type");
        assert_eq!(typed.type_annotations().get(&name), Some(&StreamType::Str));
        assert_eq!(typed.var_expr_ref(&name).unwrap().typ(), &TCType::Str);
    }

    #[test]
    fn test_gradual_struct_annotation_rejects_field_mismatch() {
        let robot: VarName = "gradual_struct_bad_robot".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            Expr::Struct(BTreeMap::from([
                (
                    "id".into(),
                    Expr::Val(Value::Str("not an int".into())).into(),
                ),
                ("name".into(), Expr::Val(Value::Str("r2d2".into())).into()),
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

        let errors =
            type_check_gradual(spec, false).expect_err("field annotation mismatch should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_heterogeneous_if_falls_back_to_any() {
        // A heterogeneous conditional is not a guaranteed runtime crash: only
        // one branch is selected at each timestamp, so gradual mode may defer
        // the result type to the untyped runtime.
        let z: VarName = "gradual_heterogeneous_if_z".into();
        let c: VarName = "gradual_heterogeneous_if_c".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            Expr::If(
                Box::new(Expr::Var(c.clone())),
                Box::new(Expr::Val(Value::Int(1))),
                Box::new(Expr::Val(Value::Str("a".into()))),
            ),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(c, StreamType::Bool);
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([z.clone()]),
            exprs,
            type_annotations,
            BTreeSet::new(),
        );

        let typed = type_check_gradual(spec, false).expect("heterogeneous if should widen to Any");
        assert_eq!(typed.type_annotations().get(&z), Some(&StreamType::Any));
        assert_eq!(typed.var_expr_ref(&z).unwrap().typ(), &TCType::Any);
    }

    #[test]
    fn test_gradual_passthrough_of_untyped_input_is_any() {
        let x: VarName = "gradual_pass_x".into();
        let y: VarName = "gradual_pass_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), Expr::Var(x.clone()));
        let spec = DsrvSpecification::new(
            BTreeSet::from([x.clone()]),
            BTreeSet::from([y.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed = type_check_gradual(spec, false).expect("passthrough should type check");
        assert_eq!(typed.type_annotations().get(&x), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations().get(&y), Some(&StreamType::Any));
        assert_eq!(typed.var_expr_ref(&y).unwrap().typ(), &TCType::Any);
    }

    #[test]
    fn test_gradual_any_plus_any_falls_back_to_any() {
        let a: VarName = "gradual_dd_a".into();
        let b: VarName = "gradual_dd_b".into();
        let z: VarName = "gradual_dd_z".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            Expr::BinOp(
                Box::new(Expr::Var(a.clone())),
                Box::new(Expr::Var(b.clone())),
                BinaryOperator::Add,
            ),
        );
        let spec = DsrvSpecification::new(
            BTreeSet::from([a.clone(), b.clone()]),
            BTreeSet::from([z.clone()]),
            exprs,
            BTreeMap::new(),
            BTreeSet::new(),
        );

        let typed = type_check_gradual(spec, false).expect("Any + Any widens to Any");
        assert_eq!(typed.type_annotations().get(&a), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations().get(&b), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations().get(&z), Some(&StreamType::Any));
    }
}
