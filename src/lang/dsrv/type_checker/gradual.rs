//! Gradual type checking: a fixed-point inference driver layered on the
//! expression-level checker, falling back to `Any` for unannotated variables
//! that cannot be given a concrete type.

use super::expr::{cast_to_type, coerce_empty_list, coerce_empty_map};
use super::*;
use crate::core::StreamType;
use crate::lang::dsrv::ast::SpannedExpr;
use crate::{UntypedDsrvSpecification, VarName};
use std::collections::BTreeMap;

struct PendingAssignment {
    var: VarName,
    expr: SpannedExpr,
    errors: SemanticErrors,
}

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

/// Replace unresolved empty-container placeholder element types with `Any` so
/// that inferred gradual expressions have concrete types at runtime.
fn coerce_gradual_placeholders(te: SExprTE) -> SExprTE {
    match te {
        SExprTE::List(tl) if *tl.element_type() == TCType::EmptyList => {
            SExprTE::List(coerce_empty_list(tl, TCType::Any))
        }
        SExprTE::Map(tm) if *tm.value_type() == TCType::EmptyMap => {
            SExprTE::Map(coerce_empty_map(tm, TCType::Any))
        }
        other => other,
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
        ),
        _ => false,
    }
}

fn can_widen_gradual_errors(errors: &[SemanticError]) -> bool {
    !errors.is_empty() && errors.iter().all(can_widen_gradual_error)
}

pub fn type_check_gradual(
    spec: UntypedDsrvSpecification,
) -> SemanticResult<TypedDsrvSpecification> {
    let mut type_context = spec.type_annotations.clone();
    for var in spec.input_vars.iter() {
        type_context.entry(var.clone()).or_insert(StreamType::Any);
    }

    let mut typed_exprs: BTreeMap<VarName, SExprTE> = BTreeMap::new();
    let mut errors = vec![];
    let mut pending: Vec<PendingAssignment> = spec
        .exprs
        .iter()
        .map(|(var, expr)| PendingAssignment {
            var: var.clone(),
            expr: expr.clone(),
            errors: vec![],
        })
        .collect();

    'outer: while !pending.is_empty() {
        // Fixed-point passes: repeatedly attempt to type check the remaining
        // assignments so that inference does not depend on declaration order.
        loop {
            let mut progressed = false;
            let mut next_pending = Vec::new();
            for pending_assignment in std::mem::take(&mut pending) {
                let PendingAssignment { var, expr, .. } = pending_assignment;
                let expected = type_context.get(&var).cloned();
                let mut ctx = type_context.clone();
                let mut local_errors = vec![];
                match expr.type_check_raw(expected.as_ref(), &mut ctx, &mut local_errors) {
                    Ok(te) => {
                        let actual = extract_type(&te);
                        match expected {
                            Some(expected_ty) => {
                                if gradual_consistent(&expected_ty, &actual) {
                                    typed_exprs.insert(var, cast_to_type(te, &expected_ty));
                                    progressed = true;
                                } else {
                                    errors.push(SemanticError::type_error(TypeErrorKind::AnnotationTypeMismatch, format!(
                                        "Variable {:?} has declared type {:?}, but expression has inconsistent type {:?}",
                                        var, expected_ty, actual
                                    )));
                                }
                            }
                            None => {
                                let final_te = coerce_gradual_placeholders(te);
                                let inferred = gradual_fallback_type(extract_type(&final_te));
                                type_context.insert(var.clone(), inferred);
                                typed_exprs.insert(var, final_te);
                                progressed = true;
                            }
                        }
                    }
                    Err(()) => next_pending.push(PendingAssignment {
                        var,
                        expr,
                        errors: local_errors,
                    }),
                }
            }
            pending = next_pending;
            if pending.is_empty() {
                break 'outer;
            }
            if !progressed {
                break;
            }
        }

        // No further progress is possible. Fall back the first unannotated
        // unresolved assignment to Any, which may unblock the others.
        if let Some(pos) = pending
            .iter()
            .position(|pending_assignment| !type_context.contains_key(&pending_assignment.var))
        {
            let PendingAssignment {
                var,
                expr,
                errors: local_errors,
            } = pending.remove(pos);
            if can_widen_gradual_errors(&local_errors) {
                typed_exprs.insert(var.clone(), SExprTE::Any(SExprAny::Expr(expr.node)));
                type_context.insert(var, StreamType::Any);
                continue 'outer;
            }
            errors.extend(local_errors);
            break 'outer;
        }

        // All remaining unresolved assignments are annotated: hard errors.
        for PendingAssignment { var, expr, .. } in std::mem::take(&mut pending) {
            let expected = type_context
                .get(&var)
                .cloned()
                .expect("only annotated variables can remain pending here");
            let mut ctx = type_context.clone();
            // Re-run the failing check to surface its underlying errors.
            if expr
                .type_check_raw(Some(&expected), &mut ctx, &mut errors)
                .is_ok()
            {
                errors.push(SemanticError::unresolved_type(
                    UnresolvedTypeKind::VariableType,
                    format!("Could not resolve a type for variable {:?}", var),
                ));
            }
        }
        break 'outer;
    }

    if errors.is_empty() && typed_exprs.len() == spec.exprs.len() {
        Ok(TypedDsrvSpecification {
            input_vars: spec.input_vars.clone(),
            output_vars: spec.output_vars.clone(),
            aux_vars: spec.aux_vars.clone(),
            stream_vars: spec.stream_vars.clone(),
            exprs: typed_exprs,
            type_annotations: type_context,
        })
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::ast::{
        BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, NumericalBinOp, SBinOp, SExpr, StrBinOp,
    };
    use crate::{Specification, Value};
    use ecow::EcoVec;
    use std::collections::{BTreeMap, BTreeSet};
    use test_log::test;

    #[test]
    fn test_gradual_type_check_infers_unannotated_output() {
        let x: VarName = "x_gradual_infer".into();
        let y: VarName = "y_gradual_infer".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            y.clone(),
            SExpr::BinOp(
                Box::new(SExpr::Var(x.clone()).into()),
                Box::new(SExpr::Val(Value::Int(1)).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(x.clone(), StreamType::Int);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x]),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("gradual type check should infer y");
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Int));
        assert!(matches!(typed.var_expr(&y), Some(SExprTE::Int(_))));
    }

    #[test]
    fn test_gradual_type_check_casts_untyped_input_to_concrete_use() {
        let x: VarName = "x_gradual_any".into();
        let y: VarName = "y_gradual_any".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            y.clone(),
            SExpr::BinOp(
                Box::new(SExpr::Var(x.clone()).into()),
                Box::new(SExpr::Val(Value::Int(1)).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x.clone()]),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("gradual type check should accept untyped x");
        assert_eq!(typed.type_annotations.get(&x), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Int));
        let Some(SExprTE::Int(SExprInt::BinOp(lhs, _, _))) = typed.var_expr(&y) else {
            panic!("expected y to be an int binop");
        };
        assert!(matches!(*lhs, SExprInt::Cast(_)));
    }

    #[test]
    fn test_gradual_type_check_preserves_aux_vars() {
        let x: VarName = "gradual_aux_x".into();
        let z: VarName = "gradual_aux_z".into();
        let u: VarName = "gradual_aux_u".into();
        let exprs = BTreeMap::from([
            (u.clone(), SExpr::Var(x.clone())),
            (z.clone(), SExpr::Var(u.clone())),
        ]);
        let type_annotations = BTreeMap::from([
            (x.clone(), StreamType::Int),
            (u.clone(), StreamType::Int),
            (z.clone(), StreamType::Int),
        ]);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x]),
            output_vars: BTreeSet::from([z.clone()]),
            aux_vars: BTreeSet::from([u.clone()]),
            stream_vars: BTreeSet::from([z.clone(), u.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
        };

        let typed = type_check_gradual(spec).expect("gradual type check should preserve aux vars");

        assert_eq!(typed.aux_vars(), BTreeSet::from([u.clone()]));
        assert_eq!(typed.stream_vars(), BTreeSet::from([z.clone(), u]));
        assert_eq!(typed.output_vars(), BTreeSet::from([z]));
    }

    #[test]
    fn test_strict_type_check_still_requires_output_annotation() {
        let y: VarName = "y_strict_missing".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), SExpr::Val(Value::Int(1)));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        assert!(type_check(spec).is_err());
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
            SExpr::BinOp(
                Box::new(SExpr::Var(y.clone()).into()),
                Box::new(SExpr::Val(Value::Int(1)).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        exprs.insert(
            y.clone(),
            SExpr::BinOp(
                Box::new(SExpr::Var(x.clone()).into()),
                Box::new(SExpr::Val(Value::Int(1)).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(x.clone(), StreamType::Int);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x]),
            output_vars: BTreeSet::from([y.clone(), z.clone()]),
            stream_vars: BTreeSet::from([y.clone(), z.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("dependency chain should be inferred");
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Int));
        assert_eq!(typed.type_annotations.get(&z), Some(&StreamType::Int));
        assert!(matches!(typed.var_expr(&y), Some(SExprTE::Int(_))));
        assert!(matches!(typed.var_expr(&z), Some(SExprTE::Int(_))));
    }

    #[test]
    fn test_gradual_empty_containers_fall_back_to_any_elements() {
        let xs: VarName = "gradual_empty_xs".into();
        let m: VarName = "gradual_empty_m".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(xs.clone(), SExpr::List(EcoVec::new()));
        exprs.insert(m.clone(), SExpr::Map(BTreeMap::new()));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([xs.clone(), m.clone()]),
            stream_vars: BTreeSet::from([xs.clone(), m.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("empty containers should fall back to Any");
        assert_eq!(
            typed.type_annotations.get(&xs),
            Some(&StreamType::List(Box::new(StreamType::Any)))
        );
        assert_eq!(
            typed.type_annotations.get(&m),
            Some(&StreamType::Map(Box::new(StreamType::Any)))
        );
        // The typed expressions must have concrete (Any) element types so they
        // can be converted to stream types at runtime.
        let Some(SExprTE::List(tl)) = typed.var_expr(&xs) else {
            panic!("expected a typed list expression for xs");
        };
        assert_eq!(*tl.element_type(), TCType::Any);
        let Some(SExprTE::Map(tm)) = typed.var_expr(&m) else {
            panic!("expected a typed map expression for m");
        };
        assert_eq!(*tm.value_type(), TCType::Any);
    }

    #[test]
    fn test_gradual_rejects_noval_literal_ast() {
        let x: VarName = "gradual_noval_x".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(x.clone(), SExpr::Val(Value::NoVal));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([x.clone()]),
            stream_vars: BTreeSet::from([x.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("NoVal literal AST should be rejected");
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
        exprs.insert(y.clone(), SExpr::Val(Value::Int(1)));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(y.clone(), StreamType::Bool);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        assert!(type_check_gradual(spec).is_err());
    }

    #[test]
    fn test_gradual_rejects_inconsistent_container_annotation() {
        let xs: VarName = "gradual_bad_xs".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            xs.clone(),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Str("a".into())).into(),
            ])),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(xs.clone(), StreamType::List(Box::new(StreamType::Int)));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([xs.clone()]),
            stream_vars: BTreeSet::from([xs.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        assert!(type_check_gradual(spec).is_err());
    }

    #[test]
    fn test_gradual_any_annotation_accepts_any_expression() {
        let y: VarName = "gradual_any_annot_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), SExpr::Val(Value::Str("hello".into())));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(y.clone(), StreamType::Any);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("Any annotation should accept any value");
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Any));
        // The expression keeps its precise type; only the declaration is Any.
        assert!(matches!(typed.var_expr(&y), Some(SExprTE::Str(_))));
    }

    #[test]
    fn test_gradual_annotated_contradiction_is_error() {
        // 1 + "a" is a contradiction; with an annotation it must hard-fail.
        let z: VarName = "gradual_contradiction_typed_z".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1)).into()),
                Box::new(SExpr::Val(Value::Str("a".into())).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(z.clone(), StreamType::Int);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([z.clone()]),
            stream_vars: BTreeSet::from([z]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("contradiction should fail");
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
            SExpr::BinOp(
                Box::new(SExpr::Val(Value::Int(1)).into()),
                Box::new(SExpr::Val(Value::Str("a".into())).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([z.clone()]),
            stream_vars: BTreeSet::from([z.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("contradiction should fail");
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
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)).into(),
                SExpr::Val(Value::Str("a".into())).into(),
            ])),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([xs.clone()]),
            stream_vars: BTreeSet::from([xs.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("heterogeneous list should widen to Any");
        assert_eq!(typed.type_annotations.get(&xs), Some(&StreamType::Any));
        assert!(matches!(
            typed.var_expr(&xs),
            Some(SExprTE::Any(SExprAny::Expr(_)))
        ));
    }

    #[test]
    fn test_gradual_heterogeneous_list_rejects_homogeneous_annotation() {
        let xs: VarName = "gradual_heterogeneous_list_annotated_xs".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            xs.clone(),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)).into(),
                SExpr::Val(Value::Str("a".into())).into(),
            ])),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(xs.clone(), StreamType::List(Box::new(StreamType::Int)));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([xs.clone()]),
            stream_vars: BTreeSet::from([xs]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("homogeneous annotation should fail");
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
            SExpr::Map(BTreeMap::from([
                ("x".into(), SExpr::Val(Value::Int(1)).into()),
                ("y".into(), SExpr::Val(Value::Str("a".into())).into()),
            ])),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([m.clone()]),
            stream_vars: BTreeSet::from([m.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("heterogeneous map should widen to Any");
        assert_eq!(typed.type_annotations.get(&m), Some(&StreamType::Any));
        assert!(matches!(
            typed.var_expr(&m),
            Some(SExprTE::Any(SExprAny::Expr(_)))
        ));
    }

    #[test]
    fn test_gradual_heterogeneous_map_rejects_homogeneous_annotation() {
        let m: VarName = "gradual_heterogeneous_map_annotated_m".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            m.clone(),
            SExpr::Map(BTreeMap::from([
                ("x".into(), SExpr::Val(Value::Int(1)).into()),
                ("y".into(), SExpr::Val(Value::Str("a".into())).into()),
            ])),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(m.clone(), StreamType::Map(Box::new(StreamType::Int)));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([m.clone()]),
            stream_vars: BTreeSet::from([m]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("homogeneous annotation should fail");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_gradual_infers_unannotated_struct_fields() {
        let robot: VarName = "gradual_struct_robot".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            SExpr::Struct(BTreeMap::from([
                ("id".into(), SExpr::Val(Value::Int(1)).into()),
                ("name".into(), SExpr::Val(Value::Str("r2d2".into())).into()),
            ])),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([robot.clone()]),
            stream_vars: BTreeSet::from([robot.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("struct fields should be inferred");
        assert_eq!(
            typed.type_annotations.get(&robot),
            Some(&StreamType::Struct(
                EcoVec::from(vec![
                    ("id".into(), StreamType::Int),
                    ("name".into(), StreamType::Str),
                ]),
                false,
            ))
        );
        let Some(SExprTE::Struct(st)) = typed.var_expr(&robot) else {
            panic!("expected inferred struct expression");
        };
        assert_eq!(
            st.typ_map,
            EcoVec::from(vec![
                ("id".into(), TCType::Int),
                ("name".into(), TCType::Str)
            ])
        );
    }

    #[test]
    fn test_gradual_inferred_struct_can_feed_field_access() {
        let robot: VarName = "gradual_struct_a_robot".into();
        let name: VarName = "gradual_struct_z_name".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            SExpr::Struct(BTreeMap::from([
                ("id".into(), SExpr::Val(Value::Int(1)).into()),
                ("name".into(), SExpr::Val(Value::Str("r2d2".into())).into()),
            ])),
        );
        exprs.insert(
            name.clone(),
            SExpr::SGet(Box::new(SExpr::Var(robot.clone()).into()), "name".into()),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([name.clone()]),
            stream_vars: BTreeSet::from([robot.clone(), name.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::from([robot]),
        };

        let typed = type_check_gradual(spec).expect("field access should use inferred struct type");
        assert_eq!(typed.type_annotations.get(&name), Some(&StreamType::Str));
        assert!(matches!(typed.var_expr(&name), Some(SExprTE::Str(_))));
    }

    #[test]
    fn test_gradual_struct_annotation_rejects_field_mismatch() {
        let robot: VarName = "gradual_struct_bad_robot".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            robot.clone(),
            SExpr::Struct(BTreeMap::from([
                (
                    "id".into(),
                    SExpr::Val(Value::Str("not an int".into())).into(),
                ),
                ("name".into(), SExpr::Val(Value::Str("r2d2".into())).into()),
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
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([robot.clone()]),
            stream_vars: BTreeSet::from([robot]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let errors = type_check_gradual(spec).expect_err("field annotation mismatch should fail");
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
            SExpr::If(
                Box::new(SExpr::Var(c.clone()).into()),
                Box::new(SExpr::Val(Value::Int(1)).into()),
                Box::new(SExpr::Val(Value::Str("a".into())).into()),
            ),
        );
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(c, StreamType::Bool);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([z.clone()]),
            stream_vars: BTreeSet::from([z.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("heterogeneous if should widen to Any");
        assert_eq!(typed.type_annotations.get(&z), Some(&StreamType::Any));
        assert!(matches!(
            typed.var_expr(&z),
            Some(SExprTE::Any(SExprAny::Expr(_)))
        ));
    }

    #[test]
    fn test_gradual_passthrough_of_untyped_input_is_any() {
        let x: VarName = "gradual_pass_x".into();
        let y: VarName = "gradual_pass_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), SExpr::Var(x.clone()));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x.clone()]),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("passthrough should type check");
        assert_eq!(typed.type_annotations.get(&x), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Any));
        assert!(matches!(
            typed.var_expr(&y),
            Some(SExprTE::Any(SExprAny::Var(_)))
        ));
    }

    #[test]
    fn test_gradual_any_plus_any_falls_back_to_any() {
        let a: VarName = "gradual_dd_a".into();
        let b: VarName = "gradual_dd_b".into();
        let z: VarName = "gradual_dd_z".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(
            z.clone(),
            SExpr::BinOp(
                Box::new(SExpr::Var(a.clone()).into()),
                Box::new(SExpr::Var(b.clone()).into()),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        );
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([a.clone(), b.clone()]),
            output_vars: BTreeSet::from([z.clone()]),
            stream_vars: BTreeSet::from([z.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations: BTreeMap::new(),
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("Any + Any widens to Any");
        assert_eq!(typed.type_annotations.get(&a), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations.get(&b), Some(&StreamType::Any));
        assert_eq!(typed.type_annotations.get(&z), Some(&StreamType::Any));
    }

    #[test]
    fn test_gradual_any_operand_comparisons_cast_to_concrete() {
        let x: VarName = "gradual_cmp_x".into();
        for op in [
            CompBinOp::Eq,
            CompBinOp::Le,
            CompBinOp::Lt,
            CompBinOp::Ge,
            CompBinOp::Gt,
        ] {
            let mut ctx = TypeInfo::new();
            ctx.insert(x.clone(), StreamType::Any);
            let expr = SExpr::BinOp(
                Box::new(SExpr::Var(x.clone()).into()),
                Box::new(SExpr::Val(Value::Int(10)).into()),
                SBinOp::COp(op.clone()),
            );
            let te = expr
                .type_check(&mut ctx)
                .expect("comparison with Any operand should type check");
            assert_eq!(extract_type(&te), TCType::Bool);
            let SExprTE::Bool(SExprBool::Cmp(got_op, lhs, rhs)) = te else {
                panic!("expected a typed comparison");
            };
            assert_eq!(got_op, op);
            assert!(
                matches!(*lhs, SExprTE::Int(SExprInt::Cast(_))),
                "Any operand should be cast to Int"
            );
            assert!(matches!(*rhs, SExprTE::Int(SExprInt::Val(_))));
        }
    }

    #[test]
    fn test_gradual_any_operand_primitive_ops_cast_to_concrete() {
        let x: VarName = "gradual_ops_x".into();
        let mut ctx = TypeInfo::new();
        ctx.insert(x.clone(), StreamType::Any);

        // Int arithmetic: x * 2 : Int with a cast on the Any side
        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone()).into()),
            Box::new(SExpr::Val(Value::Int(2)).into()),
            SBinOp::NOp(NumericalBinOp::Mul),
        );
        let te = expr.type_check(&mut ctx).expect("Any * Int should check");
        let SExprTE::Int(SExprInt::BinOp(lhs, _, IntBinOp::Mul)) = te else {
            panic!("expected an Int binop");
        };
        assert!(matches!(*lhs, SExprInt::Cast(_)));

        // Float arithmetic: x + 1.5 : Float
        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone()).into()),
            Box::new(SExpr::Val(Value::Float(1.5)).into()),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let te = expr.type_check(&mut ctx).expect("Any + Float should check");
        let SExprTE::Float(SExprFloat::BinOp(lhs, _, FloatBinOp::Add)) = te else {
            panic!("expected a Float binop");
        };
        assert!(matches!(*lhs, SExprFloat::Cast(_)));

        // Boolean operation: x && true : Bool
        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone()).into()),
            Box::new(SExpr::Val(Value::Bool(true)).into()),
            SBinOp::BOp(BoolBinOp::And),
        );
        let te = expr.type_check(&mut ctx).expect("Any && Bool should check");
        let SExprTE::Bool(SExprBool::BinOp(lhs, _, BoolBinOp::And)) = te else {
            panic!("expected a Bool binop");
        };
        assert!(matches!(*lhs, SExprBool::Cast(_)));

        // String concatenation: x ++ "s" : Str
        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone()).into()),
            Box::new(SExpr::Val(Value::Str("s".into())).into()),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let te = expr.type_check(&mut ctx).expect("Any ++ Str should check");
        let SExprTE::Str(SExprStr::BinOp(lhs, _, StrBinOp::Concat)) = te else {
            panic!("expected a Str binop");
        };
        assert!(matches!(*lhs, SExprStr::Cast(_)));
    }

    #[test]
    fn test_gradual_consistent_relation() {
        // Any is consistent with everything
        assert!(gradual_consistent(&StreamType::Any, &TCType::Int));
        assert!(gradual_consistent(&StreamType::Int, &TCType::Any));
        // Concrete types must match
        assert!(gradual_consistent(&StreamType::Int, &TCType::Int));
        assert!(!gradual_consistent(&StreamType::Int, &TCType::Bool));
        // Containers compare structurally with Any permeating inwards
        assert!(gradual_consistent(
            &StreamType::List(Box::new(StreamType::Int)),
            &TCType::List(Box::new(TCType::Any))
        ));
        assert!(gradual_consistent(
            &StreamType::List(Box::new(StreamType::Int)),
            &TCType::EmptyList
        ));
        assert!(!gradual_consistent(
            &StreamType::List(Box::new(StreamType::Int)),
            &TCType::List(Box::new(TCType::Str))
        ));
        assert!(!gradual_consistent(
            &StreamType::Map(Box::new(StreamType::Int)),
            &TCType::List(Box::new(TCType::Int))
        ));
    }

    #[test]
    fn test_gradual_keeps_explicit_input_annotations() {
        let x: VarName = "gradual_keep_x".into();
        let y: VarName = "gradual_keep_y".into();
        let mut exprs = BTreeMap::new();
        exprs.insert(y.clone(), SExpr::Var(x.clone()));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(x.clone(), StreamType::Str);
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::from([x.clone()]),
            output_vars: BTreeSet::from([y.clone()]),
            stream_vars: BTreeSet::from([y.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };

        let typed = type_check_gradual(spec).expect("annotated input passthrough");
        // The input keeps its explicit type rather than being widened to Any,
        // and the output inherits the concrete inferred type.
        assert_eq!(typed.type_annotations.get(&x), Some(&StreamType::Str));
        assert_eq!(typed.type_annotations.get(&y), Some(&StreamType::Str));
        assert!(matches!(typed.var_expr(&y), Some(SExprTE::Str(_))));
    }
}
