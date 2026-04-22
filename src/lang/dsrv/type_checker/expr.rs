//! Expression-level type checking: the `TypeCheckableHelper` implementations
//! for untyped expressions, plus the coercion/unification helpers shared by
//! the strict and gradual drivers.

use super::*;
use crate::core::{StreamType, StreamTypeAscription};
use crate::lang::dsrv::ast::{CompBinOp, SBinOp, SExpr, SpannedExpr};
use crate::{Value, VarName};
use ecow::{EcoString, EcoVec};
use std::collections::BTreeMap;

impl TypeCheckableHelper<SExprTE> for Value {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            Value::Int(v) => Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(*v)))),
            Value::Float(v) => Ok(SExprTE::Float(SExprFloat::Val(PartialStreamValue::Known(
                *v,
            )))),
            Value::Str(v) => Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                v.into(),
            )))),
            Value::Bool(v) => Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(*v)))),
            Value::List(elems) => {
                if elems.is_empty() {
                    // Use expected type to resolve the element type of an empty list
                    // Prefer the expected type when available; otherwise fall
                    // back to Poly so that type-erasing operations like
                    // is_defined and len can still accept the empty list.
                    let inner = match expected {
                        Some(StreamType::List(inner)) => TCType::from_stream_type(inner.as_ref()),
                        _ => TCType::EmptyList,
                    };
                    Ok(SExprTE::List(make_list_literal(vec![], inner)))
                } else {
                    // Derive inner expected type from expected (if it is a list type)
                    let inner_expected = match expected {
                        Some(StreamType::List(inner)) => Some(inner.as_ref()),
                        _ => None,
                    };

                    // Type-check first element to determine the element type
                    let first_te = elems[0].type_check_raw(inner_expected, ctx, errs)?;
                    let first_type = extract_type(&first_te);

                    // Type-check remaining elements and check consistency with first element's type
                    let mut typed_elems = vec![first_te];
                    for elem in elems.iter().skip(1) {
                        let elem_te = elem.type_check_raw(inner_expected, ctx, errs)?;
                        let elem_type = extract_type(&elem_te);
                        if elem_type != first_type {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::ListElementTypeMismatch,
                                format!(
                                    "List element type mismatch: expected {:?} (from first element), got {:?}",
                                    first_type, elem_type
                                ),
                            ));
                            return Err(());
                        }
                        typed_elems.push(elem_te);
                    }

                    Ok(SExprTE::List(make_list_literal(typed_elems, first_type)))
                }
            }
            Value::Map(entries) => type_check_map_literal(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            Value::Unit => Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
            Value::Deferred => {
                errs.push(SemanticError::DeferredError(
                    format!(
                        "Stream expression {:?} not assigned a type before semantic analysis",
                        self
                    ),
                    None,
                ));
                Err(())
            }
            Value::NoVal => {
                errs.push(SemanticError::UnsupportedLiteral(
                    "NoVal is an internal runtime value and is not a supported source literal"
                        .into(),
                    None,
                ));
                Err(())
            }
        }
    }
}

// Type check a binary operation
impl TypeCheckableHelper<SExprTE> for (SBinOp, &SpannedExpr, &SpannedExpr) {
    fn type_check_raw(
        &self,
        _expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (op, se1, se2) = self;
        let se1_check = se1.type_check_raw(None, ctx, errs);
        let se2_check = se2.type_check_raw(None, ctx, errs);

        match (op, se1_check, se2_check) {
            // Integer operations
            (SBinOp::NOp(op), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Int(SExprInt::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::OperatorTypeMismatch,
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }
            // Pure float operations
            (SBinOp::NOp(op), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Float(SExprFloat::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::OperatorTypeMismatch,
                            "Numerical operation not valid on floats".into(),
                        ));
                        Err(())
                    }
                }
            }

            // Gradual numeric operations: two dynamic operands stay dynamic and
            // are checked by the runtime untyped operation.
            (SBinOp::NOp(_), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Any) =>
            {
                Ok(SExprTE::Any(SExprAny::Expr(SExpr::BinOp(
                    Box::new((*se1).clone()),
                    Box::new((*se2).clone()),
                    op.clone(),
                ))))
            }

            // Gradual numeric operations: cast dynamic operands to the concrete side.
            (SBinOp::NOp(op), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Int)
                    || matches!(extract_type(&lhs), TCType::Int)
                        && matches!(extract_type(&rhs), TCType::Any) =>
            {
                let lhs = match cast_to_type(lhs, &StreamType::Int) {
                    SExprTE::Int(e) => e,
                    _ => unreachable!(),
                };
                let rhs = match cast_to_type(rhs, &StreamType::Int) {
                    SExprTE::Int(e) => e,
                    _ => unreachable!(),
                };
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Int(SExprInt::BinOp(
                        Box::new(lhs),
                        Box::new(rhs),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::OperatorTypeMismatch,
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }
            (SBinOp::NOp(op), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Float)
                    || matches!(extract_type(&lhs), TCType::Float)
                        && matches!(extract_type(&rhs), TCType::Any) =>
            {
                let lhs = match cast_to_type(lhs, &StreamType::Float) {
                    SExprTE::Float(e) => e,
                    _ => unreachable!(),
                };
                let rhs = match cast_to_type(rhs, &StreamType::Float) {
                    SExprTE::Float(e) => e,
                    _ => unreachable!(),
                };
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Float(SExprFloat::BinOp(
                        Box::new(lhs),
                        Box::new(rhs),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::OperatorTypeMismatch,
                            "Numerical operation not valid on floats".into(),
                        ));
                        Err(())
                    }
                }
            }

            // Boolean operations
            (SBinOp::BOp(op), Ok(SExprTE::Bool(se1)), Ok(SExprTE::Bool(se2))) => Ok(SExprTE::Bool(
                SExprBool::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),
            (SBinOp::BOp(_), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Any) =>
            {
                Ok(SExprTE::Any(SExprAny::Expr(SExpr::BinOp(
                    Box::new((*se1).clone()),
                    Box::new((*se2).clone()),
                    op.clone(),
                ))))
            }
            (SBinOp::BOp(op), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Bool)
                    || matches!(extract_type(&lhs), TCType::Bool)
                        && matches!(extract_type(&rhs), TCType::Any) =>
            {
                let lhs = match cast_to_type(lhs, &StreamType::Bool) {
                    SExprTE::Bool(e) => e,
                    _ => unreachable!(),
                };
                let rhs = match cast_to_type(rhs, &StreamType::Bool) {
                    SExprTE::Bool(e) => e,
                    _ => unreachable!(),
                };
                Ok(SExprTE::Bool(SExprBool::BinOp(
                    Box::new(lhs),
                    Box::new(rhs),
                    op.clone(),
                )))
            }
            // String operations
            (SBinOp::SOp(op), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => Ok(SExprTE::Str(
                SExprStr::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),
            (SBinOp::SOp(_), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Any) =>
            {
                Ok(SExprTE::Any(SExprAny::Expr(SExpr::BinOp(
                    Box::new((*se1).clone()),
                    Box::new((*se2).clone()),
                    op.clone(),
                ))))
            }
            (SBinOp::SOp(op), Ok(lhs), Ok(rhs))
                if matches!(extract_type(&lhs), TCType::Any)
                    && matches!(extract_type(&rhs), TCType::Str)
                    || matches!(extract_type(&lhs), TCType::Str)
                        && matches!(extract_type(&rhs), TCType::Any) =>
            {
                let lhs = match cast_to_type(lhs, &StreamType::Str) {
                    SExprTE::Str(e) => e,
                    _ => unreachable!(),
                };
                let rhs = match cast_to_type(rhs, &StreamType::Str) {
                    SExprTE::Str(e) => e,
                    _ => unreachable!(),
                };
                Ok(SExprTE::Str(SExprStr::BinOp(
                    Box::new(lhs),
                    Box::new(rhs),
                    op.clone(),
                )))
            }

            // Comparison operations — both operands must have the same type.
            // Eq is valid for all types; ordering (Le/Lt/Ge/Gt) only for Int, Float, Str.
            (SBinOp::COp(op), Ok(ste1), Ok(ste2)) => {
                let ty1 = extract_type(&ste1);
                let ty2 = extract_type(&ste2);
                let (ste1, ste2, ty1) = match (&ty1, &ty2) {
                    (TCType::Any, TCType::Any) => (ste1, ste2, TCType::Any),
                    (TCType::Any, TCType::Int) | (TCType::Int, TCType::Any) => (
                        cast_to_type(ste1, &StreamType::Int),
                        cast_to_type(ste2, &StreamType::Int),
                        TCType::Int,
                    ),
                    (TCType::Any, TCType::Float) | (TCType::Float, TCType::Any) => (
                        cast_to_type(ste1, &StreamType::Float),
                        cast_to_type(ste2, &StreamType::Float),
                        TCType::Float,
                    ),
                    (TCType::Any, TCType::Str) | (TCType::Str, TCType::Any) => (
                        cast_to_type(ste1, &StreamType::Str),
                        cast_to_type(ste2, &StreamType::Str),
                        TCType::Str,
                    ),
                    (TCType::Any, TCType::Bool) | (TCType::Bool, TCType::Any) => (
                        cast_to_type(ste1, &StreamType::Bool),
                        cast_to_type(ste2, &StreamType::Bool),
                        TCType::Bool,
                    ),
                    _ if ty1 == ty2 => (ste1, ste2, ty1),
                    _ => {
                        errs.push(SemanticError::type_error(TypeErrorKind::OperatorTypeMismatch, format!(
                            "Cannot apply comparison {:?} to expressions of different types: {} and {}",
                            op, ty1, ty2
                        )));
                        return Err(());
                    }
                };
                // Ordering comparisons are only valid for Int, Float, Str
                let ordering_ok =
                    matches!(ty1, TCType::Int | TCType::Float | TCType::Str | TCType::Any);
                if *op != CompBinOp::Eq && !ordering_ok {
                    errs.push(SemanticError::type_error(
                        TypeErrorKind::OperatorTypeMismatch,
                        format!("Cannot apply ordering comparison {:?} to type {}", op, ty1),
                    ));
                    return Err(());
                }
                Ok(SExprTE::Bool(SExprBool::Cmp(
                    op.clone(),
                    Box::new(ste1),
                    Box::new(ste2),
                )))
            }

            // Any other case where sub-expressions are Ok, but `op` is not supported
            (_, Ok(ste1), Ok(ste2)) => {
                errs.push(SemanticError::type_error(
                    TypeErrorKind::OperatorTypeMismatch,
                    format!(
                        "Cannot apply binary function {} to expressions of types: {} and {}",
                        op,
                        ste1.display_with_type(),
                        ste2.display_with_type()
                    ),
                ));
                Err(())
            }
            // If the underlying values already result in an error then simply propagate
            _ => Err(()),
        }
    }
}

// Type check a default operation
impl TypeCheckableHelper<SExprTE> for (&SpannedExpr, &SpannedExpr) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (se1, se2) = *self;
        let se1_check = se1.type_check_raw(expected, ctx, errs);
        let se2_check = se2.type_check_raw(expected, ctx, errs);

        match (se1_check, se2_check) {
            (Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Float(se1), SExprTE::Float(se2)) => Ok(SExprTE::Float(
                        SExprFloat::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(
                        SExprBool::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(
                        SExprUnit::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, resolved.clone());
                                let tl2 = coerce_empty_list(tl2, resolved);
                                Ok(SExprTE::List(tl1.typed_default(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(
                                    TypeErrorKind::DefaultTypeMismatch,
                                    format!(
                                        "Cannot create default-expression with two different list types: {} and {}",
                                        t1, t2
                                    ),
                                ));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Map(tm1), SExprTE::Map(tm2)) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, resolved.clone());
                                let tm2 = coerce_empty_map(tm2, resolved);
                                Ok(SExprTE::Map(tm1.typed_default(&tm2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(
                                    TypeErrorKind::DefaultTypeMismatch,
                                    format!(
                                        "Cannot create default-expression with two different map types: {} and {}",
                                        t1, t2
                                    ),
                                ));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Struct(st1), SExprTE::Struct(st2)) => {
                        let t1 = TCType::Struct(st1.typ_map.clone(), st1.allow_extra_fields);
                        let t2 = TCType::Struct(st2.typ_map.clone(), st2.allow_extra_fields);
                        if unify_element_types(&t1, &t2).is_some() {
                            Ok(SExprTE::Struct(st1.typed_default(&st2)))
                        } else {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::DefaultTypeMismatch,
                                format!(
                                    "Cannot create default-expression with two different struct types: {} and {}",
                                    t1, t2
                                ),
                            ));
                            Err(())
                        }
                    }
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::DefaultTypeMismatch,
                            format!(
                                "Cannot create default-expression with two different types: {} and {}",
                                stenum1.display_with_type(),
                                stenum2.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an if expression
impl TypeCheckableHelper<SExprTE> for (&SpannedExpr, &SpannedExpr, &SpannedExpr) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (b, se1, se2) = *self;
        let b_check = b.type_check_raw(None, ctx, errs);
        let se1_check = se1.type_check_raw(expected, ctx, errs);
        let se2_check = se2.type_check_raw(expected, ctx, errs);

        match (b_check, se1_check, se2_check) {
            (Ok(SExprTE::Bool(b)), Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Float(se1), SExprTE::Float(se2)) => {
                        Ok(SExprTE::Float(SExprFloat::If(
                            Box::new(b.clone()),
                            Box::new(se1.clone()),
                            Box::new(se2.clone()),
                        )))
                    }
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(SExprBool::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(SExprUnit::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, resolved.clone());
                                let tl2 = coerce_empty_list(tl2, resolved);
                                Ok(SExprTE::List(tl1.typed_if(Box::new(b.clone()), &tl2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(
                                    TypeErrorKind::IfBranchTypeMismatch,
                                    format!(
                                        "Cannot create if-expression with two different list types: {} and {}",
                                        t1, t2
                                    ),
                                ));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Map(tm1), SExprTE::Map(tm2)) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, resolved.clone());
                                let tm2 = coerce_empty_map(tm2, resolved);
                                Ok(SExprTE::Map(tm1.typed_if(Box::new(b.clone()), &tm2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(
                                    TypeErrorKind::IfBranchTypeMismatch,
                                    format!(
                                        "Cannot create if-expression with two different map types: {} and {}",
                                        t1, t2
                                    ),
                                ));
                                Err(())
                            }
                        }
                    }
                    (SExprTE::Struct(st1), SExprTE::Struct(st2)) => {
                        let t1 = TCType::Struct(st1.typ_map.clone(), st1.allow_extra_fields);
                        let t2 = TCType::Struct(st2.typ_map.clone(), st2.allow_extra_fields);
                        if unify_element_types(&t1, &t2).is_some() {
                            Ok(SExprTE::Struct(st1.typed_if(Box::new(b.clone()), &st2)))
                        } else {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::IfBranchTypeMismatch,
                                format!(
                                    "Cannot create if-expression with two different struct types: {} and {}",
                                    t1, t2
                                ),
                            ));
                            Err(())
                        }
                    }
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::IfBranchTypeMismatch,
                            format!(
                                "Cannot create if-expression with two different types: {} and {}",
                                stenum1.display_with_type(),
                                stenum2.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            (Ok(_), Ok(_), Ok(_)) => {
                errs.push(SemanticError::type_error(
                    TypeErrorKind::ExpectedBooleanCondition,
                    "If expression condition must be a boolean".into(),
                ));
                Err(())
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an index expression
impl TypeCheckableHelper<SExprTE> for (&SpannedExpr, u64) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (inner, idx) = *self;
        let inner_check = inner.type_check_raw(expected, ctx, errs);

        match inner_check {
            Ok(ste) => match ste {
                SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::SIndex(
                    Box::new(se.clone()),
                    idx,
                ))),
                SExprTE::Str(se) => Ok(SExprTE::Str(SExprStr::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Bool(se) => {
                    Ok(SExprTE::Bool(SExprBool::SIndex(Box::new(se.clone()), idx)))
                }
                SExprTE::Unit(se) => {
                    Ok(SExprTE::Unit(SExprUnit::SIndex(Box::new(se.clone()), idx)))
                }
                SExprTE::List(tl) => Ok(SExprTE::List(tl.typed_sindex(idx))),
                SExprTE::Map(tm) => Ok(SExprTE::Map(tm.typed_sindex(idx))),
                SExprTE::Struct(se) => Ok(SExprTE::Struct(se.typed_sindex(idx))),
                SExprTE::Any(e) => Ok(SExprTE::Any(e)),
            },
            // If there's already an error just propagate it
            Err(_) => Err(()),
        }
    }
}

// Type check a variable
impl TypeCheckableHelper<SExprTE> for VarName {
    fn type_check_raw(
        &self,
        _expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let type_opt = ctx.get(self);
        match type_opt {
            Some(t) => match t {
                StreamType::Int => Ok(SExprTE::Int(SExprInt::Var(self.clone()))),
                StreamType::Float => Ok(SExprTE::Float(SExprFloat::Var(self.clone()))),
                StreamType::Str => Ok(SExprTE::Str(SExprStr::Var(self.clone()))),
                StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Var(self.clone()))),
                StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Var(self.clone()))),
                StreamType::List(inner) => {
                    let typed_list = make_list_var(self.clone(), TCType::from_stream_type(inner));
                    Ok(SExprTE::List(typed_list))
                }
                StreamType::Map(inner) => {
                    let typed_map = make_map_var(self.clone(), TCType::from_stream_type(inner));
                    Ok(SExprTE::Map(typed_map))
                }
                StreamType::Struct(inner, allow_extra) => {
                    let typed_inner = inner
                        .iter()
                        .map(|(k, v)| (k.clone(), TCType::from_stream_type(v)))
                        .collect();
                    let typed_struct = make_struct_var(self.clone(), typed_inner, *allow_extra);
                    Ok(SExprTE::Struct(typed_struct))
                }
                StreamType::Any => Ok(SExprTE::Any(SExprAny::Var(self.clone()))),
            },
            None => {
                errs.push(SemanticError::UndeclaredVariable(
                    format!("Usage of undeclared variable: {}", self),
                    None,
                ));
                Err(())
            }
        }
    }
}

fn make_list_var(var: VarName, element_type: TCType) -> TypedListExpr {
    TypedListExpr {
        element_type,
        kind: TypedListExprKind::Var(var),
    }
}

fn make_list_literal(elements: Vec<SExprTE>, element_type: TCType) -> TypedListExpr {
    TypedListExpr {
        element_type,
        kind: TypedListExprKind::Literal(elements),
    }
}

fn make_map_var(var: VarName, value_type: TCType) -> TypedMapExpr {
    TypedMapExpr {
        value_type,
        kind: TypedMapExprKind::Var(var),
    }
}

fn make_map_literal(elements: BTreeMap<EcoString, SExprTE>, value_type: TCType) -> TypedMapExpr {
    TypedMapExpr {
        value_type,
        kind: TypedMapExprKind::Literal(elements),
    }
}

fn make_struct_var(
    var: VarName,
    value_types: EcoVec<(EcoString, TCType)>,
    allow_extra_fields: bool,
) -> TypedStructExpr {
    TypedStructExpr {
        typ_map: value_types,
        allow_extra_fields,
        kind: TypedStructExprKind::Var(var),
    }
}

fn type_check_struct_literal<T>(
    entries: Vec<(EcoString, T)>,
    fields: &EcoVec<(EcoString, StreamType)>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
    allow_extra_fields: bool,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    let mut remaining: BTreeMap<EcoString, T> = entries.into_iter().collect();
    let mut typed_entries = Vec::new();
    let mut field_types = EcoVec::new();

    for (field, field_type) in fields {
        let Some(expr) = remaining.remove(field) else {
            errs.push(SemanticError::type_error(
                TypeErrorKind::StructMissingField,
                format!("Struct literal is missing required field {:?}", field),
            ));
            return Err(());
        };
        let typed_expr = expr.type_check_raw(Some(field_type), ctx, errs)?;
        let actual = extract_type(&typed_expr);
        let expected = TCType::from_stream_type(field_type);
        if actual != expected {
            errs.push(SemanticError::type_error(
                TypeErrorKind::StructFieldTypeMismatch,
                format!(
                    "Struct field {:?} has type {}, expected {}",
                    field, actual, expected
                ),
            ));
            return Err(());
        }
        typed_entries.push((field.clone(), typed_expr));
        field_types.push((field.clone(), expected));
    }

    if !remaining.is_empty() {
        if allow_extra_fields {
            for value in remaining.into_values() {
                value.type_check_raw(None, ctx, errs)?;
            }
        } else {
            errs.push(SemanticError::type_error(
                TypeErrorKind::StructUnknownField,
                format!(
                    "Struct literal contains unknown fields: {:?}",
                    remaining.keys().collect::<Vec<_>>()
                ),
            ));
            return Err(());
        }
    }

    Ok(SExprTE::Struct(TypedStructExpr {
        typ_map: field_types,
        allow_extra_fields,
        kind: TypedStructExprKind::Literal(typed_entries),
    }))
}

fn type_check_inferred_struct_literal<T>(
    entries: Vec<(EcoString, T)>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    let mut typed_entries = Vec::new();
    let mut field_types = EcoVec::new();

    for (field, expr) in entries {
        let typed_expr = expr.type_check_raw(None, ctx, errs)?;
        let field_type = extract_type(&typed_expr);
        typed_entries.push((field.clone(), typed_expr));
        field_types.push((field, field_type));
    }

    Ok(SExprTE::Struct(TypedStructExpr {
        typ_map: field_types,
        allow_extra_fields: false,
        kind: TypedStructExprKind::Literal(typed_entries),
    }))
}

fn type_check_struct_constructor<T>(
    entries: Vec<(EcoString, T)>,
    expected: Option<&StreamType>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    match expected {
        Some(StreamType::Struct(fields, allow_extra_fields)) => {
            type_check_struct_literal(entries, fields, ctx, errs, *allow_extra_fields)
        }
        Some(_) => {
            errs.push(SemanticError::type_error(
                TypeErrorKind::StructExpected,
                "Struct constructor requires an expected Struct type".into(),
            ));
            Err(())
        }
        None => type_check_inferred_struct_literal(entries, ctx, errs),
    }
}

fn type_check_object_literal<T>(
    entries: Vec<(EcoString, T)>,
    expected: Option<&StreamType>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    if let Some(StreamType::Struct(fields, allow_extra_fields)) = expected {
        type_check_struct_literal(entries, fields, ctx, errs, *allow_extra_fields)
    } else {
        type_check_map_literal(entries, expected, ctx, errs)
    }
}

fn type_check_map_literal<T>(
    entries: Vec<(EcoString, T)>,
    expected: Option<&StreamType>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()>
where
    T: TypeCheckableHelper<SExprTE>,
{
    if matches!(expected, Some(StreamType::Struct(_, _))) {
        errs.push(SemanticError::type_error(
            TypeErrorKind::StructExpected,
            "Map constructor cannot be used for Struct-typed expressions; use Struct(...)".into(),
        ));
        return Err(());
    }

    if entries.is_empty() {
        let value_type = match expected {
            Some(StreamType::Map(inner)) => TCType::from_stream_type(inner.as_ref()),
            _ => TCType::EmptyMap,
        };
        return Ok(SExprTE::Map(make_map_literal(BTreeMap::new(), value_type)));
    }

    let value_expected = match expected {
        Some(StreamType::Map(inner)) => Some(inner.as_ref()),
        _ => None,
    };

    let mut iter = entries.into_iter();
    let (first_key, first_value) = iter.next().expect("non-empty map checked above");
    let first_te = first_value.type_check_raw(value_expected, ctx, errs)?;
    let first_type = extract_type(&first_te);
    let mut typed_entries = BTreeMap::from([(first_key, first_te)]);

    for (key, value) in iter {
        let value_te = value.type_check_raw(value_expected, ctx, errs)?;
        let value_type = extract_type(&value_te);
        if value_type != first_type {
            errs.push(SemanticError::type_error(
                TypeErrorKind::MapValueTypeMismatch,
                format!(
                    "Map value type mismatch: expected {} (from first value), got {}",
                    first_type, value_type
                ),
            ));
            return Err(());
        }
        typed_entries.insert(key, value_te);
    }

    Ok(SExprTE::Map(make_map_literal(typed_entries, first_type)))
}

/// Attempt to unify two [`TCType`]s, treating empty-container placeholders as
/// compatible with concrete types.  Returns the resolved (concrete) type on success, or `None`
/// when the two types are incompatible.
fn unify_element_types(t1: &TCType, t2: &TCType) -> Option<TCType> {
    match (t1, t2) {
        _ if t1 == t2 => Some(t1.clone()),
        (TCType::EmptyList, other) | (other, TCType::EmptyList) => Some(other.clone()),
        (TCType::EmptyMap, other) | (other, TCType::EmptyMap) => Some(other.clone()),
        (TCType::Struct(fields1, allow_extra1), TCType::Struct(fields2, allow_extra2))
            if allow_extra1 == allow_extra2
                && fields1.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()
                    == fields2.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>() =>
        {
            let args = fields1
                .iter()
                .zip(fields2.iter())
                .filter_map(|((f, a), (_, b))| match unify_element_types(a, b) {
                    Some(u) => Some((f.clone(), u)),
                    None => None,
                })
                .collect::<EcoVec<_>>();
            if args.len() != fields1.len() {
                return None;
            }
            Some(TCType::Struct(args, *allow_extra1))
        }
        (TCType::List(t1), TCType::List(t2)) => {
            Some(TCType::List(Box::new(unify_element_types(t1, t2)?)))
        }
        (TCType::Map(t1), TCType::Map(t2)) => {
            Some(TCType::Map(Box::new(unify_element_types(t1, t2)?)))
        }
        _ => None,
    }
}

/// Cast `expr` to the given target type, inserting a runtime `Cast` node when
/// the expression is not already of that type (e.g. for `Any` operands).
pub(super) fn cast_to_type(expr: SExprTE, target: &StreamType) -> SExprTE {
    match target {
        StreamType::Int => match expr {
            SExprTE::Int(e) => SExprTE::Int(e),
            other => SExprTE::Int(SExprInt::Cast(Box::new(other))),
        },
        StreamType::Float => match expr {
            SExprTE::Float(e) => SExprTE::Float(e),
            other => SExprTE::Float(SExprFloat::Cast(Box::new(other))),
        },
        StreamType::Str => match expr {
            SExprTE::Str(e) => SExprTE::Str(e),
            other => SExprTE::Str(SExprStr::Cast(Box::new(other))),
        },
        StreamType::Bool => match expr {
            SExprTE::Bool(e) => SExprTE::Bool(e),
            other => SExprTE::Bool(SExprBool::Cast(Box::new(other))),
        },
        StreamType::Unit => match expr {
            SExprTE::Unit(e) => SExprTE::Unit(e),
            other => SExprTE::Unit(SExprUnit::Cast(Box::new(other))),
        },
        StreamType::Any => expr,
        StreamType::List(_) | StreamType::Map(_) | StreamType::Struct(_, _) => expr,
    }
}

/// If `list` has an `EmptyList` element type, reconstruct it as a concrete empty
/// list literal of the given `target_element_type`.  Non-`EmptyList` lists are
/// returned unchanged.
pub(super) fn coerce_empty_list(list: TypedListExpr, target_element_type: TCType) -> TypedListExpr {
    if list.element_type() == &TCType::EmptyList {
        make_list_literal(vec![], target_element_type)
    } else {
        list
    }
}

pub(super) fn coerce_empty_map(map: TypedMapExpr, target_value_type: TCType) -> TypedMapExpr {
    if map.value_type() == &TCType::EmptyMap {
        make_map_literal(BTreeMap::new(), target_value_type)
    } else {
        map
    }
}

fn typed_struct_get(
    typed_struct: TypedStructExpr,
    key: EcoString,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()> {
    let Some((_, field_type)) = typed_struct.typ_map.iter().find(|(field, _)| *field == key) else {
        errs.push(SemanticError::type_error(
            TypeErrorKind::StructFieldAccess,
            format!("Struct has no field {:?}", key),
        ));
        return Err(());
    };

    match field_type.clone() {
        TCType::Int => Ok(SExprTE::Int(SExprInt::SGetStruct(typed_struct, key))),
        TCType::Float => Ok(SExprTE::Float(SExprFloat::SGetStruct(typed_struct, key))),
        TCType::Bool => Ok(SExprTE::Bool(SExprBool::SGetStruct(typed_struct, key))),
        TCType::Str => Ok(SExprTE::Str(SExprStr::SGetStruct(typed_struct, key))),
        TCType::Unit => Ok(SExprTE::Unit(SExprUnit::SGetStruct(typed_struct, key))),
        TCType::List(inner) => Ok(SExprTE::List(TypedListExpr {
            element_type: *inner,
            kind: TypedListExprKind::SGetStruct(Box::new(typed_struct), key),
        })),
        TCType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
            value_type: *inner,
            kind: TypedMapExprKind::SGetStruct(Box::new(typed_struct), key),
        })),
        TCType::Struct(fields, allow_extra_fields) => Ok(SExprTE::Struct(TypedStructExpr {
            typ_map: fields,
            allow_extra_fields,
            kind: TypedStructExprKind::SGet(Box::new(typed_struct), key),
        })),
        TCType::Any => Ok(SExprTE::Any(SExprAny::Expr(SExpr::SGet(
            Box::new(SExpr::Val(Value::Unit).into()),
            key,
        )))),
        TCType::EmptyList | TCType::EmptyMap | TCType::Unknown => {
            errs.push(SemanticError::type_error(
                TypeErrorKind::StructUnresolvedFieldType,
                format!(
                    "Struct field {:?} has unresolved field type {}",
                    key, field_type
                ),
            ));
            Err(())
        }
    }
}

fn typed_map_get(
    typed_map: TypedMapExpr,
    key: EcoString,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()> {
    match typed_map.value_type().clone() {
        TCType::Int => Ok(SExprTE::Int(SExprInt::MGetMap(typed_map, key))),
        TCType::Float => Ok(SExprTE::Float(SExprFloat::MGetMap(typed_map, key))),
        TCType::Bool => Ok(SExprTE::Bool(SExprBool::MGetMap(typed_map, key))),
        TCType::Str => Ok(SExprTE::Str(SExprStr::MGetMap(typed_map, key))),
        TCType::Unit => Ok(SExprTE::Unit(SExprUnit::MGetMap(typed_map, key))),
        TCType::EmptyMap => {
            errs.push(SemanticError::unresolved_type(
                UnresolvedTypeKind::EmptyMapValueType,
                "Cannot determine value type for get from an empty map".into(),
            ));
            Err(())
        }
        TCType::EmptyList | TCType::Unknown => {
            errs.push(SemanticError::unresolved_type(
                UnresolvedTypeKind::MapGetValueType,
                "Cannot determine concrete value type for map get".into(),
            ));
            Err(())
        }
        TCType::Any => Ok(SExprTE::Any(SExprAny::Expr(SExpr::MGet(
            Box::new(SExpr::Val(Value::Unit).into()),
            key,
        )))),
        TCType::List(t) => Ok(SExprTE::List(TypedListExpr {
            element_type: *t,
            kind: TypedListExprKind::MGetMap(Box::new(typed_map), key),
        })),
        TCType::Map(t) => Ok(SExprTE::Map(TypedMapExpr {
            value_type: *t,
            kind: TypedMapExprKind::MGetMap(Box::new(typed_map), key),
        })),
        TCType::Struct(fields, allow_extra_fields) => Ok(SExprTE::Struct(TypedStructExpr {
            typ_map: fields,
            allow_extra_fields,
            kind: TypedStructExprKind::MGetMap(Box::new(typed_map), key),
        })),
    }
}

#[derive(Clone, Copy)]
enum StreamBinaryOpKind {
    Update,
    Latch,
}

fn type_check_same_typed_stream_op(
    op: StreamBinaryOpKind,
    lhs: &SExpr,
    rhs: &SExpr,
    expected: Option<&StreamType>,
    ctx: &mut TypeInfo,
    errs: &mut SemanticErrors,
) -> Result<SExprTE, ()> {
    let lhs_te = lhs.type_check_raw(expected, ctx, errs)?;
    let rhs_te = rhs.type_check_raw(expected, ctx, errs)?;

    match (lhs_te, rhs_te) {
        (SExprTE::Int(a), SExprTE::Int(b)) => Ok(SExprTE::Int(match op {
            StreamBinaryOpKind::Update => SExprInt::Update(Box::new(a), Box::new(b)),
            StreamBinaryOpKind::Latch => SExprInt::Latch(Box::new(a), Box::new(b)),
        })),
        (SExprTE::Float(a), SExprTE::Float(b)) => Ok(SExprTE::Float(match op {
            StreamBinaryOpKind::Update => SExprFloat::Update(Box::new(a), Box::new(b)),
            StreamBinaryOpKind::Latch => SExprFloat::Latch(Box::new(a), Box::new(b)),
        })),
        (SExprTE::Str(a), SExprTE::Str(b)) => Ok(SExprTE::Str(match op {
            StreamBinaryOpKind::Update => SExprStr::Update(Box::new(a), Box::new(b)),
            StreamBinaryOpKind::Latch => SExprStr::Latch(Box::new(a), Box::new(b)),
        })),
        (SExprTE::Bool(a), SExprTE::Bool(b)) => Ok(SExprTE::Bool(match op {
            StreamBinaryOpKind::Update => SExprBool::Update(Box::new(a), Box::new(b)),
            StreamBinaryOpKind::Latch => SExprBool::Latch(Box::new(a), Box::new(b)),
        })),
        (SExprTE::Unit(a), SExprTE::Unit(b)) => Ok(SExprTE::Unit(match op {
            StreamBinaryOpKind::Update => SExprUnit::Update(Box::new(a), Box::new(b)),
            StreamBinaryOpKind::Latch => SExprUnit::Latch(Box::new(a), Box::new(b)),
        })),
        (SExprTE::List(a), SExprTE::List(b)) => {
            let t1 = a.element_type().clone();
            let t2 = b.element_type().clone();
            match unify_element_types(&t1, &t2) {
                Some(resolved) => {
                    let a = coerce_empty_list(a, resolved.clone());
                    let b = coerce_empty_list(b, resolved);
                    Ok(SExprTE::List(match op {
                        StreamBinaryOpKind::Update => a.typed_update_stream(&b),
                        StreamBinaryOpKind::Latch => a.typed_latch(&b),
                    }))
                }
                None => {
                    errs.push(SemanticError::type_error(
                        TypeErrorKind::ListElementTypeMismatch,
                        format!(
                            "Stream operation requires matching list types, got {} and {}",
                            t1, t2
                        ),
                    ));
                    Err(())
                }
            }
        }
        (SExprTE::Map(a), SExprTE::Map(b)) => {
            let t1 = a.value_type().clone();
            let t2 = b.value_type().clone();
            match unify_element_types(&t1, &t2) {
                Some(resolved) => {
                    let a = coerce_empty_map(a, resolved.clone());
                    let b = coerce_empty_map(b, resolved);
                    Ok(SExprTE::Map(match op {
                        StreamBinaryOpKind::Update => a.typed_update_stream(&b),
                        StreamBinaryOpKind::Latch => a.typed_latch(&b),
                    }))
                }
                None => {
                    errs.push(SemanticError::type_error(
                        TypeErrorKind::MapValueTypeMismatch,
                        format!(
                            "Stream operation requires matching map types, got {} and {}",
                            t1, t2
                        ),
                    ));
                    Err(())
                }
            }
        }
        (SExprTE::Struct(a), SExprTE::Struct(b)) => {
            let t1 = TCType::Struct(a.typ_map.clone(), a.allow_extra_fields);
            let t2 = TCType::Struct(b.typ_map.clone(), b.allow_extra_fields);
            if unify_element_types(&t1, &t2).is_some() {
                Ok(SExprTE::Struct(match op {
                    StreamBinaryOpKind::Update => a.typed_update_stream(&b),
                    StreamBinaryOpKind::Latch => a.typed_latch(&b),
                }))
            } else {
                errs.push(SemanticError::type_error(
                    TypeErrorKind::StructOperationTypeMismatch,
                    format!(
                        "Stream operation requires matching struct types, got {} and {}",
                        t1, t2
                    ),
                ));
                Err(())
            }
        }
        (a, b) => {
            errs.push(SemanticError::type_error(
                TypeErrorKind::OperatorTypeMismatch,
                format!(
                    "Stream operation requires both arguments to have the same type, got {} and {}",
                    a.display_with_type(),
                    b.display_with_type()
                ),
            ));
            Err(())
        }
    }
}

impl TypeCheckableHelper<SExprTE> for (SpannedExpr, StreamTypeAscription) {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (expr, ascription) = self;
        // If an explicit ascription is present, prefer it as the expected type for
        // the inner expression; otherwise fall back to the outer expected type.
        let effective_expected = match ascription {
            StreamTypeAscription::Ascribed(ty) => Some(ty),
            StreamTypeAscription::Unascribed => expected,
        };
        let expr_te = expr.type_check_raw(effective_expected, ctx, errs)?;

        match ascription {
            StreamTypeAscription::Unascribed => Ok(expr_te),
            StreamTypeAscription::Ascribed(expected_ty) => {
                let actual_ty = extract_type(&expr_te);
                if actual_ty == TCType::from_stream_type(expected_ty) {
                    Ok(expr_te)
                } else {
                    errs.push(SemanticError::type_error(
                        TypeErrorKind::AnnotationTypeMismatch,
                        format!("Type mismatch: expected {}, got {}", expected_ty, actual_ty),
                    ));
                    Err(())
                }
            }
        }
    }
}

// Type check an expression
impl TypeCheckableHelper<SExprTE> for SExpr {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            SExpr::Val(sdata) => sdata.type_check_raw(expected, ctx, errs),
            SExpr::BinOp(se1, se2, op) => {
                (op.clone(), se1.as_ref(), se2.as_ref()).type_check_raw(expected, ctx, errs)
            }
            SExpr::If(b, se1, se2) => {
                (b.as_ref(), se1.as_ref(), se2.as_ref()).type_check_raw(expected, ctx, errs)
            }
            SExpr::SIndex(inner, idx) => (inner.as_ref(), *idx).type_check_raw(expected, ctx, errs),
            SExpr::Var(id) => id.type_check_raw(expected, ctx, errs),
            SExpr::Dynamic(e, type_ascription) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Ascriptions are required for defers in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::MissingTypeAscription(
                            "Type ascription required for defer".into(),
                            None,
                        ));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ExpectedDynamicString,
                            format!(
                                "Expected Dynamic to be applied to a Str, got {}",
                                ty.display_with_type()
                            ),
                        ));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        element_type: TCType::from_stream_type(inner),
                        kind: TypedListExprKind::Dynamic(Box::new(e_str), ctx.clone()),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        value_type: TCType::from_stream_type(inner),
                        kind: TypedMapExprKind::Dynamic(Box::new(e_str), ctx.clone()),
                    })),
                    StreamType::Struct(fields, allow_extra_fields) => {
                        Ok(SExprTE::Struct(TypedStructExpr {
                            typ_map: fields
                                .iter()
                                .map(|(n, t)| (n.clone(), TCType::from_stream_type(t)))
                                .collect(),
                            allow_extra_fields: *allow_extra_fields,
                            kind: TypedStructExprKind::Dynamic(Box::new(e_str), ctx.clone()),
                        }))
                    }
                    StreamType::Any => Ok(SExprTE::Any(SExprAny::Expr(SExpr::Val(Value::Unit)))),
                }
            }
            SExpr::RestrictedDynamic(e, type_ascription, vs) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ExpectedDynamicString,
                            format!(
                                "Expected RestrictedDynamic to be applied to a Str, got {}",
                                ty.display_with_type()
                            ),
                        ));
                        return Err(());
                    }
                };

                // Verify type ascription if provided - RestrictedDynamic only supports Str output
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::MissingTypeAscription(
                            "Type ascription required for dynamic".into(),
                            None,
                        ));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        element_type: TCType::from_stream_type(inner),
                        kind: TypedListExprKind::RestrictedDynamic(
                            Box::new(e_str),
                            vs.clone(),
                            ctx.clone(),
                        ),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        value_type: TCType::from_stream_type(inner),
                        kind: TypedMapExprKind::RestrictedDynamic(
                            Box::new(e_str),
                            vs.clone(),
                            ctx.clone(),
                        ),
                    })),
                    StreamType::Struct(fields, allow_extra_fields) => {
                        Ok(SExprTE::Struct(TypedStructExpr {
                            typ_map: fields
                                .iter()
                                .map(|(n, t)| (n.clone(), TCType::from_stream_type(t)))
                                .collect(),
                            allow_extra_fields: *allow_extra_fields,
                            kind: TypedStructExprKind::RestrictedDynamic(
                                Box::new(e_str),
                                vs.clone(),
                                ctx.clone(),
                            ),
                        }))
                    }
                    StreamType::Any => Ok(SExprTE::Any(SExprAny::Expr(SExpr::Val(Value::Unit)))),
                }
            }
            SExpr::Defer(e, type_ascription, vs) => {
                let e_check = e.type_check_raw(None, ctx, errs)?;

                // Ascriptions are required for defer in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::MissingTypeAscription(
                            "Type ascription required for defer".into(),
                            None,
                        ));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ExpectedDynamicString,
                            format!(
                                "Expected Defer to be applied to a Str, got {}",
                                ty.display_with_type()
                            ),
                        ));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                        vs.clone(),
                    ))),
                    StreamType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                        element_type: TCType::from_stream_type(inner),
                        kind: TypedListExprKind::Defer(Box::new(e_str), ctx.clone(), vs.clone()),
                    })),
                    StreamType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                        value_type: TCType::from_stream_type(inner),
                        kind: TypedMapExprKind::Defer(Box::new(e_str), ctx.clone(), vs.clone()),
                    })),
                    StreamType::Struct(fields, allow_extra_fields) => {
                        Ok(SExprTE::Struct(TypedStructExpr {
                            typ_map: fields
                                .iter()
                                .map(|(n, t)| (n.clone(), TCType::from_stream_type(t)))
                                .collect(),
                            allow_extra_fields: *allow_extra_fields,
                            kind: TypedStructExprKind::Defer(
                                Box::new(e_str),
                                ctx.clone(),
                                vs.clone(),
                            ),
                        }))
                    }
                    StreamType::Any => Ok(SExprTE::Any(SExprAny::Expr(SExpr::Val(Value::Unit)))),
                }
            }
            SExpr::Update(lhs, rhs) => type_check_same_typed_stream_op(
                StreamBinaryOpKind::Update,
                lhs,
                rhs,
                expected,
                ctx,
                errs,
            ),
            SExpr::Default(se, d) => (se.as_ref(), d.as_ref()).type_check_raw(expected, ctx, errs),
            SExpr::Not(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Bool), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Bool(se) => Ok(SExprTE::Bool(SExprBool::Not(Box::new(se)))),
                    _ => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ExpectedBooleanCondition,
                            "Not can only be applied to boolean expressions".into(),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::List(exprs) => {
                if exprs.is_empty() {
                    // Use expected type to resolve the element type of an empty list.
                    // Prefer the expected type when available; otherwise fall
                    // back to Poly so that type-erasing operations like
                    // is_defined and len can still accept the empty list.
                    let inner = match expected {
                        Some(StreamType::List(inner)) => TCType::from_stream_type(inner.as_ref()),
                        _ => TCType::EmptyList,
                    };
                    Ok(SExprTE::List(make_list_literal(vec![], inner)))
                } else {
                    // Derive inner expected type from expected (if it is a list type)
                    let inner_expected = match expected {
                        Some(StreamType::List(inner)) => Some(inner.as_ref()),
                        _ => None,
                    };

                    // Type-check first element to determine element type
                    let first_te = exprs[0].type_check_raw(inner_expected, ctx, errs)?;
                    let first_type = extract_type(&first_te);

                    // Type-check remaining elements and check consistency with first element's type
                    let mut typed_exprs = vec![first_te];
                    for expr in exprs.iter().skip(1) {
                        let elem_te = expr.type_check_raw(inner_expected, ctx, errs)?;
                        let elem_type = extract_type(&elem_te);
                        if elem_type != first_type {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::ListElementTypeMismatch,
                                format!(
                                    "List element type mismatch: expected {} (from first element), got {}",
                                    first_type, elem_type
                                ),
                            ));
                            return Err(());
                        }
                        typed_exprs.push(elem_te);
                    }

                    let typed_list = make_list_literal(typed_exprs, first_type);
                    Ok(SExprTE::List(typed_list))
                }
            }
            SExpr::LIndex(list_expr, idx_expr) => {
                // If we expect element type T, then the list should be List<T>
                let list_expected = expected.map(|t| StreamType::List(Box::new(t.clone())));
                let list_te = list_expr.type_check_raw(list_expected.as_ref(), ctx, errs)?;
                let idx_te = idx_expr.type_check_raw(Some(&StreamType::Int), ctx, errs)?;

                let idx_int = match idx_te {
                    SExprTE::Int(e) => e,
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListIndexTypeMismatch,
                            format!(
                                "LIndex index must be Int, got {}",
                                other.display_with_type()
                            ),
                        ));
                        return Err(());
                    }
                };

                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        match elem_type {
                            TCType::Int => Ok(SExprTE::Int(SExprInt::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Float => Ok(SExprTE::Float(SExprFloat::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Bool => Ok(SExprTE::Bool(SExprBool::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Str => Ok(SExprTE::Str(SExprStr::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Unit => Ok(SExprTE::Unit(SExprUnit::LIndexList(
                                typed_list,
                                Box::new(idx_int),
                            ))),
                            TCType::Any => {
                                Ok(SExprTE::Any(SExprAny::Expr(SExpr::Val(Value::Unit))))
                            }
                            TCType::EmptyList => {
                                errs.push(SemanticError::unresolved_type(
                                    UnresolvedTypeKind::EmptyListIndexElementType,
                                    "Cannot determine element type for index into an empty list"
                                        .into(),
                                ));
                                Err(())
                            }
                            TCType::EmptyMap | TCType::Unknown => {
                                errs.push(SemanticError::unresolved_type(
                                    UnresolvedTypeKind::ListIndexElementType,
                                    format!(
                                        "Cannot index list with unresolved element type {}",
                                        elem_type
                                    ),
                                ));
                                Err(())
                            }
                            TCType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                                element_type: *inner,
                                kind: TypedListExprKind::LIndexList(
                                    Box::new(typed_list),
                                    Box::new(idx_int),
                                ),
                            })),
                            TCType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                                value_type: *inner,
                                kind: TypedMapExprKind::LIndexList(
                                    Box::new(typed_list),
                                    Box::new(idx_int),
                                ),
                            })),
                            TCType::Struct(fields, allow_extra_fields) => {
                                Ok(SExprTE::Struct(TypedStructExpr {
                                    typ_map: fields,
                                    allow_extra_fields,
                                    kind: TypedStructExprKind::LIndexList(
                                        Box::new(typed_list),
                                        Box::new(idx_int),
                                    ),
                                }))
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!("LIndex requires a List, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::LAppend(list_expr, elem_expr) => {
                let list_te = list_expr.type_check_raw(expected, ctx, errs)?;

                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        // When the list is EmptyList (empty), we cannot constrain
                        // the element; let it infer its own type instead.
                        let elem_expected = elem_type.to_stream_type();
                        let elem_te =
                            elem_expr.type_check_raw(elem_expected.as_ref(), ctx, errs)?;
                        let actual_elem_type = extract_type(&elem_te);
                        match unify_element_types(&elem_type, &actual_elem_type) {
                            Some(resolved) => {
                                let typed_list = coerce_empty_list(typed_list, resolved);
                                Ok(SExprTE::List(typed_list.typed_append(Box::new(elem_te))))
                            }
                            None => {
                                errs.push(SemanticError::type_error(TypeErrorKind::ListElementTypeMismatch, format!(
                                    "LAppend element type mismatch: list has element type {}, but got {}",
                                    elem_type, actual_elem_type
                                )));
                                Err(())
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!(
                                "LAppend requires a List as first argument, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::LConcat(list1_expr, list2_expr) => {
                let list1_te = list1_expr.type_check_raw(expected, ctx, errs)?;
                let list2_te = list2_expr.type_check_raw(expected, ctx, errs)?;

                match (list1_te, list2_te) {
                    (SExprTE::List(tl1), SExprTE::List(tl2)) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, resolved.clone());
                                let tl2 = coerce_empty_list(tl2, resolved);
                                Ok(SExprTE::List(tl1.typed_concat(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(
                                    TypeErrorKind::ListElementTypeMismatch,
                                    format!(
                                        "LConcat requires lists of the same type, got {} and {}",
                                        t1, t2
                                    ),
                                ));
                                Err(())
                            }
                        }
                    }
                    (other1, other2) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!(
                                "LConcat requires two Lists, got {} and {}",
                                other1.display_with_type(),
                                other2.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::LHead(list_expr) => {
                // If we expect type T, then the list should be List<T>
                let list_expected = expected.map(|t| StreamType::List(Box::new(t.clone())));
                let list_te = list_expr.type_check_raw(list_expected.as_ref(), ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => {
                        let elem_type = typed_list.element_type().clone();
                        match elem_type {
                            TCType::Int => Ok(SExprTE::Int(SExprInt::LHeadList(typed_list))),
                            TCType::Float => Ok(SExprTE::Float(SExprFloat::LHeadList(typed_list))),
                            TCType::Bool => Ok(SExprTE::Bool(SExprBool::LHeadList(typed_list))),
                            TCType::Str => Ok(SExprTE::Str(SExprStr::LHeadList(typed_list))),
                            TCType::Unit => Ok(SExprTE::Unit(SExprUnit::LHeadList(typed_list))),
                            TCType::Any => {
                                Ok(SExprTE::Any(SExprAny::Expr(SExpr::Val(Value::Unit))))
                            }
                            TCType::EmptyList => {
                                errs.push(SemanticError::unresolved_type(
                                    UnresolvedTypeKind::EmptyListHeadElementType,
                                    "Cannot determine element type for head of an empty list"
                                        .into(),
                                ));
                                Err(())
                            }
                            TCType::EmptyMap | TCType::Unknown => {
                                errs.push(SemanticError::unresolved_type(
                                    UnresolvedTypeKind::ListHeadElementType,
                                    format!(
                                        "Cannot take head of list with unresolved element type {}",
                                        elem_type
                                    ),
                                ));
                                Err(())
                            }
                            TCType::List(inner) => Ok(SExprTE::List(TypedListExpr {
                                element_type: *inner,
                                kind: TypedListExprKind::LHeadList(Box::new(typed_list)),
                            })),
                            TCType::Map(inner) => Ok(SExprTE::Map(TypedMapExpr {
                                value_type: *inner,
                                kind: TypedMapExprKind::LHeadList(Box::new(typed_list)),
                            })),
                            TCType::Struct(fields, allow_extra_fields) => {
                                Ok(SExprTE::Struct(TypedStructExpr {
                                    typ_map: fields,
                                    allow_extra_fields,
                                    kind: TypedStructExprKind::LHeadList(Box::new(typed_list)),
                                }))
                            }
                        }
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!("LHead requires a List, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::LTail(list_expr) => {
                // LTail preserves the list type, so propagate expected directly
                let list_te = list_expr.type_check_raw(expected, ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => Ok(SExprTE::List(typed_list.typed_tail())),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!("LTail requires a List, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::LLen(list_expr) => {
                // LLen produces Int; the inner list can be any type
                let list_te = list_expr.type_check_raw(None, ctx, errs)?;
                match list_te {
                    SExprTE::List(typed_list) => Ok(SExprTE::Int(SExprInt::LLen(typed_list))),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::ListOperationTypeMismatch,
                            format!("LLen requires a List, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::IsDefined(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(None, ctx, errs)?;
                Ok(SExprTE::Bool(SExprBool::IsDefined(Box::new(sexpr_check))))
            }
            SExpr::When(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(None, ctx, errs)?;
                Ok(SExprTE::Bool(SExprBool::When(Box::new(sexpr_check))))
            }
            SExpr::Latch(lhs, rhs) => type_check_same_typed_stream_op(
                StreamBinaryOpKind::Latch,
                lhs,
                rhs,
                expected,
                ctx,
                errs,
            ),
            SExpr::Init(se1, se2) => {
                let se1_check = se1.type_check_raw(expected, ctx, errs);
                let se2_check = se2.type_check_raw(expected, ctx, errs);
                match (se1_check, se2_check) {
                    (Ok(SExprTE::Int(e1)), Ok(SExprTE::Int(e2))) => {
                        Ok(SExprTE::Int(SExprInt::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Float(e1)), Ok(SExprTE::Float(e2))) => {
                        Ok(SExprTE::Float(SExprFloat::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Str(e1)), Ok(SExprTE::Str(e2))) => {
                        Ok(SExprTE::Str(SExprStr::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Bool(e1)), Ok(SExprTE::Bool(e2))) => {
                        Ok(SExprTE::Bool(SExprBool::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Unit(e1)), Ok(SExprTE::Unit(e2))) => {
                        Ok(SExprTE::Unit(SExprUnit::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::List(tl1)), Ok(SExprTE::List(tl2))) => {
                        let t1 = tl1.element_type().clone();
                        let t2 = tl2.element_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tl1 = coerce_empty_list(tl1, resolved.clone());
                                let tl2 = coerce_empty_list(tl2, resolved);
                                Ok(SExprTE::List(tl1.typed_init(&tl2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(TypeErrorKind::ListElementTypeMismatch, format!(
                                    "Init requires both arguments to have the same type, got List<{}> and List<{}>",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (Ok(SExprTE::Map(tm1)), Ok(SExprTE::Map(tm2))) => {
                        let t1 = tm1.value_type().clone();
                        let t2 = tm2.value_type().clone();
                        match unify_element_types(&t1, &t2) {
                            Some(resolved) => {
                                let tm1 = coerce_empty_map(tm1, resolved.clone());
                                let tm2 = coerce_empty_map(tm2, resolved);
                                Ok(SExprTE::Map(tm1.typed_init(&tm2)))
                            }
                            None => {
                                errs.push(SemanticError::type_error(TypeErrorKind::MapValueTypeMismatch, format!(
                                    "Init requires both arguments to have the same type, got Map<{}> and Map<{}>",
                                    t1, t2
                                )));
                                Err(())
                            }
                        }
                    }
                    (Ok(SExprTE::Struct(st1)), Ok(SExprTE::Struct(st2))) => {
                        let t1 = TCType::Struct(st1.typ_map.clone(), st1.allow_extra_fields);
                        let t2 = TCType::Struct(st2.typ_map.clone(), st2.allow_extra_fields);
                        if unify_element_types(&t1, &t2).is_some() {
                            Ok(SExprTE::Struct(st1.typed_init(&st2)))
                        } else {
                            errs.push(SemanticError::type_error(TypeErrorKind::StructOperationTypeMismatch, format!(
                                "Init requires both arguments to have the same type, got {} and {}",
                                t1, t2
                            )));
                            Err(())
                        }
                    }
                    (Ok(ste1), Ok(ste2)) => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::OperatorTypeMismatch,
                            format!(
                                "Init requires both arguments to have the same type, got {} and {}",
                                ste1.display_with_type(),
                                ste2.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                    _ => Err(()),
                }
            }
            SExpr::Sin(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Sin(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::NumericArgumentTypeMismatch,
                            format!(
                                "Sin can only be applied to float expressions, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::Cos(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Cos(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::NumericArgumentTypeMismatch,
                            format!(
                                "Cos can only be applied to float expressions, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::Tan(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(Some(&StreamType::Float), ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Tan(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::NumericArgumentTypeMismatch,
                            format!(
                                "Tan can only be applied to float expressions, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::Abs(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(expected, ctx, errs)?;
                match sexpr_check {
                    SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::Abs(Box::new(se)))),
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Abs(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::NumericArgumentTypeMismatch,
                            format!(
                                "Abs can only be applied to numeric expressions, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::MonitoredAt(_, _) => {
                errs.push(SemanticError::UnsupportedExpression(
                    "monitored_at is only supported in distributed untyped semantics".into(),
                    None,
                ));
                Err(())
            }
            SExpr::Dist(_, _) => {
                errs.push(SemanticError::UnsupportedExpression(
                    "dist is only supported in distributed untyped semantics".into(),
                    None,
                ));
                Err(())
            }
            SExpr::Map(entries) => type_check_map_literal(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            SExpr::Struct(entries) => type_check_struct_constructor(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            SExpr::ObjectLiteral(entries) => type_check_object_literal(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                expected,
                ctx,
                errs,
            ),
            SExpr::MGet(map_expr, key) => {
                let map_expected = expected.map(|t| StreamType::Map(Box::new(t.clone())));
                let map_te = map_expr.type_check_raw(map_expected.as_ref(), ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => typed_map_get(typed_map, key.clone(), errs),
                    SExprTE::Struct(typed_struct) => {
                        typed_struct_get(typed_struct, key.clone(), errs)
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::MapOperationTypeMismatch,
                            format!(
                                "MGet requires a Map or Struct, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::SGet(struct_expr, key) => {
                let struct_te = struct_expr.type_check_raw(None, ctx, errs)?;
                match struct_te {
                    SExprTE::Struct(typed_struct) => {
                        typed_struct_get(typed_struct, key.clone(), errs)
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::StructExpected,
                            format!(
                                "Dot field access requires a Struct in typed semantics, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::MInsert(map_expr, key, value_expr) => {
                let map_te = map_expr.type_check_raw(expected, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        let value_type = typed_map.value_type().clone();
                        let value_expected = value_type.to_stream_type();
                        let value_te =
                            value_expr.type_check_raw(value_expected.as_ref(), ctx, errs)?;
                        let actual_value_type = extract_type(&value_te);
                        match unify_element_types(&value_type, &actual_value_type) {
                            Some(resolved) => {
                                let typed_map = coerce_empty_map(typed_map, resolved);
                                Ok(SExprTE::Map(
                                    typed_map.typed_insert(key.clone(), Box::new(value_te)),
                                ))
                            }
                            None => {
                                errs.push(SemanticError::type_error(TypeErrorKind::MapValueTypeMismatch, format!(
                                    "MInsert value type mismatch: map has value type {}, but got {}",
                                    value_type, actual_value_type
                                )));
                                Err(())
                            }
                        }
                    }
                    SExprTE::Struct(typed_struct) => {
                        let Some((_, field_type)) = typed_struct
                            .typ_map
                            .iter()
                            .find(|(field, _)| *field == *key)
                        else {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::StructFieldAccess,
                                format!("Struct has no field {:?}", key),
                            ));
                            return Err(());
                        };
                        let value_expected = field_type.to_stream_type();
                        let value_te =
                            value_expr.type_check_raw(value_expected.as_ref(), ctx, errs)?;
                        let actual_value_type = extract_type(&value_te);
                        if actual_value_type != *field_type {
                            errs.push(SemanticError::type_error(
                                TypeErrorKind::StructFieldTypeMismatch,
                                format!(
                                    "Struct field {:?} has type {}, but got {}",
                                    key, field_type, actual_value_type
                                ),
                            ));
                            return Err(());
                        }
                        Ok(SExprTE::Struct(
                            typed_struct.typed_update(key.clone(), Box::new(value_te)),
                        ))
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::MapOperationTypeMismatch,
                            format!(
                                "MInsert requires a Map as first argument, got {}",
                                other.display_with_type()
                            ),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::MRemove(map_expr, key) => {
                let map_te = map_expr.type_check_raw(expected, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        Ok(SExprTE::Map(typed_map.typed_remove(key.clone())))
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::MapOperationTypeMismatch,
                            format!("MRemove requires a Map, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::MHasKey(map_expr, key) => {
                let map_te = map_expr.type_check_raw(None, ctx, errs)?;
                match map_te {
                    SExprTE::Map(typed_map) => {
                        Ok(SExprTE::Bool(SExprBool::MHasKeyMap(typed_map, key.clone())))
                    }
                    SExprTE::Struct(typed_struct) => {
                        Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(
                            typed_struct.typ_map.iter().any(|(field, _)| field == key),
                        ))))
                    }
                    other => {
                        errs.push(SemanticError::type_error(
                            TypeErrorKind::MapOperationTypeMismatch,
                            format!("MHasKey requires a Map, got {}", other.display_with_type()),
                        ));
                        Err(())
                    }
                }
            }
        }
    }
}

impl TypeCheckableHelper<SExprTE> for SpannedExpr {
    fn type_check_raw(
        &self,
        expected: Option<&StreamType>,
        ctx: &mut TypeInfo,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let first_new_error = errs.len();
        let result = self.node.type_check_raw(expected, ctx, errs);
        for err in &mut errs[first_new_error..] {
            err.set_span_if_absent(self.span);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use crate::lang::dsrv::ast::{BoolBinOp, IntBinOp, NumericalBinOp, StrBinOp};

    use super::*;
    use ecow::eco_vec;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};
    use test_log::test;

    type SExpr = SpannedExpr;
    type SExprV = SpannedExpr;
    type SemantResultStr = SemanticResult<SExprTE>;

    trait BinOpExpr<Expr> {
        fn binop_expr(lhs: Expr, rhs: Expr, op: SBinOp) -> Self;
    }

    trait IfExpr<Expr, BoolExpr> {
        fn if_expr(b: BoolExpr, t: Expr, f: Expr) -> Self;
    }

    impl BinOpExpr<Box<SExpr>> for SExpr {
        fn binop_expr(lhs: Box<SExpr>, rhs: Box<SExpr>, op: SBinOp) -> Self {
            SExpr::BinOp(lhs, rhs, op)
        }
    }

    impl BinOpExpr<Box<SExprInt>> for SExprInt {
        fn binop_expr(lhs: Box<SExprInt>, rhs: Box<SExprInt>, op: SBinOp) -> Self {
            match op {
                SBinOp::NOp(op) => SExprInt::BinOp(lhs, rhs, op.try_into().unwrap()),
                _ => panic!("Invalid operation for SExprInt: {:?}", op),
            }
        }
    }

    impl IfExpr<Box<SExpr>, Box<SExpr>> for SExpr {
        fn if_expr(b: Box<SExpr>, t: Box<SExpr>, f: Box<SExpr>) -> Self {
            SExpr::If(b, t, f)
        }
    }

    impl IfExpr<Box<SExprInt>, Box<SExprBool>> for SExprInt {
        fn if_expr(b: Box<SExprBool>, t: Box<SExprInt>, f: Box<SExprInt>) -> Self {
            SExprInt::If(b, t, f)
        }
    }

    fn check_correct_error_type(result: &SemantResultStr, expected: &SemantResultStr) {
        // Checking that error type is correct but not the specific message
        if let (Err(res_errs), Err(exp_errs)) = (&result, &expected) {
            assert_eq!(res_errs.len(), exp_errs.len());
            let mut errs = zip(res_errs, exp_errs);
            assert!(
                errs.all(|(res, exp)| discriminant(res) == discriminant(exp)),
                "Error variants do not match: got {:?}, expected {:?}",
                res_errs,
                exp_errs
            );
        } else {
            // We didn't receive error - make assertion fail with nice output
            let msg = format!(
                "Expected error: {:?}. Received result: {:?}",
                expected, result
            );
            assert!(false, "{}", msg);
        }
    }

    fn check_correct_error_types(results: &Vec<SemantResultStr>, expected: &Vec<SemantResultStr>) {
        assert_eq!(
            results.len(),
            expected.len(),
            "Result and expected vectors must have the same length"
        );

        // Iterate over both vectors and call check_correct_error_type on each pair
        for (result, exp) in results.iter().zip(expected.iter()) {
            check_correct_error_type(result, exp);
        }
    }

    // // Helper function that returns all the sbinop variants at the time of writing these tests
    // // (Not guaranteed to be maintained)
    fn all_sbinop_variants() -> Vec<SBinOp> {
        vec![
            SBinOp::NOp(NumericalBinOp::Add),
            SBinOp::NOp(NumericalBinOp::Sub),
            SBinOp::NOp(NumericalBinOp::Mul),
            SBinOp::NOp(NumericalBinOp::Div),
        ]
    }

    // Function to generate combinations to use in tests, e.g., for binops
    fn generate_combinations<T, Expr, F>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        generate_expr: F,
    ) -> Vec<T>
    where
        // T: AsExpr<Box<Expr>>,
        Expr: Clone,
        F: Fn(Box<Expr>, Box<Expr>) -> T,
    {
        let mut vals = Vec::new();

        for a in variants_a.iter() {
            for b in variants_b.iter() {
                vals.push(generate_expr(Box::new(a.clone()), Box::new(b.clone())));
            }
        }

        vals
    }

    // Example usage for binary operations
    fn generate_binop_combinations<T, Expr>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        sbinops: Vec<SBinOp>,
    ) -> Vec<T>
    where
        T: BinOpExpr<Box<Expr>>,
        Expr: Clone,
    {
        let mut vals = Vec::new();

        for op in sbinops {
            vals.extend(generate_combinations(variants_a, variants_b, |lhs, rhs| {
                T::binop_expr(lhs, rhs, op.clone())
            }));
        }

        vals
    }

    fn generate_concat_combinations(
        variants_a: &[SExprStr],
        variants_b: &[SExprStr],
    ) -> Vec<SExprStr> {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            SExprStr::BinOp(Box::new(*lhs), Box::new(*rhs), StrBinOp::Concat)
        })
    }

    // // Example usage for if-expressions
    fn generate_if_combinations<T, Expr, BoolExpr: Clone>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        b_expr: Box<BoolExpr>,
    ) -> Vec<T>
    where
        T: IfExpr<Box<Expr>, Box<BoolExpr>>,
        Expr: Clone,
    {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            T::if_expr(b_expr.clone(), lhs, rhs)
        })
    }

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals: Vec<(SExprV, TCType)> = vec![
            (SExprV::Val(Value::Int(1)), TCType::Int),
            (SExprV::Val(Value::Str("".into())), TCType::Str),
            (SExprV::Val(Value::Bool(true)), TCType::Bool),
            (SExprV::Val(Value::Unit), TCType::Unit),
        ];
        let results = vals
            .iter()
            .map(|(v, _t)| v.type_check(&mut TypeInfo::new()));
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(1)))),
            Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                "".into(),
            )))),
            Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(
                true,
            )))),
            Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_deferred_err() {
        // Checks that if a Val is deferred during semantic analysis it produces a DeferredError
        let val = SExprV::Val(Value::Deferred);
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::DeferredError("".into(), None)]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Bool(false))),
                Box::new(SExprV::Val(Value::Bool(false))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Unit)),
                Box::new(SExprV::Val(Value::Unit)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::type_check_with_default)
            .collect();
        let expected: Vec<SemantResultStr> = vec![
            Err(vec![SemanticError::type_error(
                TypeErrorKind::OperatorTypeMismatch,
                "".into(),
            )]),
            Err(vec![SemanticError::type_error(
                TypeErrorKind::OperatorTypeMismatch,
                "".into(),
            )]),
        ];
        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_binop_err_diff_types() {
        // Checks that calling a BinOp on two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let sbinops = all_sbinop_variants();

        let vals_tmp = generate_binop_combinations(&variants, &variants, sbinops);
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV {
                    node: crate::lang::dsrv::ast::SExpr::BinOp(left, right, _),
                    ..
                } => {
                    // Only keep values where left != right
                    left != right
                }
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| TypeCheckable::type_check_with_default(&x))
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| {
                Err(vec![SemanticError::type_error(
                    TypeErrorKind::OperatorTypeMismatch,
                    "".into(),
                )])
            })
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_plus_err_deferred() {
        // Checks that if either value is deferred then Plus does not generate further errors
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(0))),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Int(0))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
        ];
        let results = vals.iter().map(TypeCheckable::type_check_with_default);
        let expected_err_lens = vec![1, 1, 2];

        // For each result, check that we got errors and that we got the correct amount:
        for (res, exp_err_len) in zip(results, expected_err_lens) {
            match res {
                Err(errs) => {
                    assert_eq!(
                        errs.len(),
                        exp_err_len,
                        "Expected {} errors but got {}: {:?}",
                        exp_err_len,
                        errs.len(),
                        errs
                    );
                }
                Ok(_) => {
                    assert!(
                        false,
                        "Expected an error but got a successful result: {:?}",
                        res
                    );
                }
            }
        }
    }

    #[test]
    fn test_int_binop_ok() {
        // Checks that if we BinOp two Ints together it results in typed AST after semantic analysis
        let int_val = vec![SExprV::Val(Value::Int(0))];
        let sbinops = all_sbinop_variants();
        let vals: Vec<SExpr> = generate_binop_combinations(&int_val, &int_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let int_t_val = vec![SExprInt::Val(PartialStreamValue::Known(0))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprInt> =
            generate_binop_combinations(&int_t_val, &int_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Int(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_str_plus_ok() {
        // Checks that if we add two Strings together it results in typed AST after semantic analysis
        let str_val = vec![SExprV::Val(Value::Str("".into()))];
        let sbinops = vec![SBinOp::SOp(StrBinOp::Concat)];
        let vals: Vec<SExpr> = generate_binop_combinations(&str_val, &str_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let str_t_val = vec![SExprStr::Val(PartialStreamValue::Known("".into()))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprStr> = generate_concat_combinations(&str_t_val, &str_t_val);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Str(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_ok() {
        // Checks that typechecking if-statements with identical types for if- and else- part results in correct typed AST

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let val_variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let bexpr = Box::new(SExpr::Val(true));
        let bexpr_checked = Box::new(SExprBool::Val(PartialStreamValue::Known(true)));

        let vals_tmp = generate_if_combinations(&val_variants, &val_variants, bexpr.clone());

        // Only consider cases where true and false cases are equal
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV {
                    node: crate::lang::dsrv::ast::SExpr::If(_, t, f),
                    ..
                } => t == f,
                _ => true, // Keep non-ifs (unused in this case)
            }
        });
        let results = vals.map(|x| x.type_check_with_default());

        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::If(
                bexpr_checked.clone(),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
            ))),
            Ok(SExprTE::Str(SExprStr::If(
                bexpr_checked.clone(),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
            ))),
            Ok(SExprTE::Bool(SExprBool::If(
                bexpr_checked.clone(),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            ))),
            Ok(SExprTE::Unit(SExprUnit::If(
                bexpr_checked.clone(),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_err() {
        // Checks that creating an if-expression with two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        let bexpr = Box::new(SExpr::Val(true));

        let vals_tmp = generate_if_combinations(&variants, &variants, bexpr.clone());
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV {
                    node: crate::lang::dsrv::ast::SExpr::If(_, t, f),
                    ..
                } => t != f,
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| x.type_check_with_default())
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| {
                Err(vec![SemanticError::type_error(
                    TypeErrorKind::OperatorTypeMismatch,
                    "".into(),
                )])
            })
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_var_ok() {
        // Checks that Vars are correctly typechecked if they exist in the context

        let variant_names = vec!["int", "str", "bool", "unit"];
        let variant_types = vec![
            StreamType::Int,
            StreamType::Str,
            StreamType::Bool,
            StreamType::Unit,
        ];
        let vals = variant_names
            .clone()
            .into_iter()
            .map(|n| SExprV::Var(n.into()));

        // Fake context/environment that simulates type-checking context
        let mut ctx = TypeInfo::new();
        for (n, t) in variant_names.into_iter().zip(variant_types.into_iter()) {
            ctx.insert(n.into(), t);
        }

        let results = vals.into_iter().map(|sexpr| sexpr.type_check(&mut ctx));

        let expected = vec![
            Ok(SExprTE::Int(SExprInt::Var("int".into()))),
            Ok(SExprTE::Str(SExprStr::Var("str".into()))),
            Ok(SExprTE::Bool(SExprBool::Var("bool".into()))),
            Ok(SExprTE::Unit(SExprUnit::Var("unit".into()))),
        ];

        assert!(results.eq(expected));
    }

    #[test]
    fn test_var_err() {
        // Checks that Vars produce UndeclaredVariable errors if they do not exist in the context

        let val = SExprV::Var("undeclared_name".into());
        let result = val.type_check_with_default();
        let expected: SemantResultStr =
            Err(vec![SemanticError::UndeclaredVariable("".into(), None)]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_dodgy_if() {
        let dodgy_bexpr = SExpr::BinOp(
            Box::new(SExprV::Val(Value::Int(0))),
            Box::new(SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(3))),
                Box::new(SExprV::Val(Value::Str("Banana".into()))),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            SBinOp::COp(CompBinOp::Eq),
        );
        let sexpr = SExprV::If(
            Box::new(dodgy_bexpr),
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Val(Value::Int(2))),
        );
        if let Ok(_) = sexpr.type_check_with_default() {
            assert!(false, "Expected type error but got a successful result");
        }
    }

    #[test]
    fn test_defer_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_no_eval() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_incorrect_inner_type() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Bool);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                eco_vec!["x".into()],
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
                eco_vec!["x".into()],
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_int_missing_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
                eco_vec!["x".into()],
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Str),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeInfo::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Str(SExprStr::BinOp(
            Box::new(SExprStr::Val(PartialStreamValue::Known("hello".into()))),
            Box::new(SExprStr::RestrictedDynamic(
                Box::new(SExprStr::Var("x".into())),
                EcoVec::from(vec!["x".into(), "y".into()]),
                expected_ctx.clone(),
            )),
            StrBinOp::Concat,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_restricted_dynamic_nested_int_ascription_error() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("test".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        // Type ascription mismatch should cause error
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_typed_spec_display_output() {
        let input_vars = BTreeSet::from(["x".into(), "y".into()]);
        let output_vars = BTreeSet::from(["z".into(), "d".into()]);

        let mut dynamic_ctx = BTreeMap::new();
        dynamic_ctx.insert("s".into(), StreamType::Str);

        let exprs = BTreeMap::from([
            (
                "z".into(),
                SExprTE::Int(SExprInt::BinOp(
                    Box::new(SExprInt::Var("x".into())),
                    Box::new(SExprInt::Var("y".into())),
                    IntBinOp::Add,
                )),
            ),
            (
                "d".into(),
                SExprTE::Int(SExprInt::Dynamic(
                    Box::new(SExprStr::Var("s".into())),
                    dynamic_ctx,
                )),
            ),
        ]);

        let type_annotations = BTreeMap::from([
            ("x".into(), StreamType::Int),
            ("y".into(), StreamType::Int),
            ("s".into(), StreamType::Str),
            ("z".into(), StreamType::Int),
            ("d".into(), StreamType::Int),
        ]);

        let spec = TypedDsrvSpecification {
            input_vars,
            stream_vars: output_vars.clone(),
            output_vars,
            aux_vars: BTreeSet::new(),
            exprs,
            type_annotations,
        };

        let rendered = format!("{}", spec);
        let lines: Vec<&str> = rendered.lines().collect();

        assert!(lines.contains(&"in x: Int"));
        assert!(lines.contains(&"in y: Int"));
        assert!(lines.contains(&"out z: Int"));
        assert!(lines.contains(&"out d: Int"));
        assert!(lines.contains(&"z = (x + y)"));
        assert!(lines.contains(&"d = dynamic(s: Int)"));
    }

    #[test]
    fn test_display_with_type_for_container_types() {
        let list = SExprTE::List(TypedListExpr {
            element_type: TCType::Int,
            kind: TypedListExprKind::Literal(vec![
                SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(1))),
                SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(2))),
            ]),
        });
        let map = SExprTE::Map(TypedMapExpr {
            value_type: TCType::Bool,
            kind: TypedMapExprKind::Literal(BTreeMap::from([(
                "ok".into(),
                SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(true))),
            )])),
        });
        let structure = SExprTE::Struct(TypedStructExpr {
            typ_map: eco_vec![
                ("id".into(), TCType::Int),
                ("flags".into(), TCType::List(Box::new(TCType::Bool))),
            ],
            allow_extra_fields: false,
            kind: TypedStructExprKind::Literal(vec![
                (
                    "id".into(),
                    SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(7))),
                ),
                (
                    "flags".into(),
                    SExprTE::List(TypedListExpr {
                        element_type: TCType::Bool,
                        kind: TypedListExprKind::Literal(vec![
                            SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(true))),
                            SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(false))),
                        ]),
                    }),
                ),
            ]),
        });

        assert_eq!(
            format!("{}", list.display_with_type()),
            "List(1, 2): List<Int>"
        );
        assert_eq!(
            format!("{}", map.display_with_type()),
            "Map(\"ok\": true): Map<Bool>"
        );
        assert_eq!(
            format!("{}", structure.display_with_type()),
            "Struct(\"id\": 7, \"flags\": List(true, false)): Struct<id: Int, flags: List<Bool>>"
        );
    }

    #[test]
    fn test_typed_spec_display_roundtrips_container_outputs() {
        let input_vars = BTreeSet::from(["tick".into()]);
        let output_vars = BTreeSet::from(["xs".into(), "m".into(), "robot".into()]);

        let exprs = BTreeMap::from([
            (
                "xs".into(),
                SExprTE::List(TypedListExpr {
                    element_type: TCType::Int,
                    kind: TypedListExprKind::Literal(vec![
                        SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(1))),
                        SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(2))),
                    ]),
                }),
            ),
            (
                "m".into(),
                SExprTE::Map(TypedMapExpr {
                    value_type: TCType::Bool,
                    kind: TypedMapExprKind::Literal(BTreeMap::from([(
                        "ok".into(),
                        SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(true))),
                    )])),
                }),
            ),
            (
                "robot".into(),
                SExprTE::Struct(TypedStructExpr {
                    typ_map: eco_vec![
                        ("id".into(), TCType::Int),
                        ("flags".into(), TCType::List(Box::new(TCType::Bool))),
                    ],
                    allow_extra_fields: false,
                    kind: TypedStructExprKind::Literal(vec![
                        (
                            "id".into(),
                            SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(7))),
                        ),
                        (
                            "flags".into(),
                            SExprTE::List(TypedListExpr {
                                element_type: TCType::Bool,
                                kind: TypedListExprKind::Literal(vec![
                                    SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(true))),
                                    SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(false))),
                                ]),
                            }),
                        ),
                    ]),
                }),
            ),
        ]);

        let type_annotations = BTreeMap::from([
            ("tick".into(), StreamType::Int),
            ("xs".into(), StreamType::List(Box::new(StreamType::Int))),
            ("m".into(), StreamType::Map(Box::new(StreamType::Bool))),
            (
                "robot".into(),
                StreamType::Struct(
                    eco_vec![
                        ("id".into(), StreamType::Int),
                        ("flags".into(), StreamType::List(Box::new(StreamType::Bool))),
                    ],
                    false,
                ),
            ),
        ]);

        let spec = TypedDsrvSpecification {
            input_vars,
            stream_vars: output_vars.clone(),
            output_vars,
            aux_vars: BTreeSet::new(),
            exprs,
            type_annotations,
        };

        let rendered = format!("{}", spec);
        let mut input = rendered.as_str();
        let parsed = crate::lang::dsrv::parser::dsrv_specification(&mut input).unwrap_or_else(
            |err| panic!("container typed Display output should parse as a DsrvSpecification:\n{rendered}\n{err:?}"),
        );

        assert_eq!(parsed.type_annotations, spec.type_annotations);
        assert_eq!(parsed.exprs.len(), spec.exprs.len());
    }

    fn arb_typed_roundtrip_spec() -> impl Strategy<Value = TypedDsrvSpecification> {
        let fixed_inputs: BTreeSet<VarName> = BTreeSet::from(["a".into(), "b".into(), "s".into()]);

        let all_vars: Vec<VarName> =
            vec!["a".into(), "b".into(), "s".into(), "x".into(), "y".into()];

        (
            crate::lang::dsrv::ast::generation::arb_int_sexpr(all_vars.clone()),
            crate::lang::dsrv::ast::generation::arb_int_sexpr(all_vars.clone()),
            "[a-z]{1,3}",
        )
            .prop_filter_map(
                "generated expressions must typecheck in fixed context",
                move |(expr_x, expr_y, env_name)| {
                    let mut ctx = TypeInfo::new();
                    ctx.insert("a".into(), StreamType::Int);
                    ctx.insert("b".into(), StreamType::Int);
                    ctx.insert("s".into(), StreamType::Str);
                    ctx.insert("x".into(), StreamType::Int);
                    ctx.insert("y".into(), StreamType::Int);

                    let typed_x = expr_x.type_check(&mut ctx).ok()?;
                    let typed_y = expr_y.type_check(&mut ctx).ok()?;

                    let typed_x = match typed_x {
                        SExprTE::Int(e) => SExprTE::Int(e),
                        _ => return None,
                    };
                    let typed_y = match typed_y {
                        SExprTE::Int(e) => SExprTE::Int(e),
                        _ => return None,
                    };

                    let env_var: VarName = env_name.as_str().into();
                    let typed_dyn = SExprTE::Int(SExprInt::RestrictedDynamic(
                        Box::new(SExprStr::Var("s".into())),
                        eco_vec![env_var],
                        BTreeMap::new(),
                    ));

                    Some(TypedDsrvSpecification {
                        input_vars: fixed_inputs.clone(),
                        output_vars: BTreeSet::from(["x".into(), "y".into(), "d".into()]),
                        aux_vars: BTreeSet::new(),
                        stream_vars: BTreeSet::from(["x".into(), "y".into(), "d".into()]),
                        exprs: BTreeMap::from([
                            ("x".into(), typed_x),
                            ("y".into(), typed_y),
                            ("d".into(), typed_dyn),
                        ]),
                        type_annotations: BTreeMap::from([
                            ("a".into(), StreamType::Int),
                            ("b".into(), StreamType::Int),
                            ("s".into(), StreamType::Str),
                            ("x".into(), StreamType::Int),
                            ("y".into(), StreamType::Int),
                            ("d".into(), StreamType::Int),
                        ]),
                    })
                },
            )
    }

    proptest! {
        #[test]
        fn test_prop_typed_spec_display_parse_roundtrip(spec in arb_typed_roundtrip_spec()) {
            let rendered = format!("{}", spec);
            let mut input = rendered.as_str();
            let parsed = crate::lang::dsrv::parser::dsrv_specification(&mut input)
                .expect("Typed Display output should parse as a Specification");

            prop_assert_eq!(parsed.input_vars, spec.input_vars);
            prop_assert_eq!(parsed.output_vars, spec.output_vars);
            prop_assert_eq!(parsed.type_annotations, spec.type_annotations);
            prop_assert_eq!(parsed.exprs.len(), spec.exprs.len());
        }
    }

    // -----------------------------------------------------------------------
    // List type-checking tests
    // -----------------------------------------------------------------------

    // --- Value::List literal type determination ---

    #[test]
    fn test_list_val_flat_int() {
        let val = Value::List(EcoVec::from(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    #[test]
    fn test_list_val_flat_bool() {
        let val = Value::List(EcoVec::from(vec![Value::Bool(true), Value::Bool(false)]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Bool));
    }

    #[test]
    fn test_list_val_flat_str() {
        let val = Value::List(EcoVec::from(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Str));
    }

    #[test]
    fn test_list_val_nested_list_of_list_int() {
        // [[1, 2], [3]]
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1), Value::Int(2)])),
            Value::List(EcoVec::from(vec![Value::Int(3)])),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::list(TCType::Int)));
    }

    #[test]
    fn test_list_val_triple_nested() {
        // [[[true]]]
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Bool(true)])),
        ]))]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        let te = result.unwrap();
        assert_eq!(
            extract_type(&te),
            TCType::list(TCType::list(TCType::list(TCType::Bool)))
        );
    }

    #[test]
    fn test_list_val_empty_without_expected() {
        // Without expected type, empty list literal succeeds with empty-list element type
        let val = Value::List(EcoVec::new());
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for empty list literal with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_list_val_empty_ok_with_expected() {
        // With expected type, empty list literal succeeds
        let val = Value::List(EcoVec::new());
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = val.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for empty list with expected type, got {:?}",
            errs
        );
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    #[test]
    fn test_list_val_empty_nested_ok_with_expected() {
        // [[]] with expected List<List<Int>> should succeed
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::new())]));
        let expected = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        let mut errs = vec![];
        let result = val.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with expected List<List<Int>>, got {:?}",
            errs
        );
        let te = result.unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::list(TCType::Int)));
    }

    #[test]
    fn test_list_val_nested_inner_empty_without_expected() {
        // [[]] — without expected type, the inner empty list gets empty-list element type.
        // The outer list's first element is List(Poly), so the outer list is
        // List(List(Poly)).
        let val = Value::List(EcoVec::from(vec![Value::List(EcoVec::new())]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::EmptyList))
        );
    }

    #[test]
    fn test_list_val_mixed_element_types_error() {
        // [1, "hello"] — first element is Int, second is Str
        let val = Value::List(EcoVec::from(vec![
            Value::Int(1),
            Value::Str("hello".into()),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err(), "Expected Err for mixed element types");
    }

    #[test]
    fn test_list_val_nested_mixed_depth_error() {
        // [[1, 2], 3] — first element is List<Int>, second is Int
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1), Value::Int(2)])),
            Value::Int(3),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err(), "Expected Err for mixed nesting depths");
    }

    #[test]
    fn test_list_val_nested_inconsistent_inner_type_error() {
        // [[1], [true]] — first inner is List<Int>, second is List<Bool>
        let val = Value::List(EcoVec::from(vec![
            Value::List(EcoVec::from(vec![Value::Int(1)])),
            Value::List(EcoVec::from(vec![Value::Bool(true)])),
        ]));
        let expr = SExprV::Val(val);
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for inconsistent nested element types"
        );
    }

    // --- SExpr::List (expression-level list literal) ---

    #[test]
    fn test_sexpr_list_flat_int() {
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(10)),
            SExpr::Val(Value::Int(20)),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_sexpr_list_nested() {
        // List of list-literals: [[1], [2, 3]]
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ])),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_sexpr_list_empty_without_expected() {
        // Without expected type, empty SExpr::List succeeds with empty-list element type
        let expr = SExpr::List(EcoVec::new());
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for empty SExpr::List with empty-list element type, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_sexpr_list_empty_ok_with_expected() {
        // With expected type, empty SExpr::List succeeds
        let expr = SExpr::List(EcoVec::new());
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for empty SExpr::List with expected type, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_sexpr_list_empty_nested_ok_with_expected() {
        // [[]] with expected List<List<Bool>> should succeed
        let expr = SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::new())]));
        let expected = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool))));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for [[]] with expected List<List<Bool>>, got {:?}",
            errs
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Bool))
        );
    }

    #[test]
    fn test_sexpr_list_mixed_types_error() {
        let expr = SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Bool(true)),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- Empty lists in compound expressions ---

    #[test]
    fn test_default_empty_list_with_typed_list() {
        // default([], xs) where xs : List<Int> → List<Int>
        // The expected type from xs propagates to the empty list via the Default handler.
        let expr = SExpr::Default(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut ctx, &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for default([], xs) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_if_empty_list_branch_with_expected() {
        // if true then [] else [1, 2] with expected List<Int>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ]))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for if(true, [], [1,2]) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_init_empty_list_with_expected() {
        // init([], xs) where xs : List<Str> with expected List<Str>
        let expr = SExpr::Init(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Str)));
        let expected = StreamType::List(Box::new(StreamType::Str));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut ctx, &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for init([], xs) with expected List<Str>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Str));
    }

    #[test]
    fn test_lconcat_empty_list_with_expected() {
        // concat([], [1]) with expected List<Int>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for concat([], [1]) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_to_empty_list_with_expected() {
        // append([], 42) with expected List<Int>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let expected = StreamType::List(Box::new(StreamType::Int));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for append([], 42) with expected List<Int>, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    // --- Empty lists in type-erasing operations ---
    // Operations like is_defined and len produce a fixed output type (Bool / Int)
    // that does not constrain their input type.  They pass None as the expected
    // type to their argument, so the empty list falls back to empty-list element type.
    // Because these operations never inspect the element type, the Poly list
    // passes through successfully.

    #[test]
    fn test_is_defined_empty_list_ok() {
        // is_defined([]) — the empty list gets empty-list element type; is_defined
        // does not inspect the element type, so it succeeds.
        let expr = SExpr::IsDefined(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for is_defined([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_is_defined_empty_list_ok_with_outer_expected() {
        // With outer expected Bool, same outcome.
        let expr = SExpr::IsDefined(Box::new(SExpr::List(EcoVec::new())));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&StreamType::Bool), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for is_defined([]) with outer expected Bool, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_llen_empty_list_ok() {
        // len([]) — the empty list gets empty-list element type; len does not
        // inspect the element type, so it succeeds.
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for len([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_empty_list_ok_with_outer_expected() {
        // With outer expected Int, same outcome.
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::new())));
        let mut errs = vec![];
        let result = expr.type_check_raw(Some(&StreamType::Int), &mut TypeInfo::new(), &mut errs);
        assert!(
            result.is_ok(),
            "Expected Ok for len([]) with outer expected Int, got {:?}",
            errs
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_when_empty_list_ok() {
        // when([]) — same as is_defined: does not inspect element type.
        let expr = SExpr::When(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for when([]) via Poly, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_ltail_empty_list_ok() {
        // tail([]) — preserves the list type including Poly.
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for tail([]) via Poly, got {:?}",
            result
        );
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::EmptyList)
        );
    }

    #[test]
    fn test_lhead_empty_list_poly_errors() {
        // head([]) — the element type is Poly and head needs a concrete element
        // type, so this is correctly rejected.
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::new())));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for head([]) with empty-list element type"
        );
    }

    #[test]
    fn test_lindex_empty_list_poly_errors() {
        // [][0] — same: index needs a concrete element type.
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_err(),
            "Expected Err for [][0] with empty-list element type"
        );
    }

    #[test]
    fn test_default_empty_list_with_concrete_no_expected() {
        // default([], xs) where xs : List<Int>, without outer expected type.
        // The empty list gets Poly; unification with Int succeeds.
        let expr = SExpr::Default(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Var("xs".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(
            result.is_ok(),
            "Expected Ok for default([], xs) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lconcat_empty_list_with_concrete_no_expected() {
        // concat([], [1]) without outer expected type.
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for concat([], [1]) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_empty_list_no_expected() {
        // append([], 42) without outer expected type.
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::new())),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(
            result.is_ok(),
            "Expected Ok for append([], 42) via empty-literal unification, got {:?}",
            result
        );
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    // --- LHead ---

    #[test]
    fn test_lhead_flat_int() {
        // head([1, 2]) : Int
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lhead_flat_bool() {
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(
            Value::Bool(true),
        )]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_lhead_nested_produces_list() {
        // head([[1, 2], [3]]) : List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_triple_nested() {
        // head([[[1]]]) : List<List<Int>>
        let expr = SExpr::LHead(Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(
            EcoVec::from(vec![SExpr::List(EcoVec::from(vec![SExpr::Val(
                Value::Int(1),
            )]))]),
        )]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lhead_of_non_list_error() {
        let expr = SExpr::LHead(Box::new(SExpr::Val(Value::Int(42))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lhead_of_lhead_nested() {
        // head(head([[[ 7 ]]])) : List<Int>  then head again : Int
        // Inner: [[[7]]] is List<List<List<Int>>>
        // head(.) → List<List<Int>>
        // head(.) → List<Int>
        let innermost = SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(7))]));
        let middle = SExpr::List(EcoVec::from(vec![innermost]));
        let outer = SExpr::List(EcoVec::from(vec![middle]));
        let head_once = SExpr::LHead(Box::new(outer));
        let head_twice = SExpr::LHead(Box::new(head_once));
        let result = head_twice.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_of_lhead_of_lhead_to_scalar() {
        // head(head(head([[[42]]]))) : Int
        let innermost = SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(42))]));
        let middle = SExpr::List(EcoVec::from(vec![innermost]));
        let outer = SExpr::List(EcoVec::from(vec![middle]));
        let h1 = SExpr::LHead(Box::new(outer));
        let h2 = SExpr::LHead(Box::new(h1));
        let h3 = SExpr::LHead(Box::new(h2));
        let result = h3.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    // --- LTail ---

    #[test]
    fn test_ltail_flat() {
        // tail([1, 2, 3]) : List<Int>
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
            SExpr::Val(Value::Int(3)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_ltail_nested() {
        // tail([[1], [2], [3]]) : List<List<Int>>
        let expr = SExpr::LTail(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(2))])),
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_ltail_of_non_list_error() {
        let expr = SExpr::LTail(Box::new(SExpr::Val(Value::Bool(true))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LLen ---

    #[test]
    fn test_llen_flat() {
        // len([1, 2]) : Int
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::Val(Value::Int(1)),
            SExpr::Val(Value::Int(2)),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_nested() {
        // len([[1], [2, 3]]) : Int  (length of outer list = 2)
        let expr = SExpr::LLen(Box::new(SExpr::List(EcoVec::from(vec![
            SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))])),
            SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ])),
        ]))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_llen_of_non_list_error() {
        let expr = SExpr::LLen(Box::new(SExpr::Val(Value::Int(1))));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LIndex ---

    #[test]
    fn test_lindex_flat_int() {
        // [10, 20][0] : Int
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(10)),
                SExpr::Val(Value::Int(20)),
            ]))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lindex_nested_produces_list() {
        // [[1, 2], [3]][0] : List<Int>
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::List(EcoVec::from(vec![
                    SExpr::Val(Value::Int(1)),
                    SExpr::Val(Value::Int(2)),
                ])),
                SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(3))])),
            ]))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lindex_non_int_index_error() {
        let expr = SExpr::LIndex(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::Val(Value::Bool(true))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lindex_non_list_error() {
        let expr = SExpr::LIndex(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LAppend ---

    #[test]
    fn test_lappend_flat() {
        // append([1, 2], 3) : List<Int>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(1)),
                SExpr::Val(Value::Int(2)),
            ]))),
            Box::new(SExpr::Val(Value::Int(3))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lappend_nested() {
        // append([[1]], [2, 3]) : List<List<Int>>
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lappend_type_mismatch_error() {
        // append([1, 2], "hello") — element type mismatch
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::Val(Value::Str("hello".into()))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lappend_nested_type_mismatch_error() {
        // append([[1]], 42) — expects List<Int> element, got Int
        let expr = SExpr::LAppend(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::Val(Value::Int(42))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lappend_to_non_list_error() {
        let expr = SExpr::LAppend(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::Val(Value::Int(2))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- LConcat ---

    #[test]
    fn test_lconcat_flat() {
        // concat([1], [2, 3]) : List<Int>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![
                SExpr::Val(Value::Int(2)),
                SExpr::Val(Value::Int(3)),
            ]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lconcat_nested() {
        // concat([[1]], [[2]]) : List<List<Int>>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(1))],
            ))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(2))],
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Int))
        );
    }

    #[test]
    fn test_lconcat_type_mismatch_error() {
        // concat([1], ["a"]) — different element types
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Str(
                "a".into(),
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lconcat_nesting_mismatch_error() {
        // concat([1], [[2]]) — List<Int> vs List<List<Int>>
        let expr = SExpr::LConcat(
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(1))]))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::List(EcoVec::from(
                vec![SExpr::Val(Value::Int(2))],
            ))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_lconcat_non_list_error() {
        let expr = SExpr::LConcat(
            Box::new(SExpr::Val(Value::Int(1))),
            Box::new(SExpr::List(EcoVec::from(vec![SExpr::Val(Value::Int(2))]))),
        );
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_err());
    }

    // --- Var with list types ---

    #[test]
    fn test_var_list_int() {
        let expr = SExpr::Var("xs".into());
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_var_list_nested() {
        let expr = SExpr::Var("xss".into());
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Bool))
        );
    }

    #[test]
    fn test_lhead_of_var_nested() {
        // head(xss) where xss : List<List<Int>> → List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_lhead_of_lhead_of_var_to_scalar() {
        // head(head(xss)) where xss : List<List<Int>> → Int
        let expr = SExpr::LHead(Box::new(SExpr::LHead(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_ltail_of_var_nested() {
        // tail(xss) where xss : List<List<Str>> → List<List<Str>>
        let expr = SExpr::LTail(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Str)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::list(TCType::list(TCType::Str))
        );
    }

    #[test]
    fn test_llen_of_var_nested() {
        // len(xss) where xss : List<List<Int>> → Int
        let expr = SExpr::LLen(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lindex_of_var_nested() {
        // xss[0] where xss : List<List<Float>> → List<Float>
        let expr = SExpr::LIndex(
            Box::new(SExpr::Var("xss".into())),
            Box::new(SExpr::Val(Value::Int(0))),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Float)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Float));
    }

    // --- Float type checking ---

    #[test]
    fn test_default_float_ok() {
        // default(x, y) where x, y : Float
        let expr = SExpr::Default(
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Float);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Float);
    }

    #[test]
    fn test_if_float_ok() {
        // if true then x else y where x, y : Float
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Float);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Float);
    }

    #[test]
    fn test_default_float_type_mismatch_error() {
        // default(x, y) where x : Float, y : Int → error
        let expr = SExpr::Default(
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_if_float_type_mismatch_error() {
        // if true then x else y where x : Float, y : Int → error
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("x".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("x".into(), StreamType::Float);
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- List operations with non-scalar types ---

    #[test]
    fn test_default_nested_lists() {
        // default(xs, ys) where xs, ys : List<List<Int>>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_default_nested_list_type_mismatch_error() {
        // default(xs, ys) where xs : List<Int>, ys : List<Bool>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Bool)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_nested_list_depth_mismatch_error() {
        // default(xs, ys) where xs : List<Int>, ys : List<List<Int>>
        let expr = SExpr::Default(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert(
            "ys".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- If on nested lists ---

    #[test]
    fn test_if_nested_lists() {
        // if true then xs else ys where xs, ys : List<List<Int>>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_if_nested_list_branch_type_mismatch_error() {
        // if true then xs else ys where xs : List<Int>, ys : List<Str>
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Str)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_if_list_vs_scalar_branch_error() {
        // if true then xs else y where xs : List<Int>, y : Int
        let expr = SExpr::If(
            Box::new(SExpr::Val(Value::Bool(true))),
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("y".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("y".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- Init on nested lists ---

    #[test]
    fn test_init_nested_lists() {
        // init(xs, ys) where xs, ys : List<List<Bool>>
        let expr = SExpr::Init(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Bool))));
        ctx.insert("xs".into(), ty.clone());
        ctx.insert("ys".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_init_nested_list_type_mismatch_error() {
        // init(xs, ys) where xs : List<Int>, ys : List<Float>
        let expr = SExpr::Init(
            Box::new(SExpr::Var("xs".into())),
            Box::new(SExpr::Var("ys".into())),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        ctx.insert("ys".into(), StreamType::List(Box::new(StreamType::Float)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    // --- IsDefined on nested lists ---

    #[test]
    fn test_is_defined_nested_list() {
        // is_defined(xss) where xss : List<List<Int>> → Bool
        let expr = SExpr::IsDefined(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_is_defined_flat_list() {
        // is_defined(xs) where xs : List<Str> → Bool
        let expr = SExpr::IsDefined(Box::new(SExpr::Var("xs".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Str)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    // --- When on nested lists ---

    #[test]
    fn test_when_nested_list() {
        // when(xss) where xss : List<List<Float>> → Bool
        let expr = SExpr::When(Box::new(SExpr::Var("xss".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Float)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    #[test]
    fn test_when_flat_list() {
        // when(xs) where xs : List<Int> → Bool
        let expr = SExpr::When(Box::new(SExpr::Var("xs".into())));
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }

    // --- SIndex on nested lists ---

    #[test]
    fn test_sindex_on_nested_list_var() {
        // xs[-1] (stream index) where xs : List<List<Int>> → List<List<Int>>
        let expr = SExpr::SIndex(Box::new(SExpr::Var("xs".into())), 1);
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xs".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    // --- Compositions: head of tail, len of head, etc. ---

    #[test]
    fn test_lhead_of_ltail_nested() {
        // head(tail(xss)) where xss : List<List<Int>> → List<Int>
        let expr = SExpr::LHead(Box::new(SExpr::LTail(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_llen_of_lhead_nested() {
        // len(head(xss)) where xss : List<List<Int>> → Int
        let expr = SExpr::LLen(Box::new(SExpr::LHead(Box::new(SExpr::Var("xss".into())))));
        let mut ctx = TypeInfo::new();
        ctx.insert(
            "xss".into(),
            StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int)))),
        );
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_lappend_head_to_tail_nested() {
        // append(tail(xss), head(xss)) where xss : List<List<Int>> → List<List<Int>>
        // tail(xss) : List<List<Int>>, head(xss) : List<Int>
        let xss = || SExpr::Var("xss".into());
        let expr = SExpr::LAppend(
            Box::new(SExpr::LTail(Box::new(xss()))),
            Box::new(SExpr::LHead(Box::new(xss()))),
        );
        let mut ctx = TypeInfo::new();
        let ty = StreamType::List(Box::new(StreamType::List(Box::new(StreamType::Int))));
        ctx.insert("xss".into(), ty.clone());
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(
            extract_type(&result.unwrap()),
            TCType::from_stream_type(&ty)
        );
    }

    #[test]
    fn test_lconcat_of_ltail() {
        // concat(tail(xs), tail(xs)) where xs : List<Int> → List<Int>
        let xs = || SExpr::Var("xs".into());
        let expr = SExpr::LConcat(
            Box::new(SExpr::LTail(Box::new(xs()))),
            Box::new(SExpr::LTail(Box::new(xs()))),
        );
        let mut ctx = TypeInfo::new();
        ctx.insert("xs".into(), StreamType::List(Box::new(StreamType::Int)));
        let result = expr.type_check(&mut ctx);
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::list(TCType::Int));
    }

    #[test]
    fn test_sexpr_map_literal_and_get() {
        let expr = SExpr::Map(BTreeMap::from([
            ("x".into(), SExpr::Val(Value::Int(1))),
            ("y".into(), SExpr::Val(Value::Int(2))),
        ]));
        let result = expr.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Int));

        let get = SExpr::MGet(Box::new(expr), "x".into());
        let result = get.type_check(&mut TypeInfo::new());
        assert!(result.is_ok(), "Expected Ok, got {:?}", result);
        assert_eq!(extract_type(&result.unwrap()), TCType::Int);
    }

    #[test]
    fn test_sexpr_map_insert_remove_and_has_key() {
        let empty = SExpr::Map(BTreeMap::new());
        let expected = StreamType::Map(Box::new(StreamType::Bool));
        let mut errs = vec![];
        let inserted = SExpr::MInsert(
            Box::new(empty.clone()),
            "flag".into(),
            Box::new(SExpr::Val(Value::Bool(true))),
        );
        let result = inserted.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Bool));

        let removed = SExpr::MRemove(Box::new(empty.clone()), "flag".into());
        let mut errs = vec![];
        let result = removed.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::map(TCType::Bool));

        let has_key = SExpr::MHasKey(Box::new(empty), "flag".into());
        let mut errs = vec![];
        let result = has_key.type_check_raw(Some(&expected), &mut TypeInfo::new(), &mut errs);
        assert!(result.is_ok(), "Expected Ok, got {:?}", errs);
        assert_eq!(extract_type(&result.unwrap()), TCType::Bool);
    }
}
