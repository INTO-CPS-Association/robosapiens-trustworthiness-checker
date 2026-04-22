//! The strict type-checking driver, which requires an explicit type
//! annotation for every variable in the specification.

use super::*;
use crate::UntypedDsrvSpecification;
use std::collections::BTreeMap;

pub fn type_check(spec: UntypedDsrvSpecification) -> SemanticResult<TypedDsrvSpecification> {
    let type_context = spec.type_annotations.clone();
    let mut typed_exprs = BTreeMap::new();
    let mut errors = vec![];
    for (var, expr) in spec.exprs.iter() {
        let mut ctx = type_context.clone();
        let expected = match ctx.get(var).cloned() {
            Some(t) => t,
            None => {
                errors.push(SemanticError::MissingTypeAnnotation(
                    format!("Variable {} is missing a type annotation", var),
                    None,
                ));
                continue;
            }
        };
        let typed_expr = expr.type_check_raw(Some(&expected), &mut ctx, &mut errors);
        // Check consistency of inferred type with the declared type annotation
        if let Ok(ref te) = typed_expr {
            let actual = extract_type(te);
            if actual != TCType::from_stream_type(&expected) {
                errors.push(SemanticError::type_error_at(
                    TypeErrorKind::AnnotationTypeMismatch,
                    format!(
                        "Variable {} has declared type {}, but expression has type {}",
                        var, expected, actual
                    ),
                    expr.span,
                ));
            }
        }
        typed_exprs.insert(var, typed_expr);
    }
    if errors.is_empty() {
        Ok(TypedDsrvSpecification {
            input_vars: spec.input_vars.clone(),
            output_vars: spec.output_vars.clone(),
            aux_vars: spec.aux_vars.clone(),
            stream_vars: spec.stream_vars.clone(),
            exprs: typed_exprs
                .into_iter()
                .map(|(k, v)| (k.clone(), v.unwrap()))
                .collect(),
            type_annotations: spec.type_annotations.clone(),
        })
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StreamType;
    use crate::lang::core::parser::SpecParser;
    use crate::lang::dsrv::ast::SExpr;
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::lang::dsrv::parser::dsrv_specification;
    use crate::lang::dsrv::span::Span;
    use crate::{Specification, VarName};
    use ecow::EcoVec;
    use std::collections::{BTreeMap, BTreeSet};
    use test_log::test;

    #[test]
    fn test_top_level_type_check_empty_list_output() {
        // Simulates a full spec where an output variable is assigned []
        // and its declared type is List<Int>.
        let mut exprs = BTreeMap::new();
        let var: VarName = "y".into();
        exprs.insert(var.clone(), SExpr::List(EcoVec::new()));
        let mut type_annotations = BTreeMap::new();
        type_annotations.insert(var.clone(), StreamType::List(Box::new(StreamType::Int)));
        let spec = UntypedDsrvSpecification {
            input_vars: BTreeSet::new(),
            output_vars: BTreeSet::from([var.clone()]),
            stream_vars: BTreeSet::from([var.clone()]),
            exprs: exprs
                .into_iter()
                .map(|(name, expr)| (name, expr.into()))
                .collect(),
            type_annotations,
            aux_vars: BTreeSet::new(),
        };
        let result = type_check(spec);
        assert!(
            result.is_ok(),
            "Expected Ok for spec with y : List<Int> = [], got {:?}",
            result
        );
        let typed_spec = result.unwrap();
        let te = typed_spec.var_expr(&var).unwrap();
        assert_eq!(extract_type(&te), TCType::list(TCType::Int));
    }

    #[test]
    fn test_type_check_preserves_aux_vars() {
        let x: VarName = "strict_aux_x".into();
        let z: VarName = "strict_aux_z".into();
        let u: VarName = "strict_aux_u".into();
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

        let typed = type_check(spec).expect("strict type check should preserve aux vars");

        assert_eq!(typed.aux_vars(), BTreeSet::from([u.clone()]));
        assert_eq!(typed.stream_vars(), BTreeSet::from([z.clone(), u]));
        assert_eq!(typed.output_vars(), BTreeSet::from([z]));
    }

    #[test]
    fn top_level_type_error_uses_document_span() {
        let input = "in x: Int\nout z: Int\nz = x + true";
        let mut source = input;
        let spec = dsrv_specification(&mut source).expect("spec should parse");

        let errors = type_check(spec).expect_err("spec should fail type checking");
        let expected_start = input.find("x + true").expect("expression should exist") as u32;
        let expected_end = expected_start + "x + true".len() as u32;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected top-level operator error at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }

    #[test]
    fn lalr_top_level_type_error_uses_document_span() {
        let input = "in x: Int\nout z: Int\nz = x + true";
        let mut source = input;
        let spec = LALRParser::parse(&mut source).expect("spec should parse");

        let errors = type_check(spec).expect_err("spec should fail type checking");
        let expected_start = input.find("x + true").expect("expression should exist") as u32;
        let expected_end = expected_start + "x + true".len() as u32;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::OperatorTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected LALR top-level operator error at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }

    #[test]
    fn annotation_mismatch_uses_expression_span() {
        let input = "out z: Bool\nz = 1";
        let mut source = input;
        let spec = LALRParser::parse(&mut source).expect("spec should parse");

        let errors = type_check(spec).expect_err("spec should fail type checking");
        let expected_start = input.find('1').expect("expression should exist") as u32;
        let expected_end = expected_start + 1;

        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::TypeError(type_error)
                    if type_error.kind() == &TypeErrorKind::AnnotationTypeMismatch
                        && error.span() == Some(Span::new(expected_start, expected_end))
            )),
            "expected annotation mismatch at {:?}, got {:?}",
            Span::new(expected_start, expected_end),
            errors
        );
    }
}
