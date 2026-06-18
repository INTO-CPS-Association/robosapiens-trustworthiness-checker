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
                errors.push(SemanticError::MissingTypeAnnotation(format!(
                    "Variable {:?} is missing a type annotation",
                    var
                )));
                continue;
            }
        };
        let typed_expr = expr.type_check_raw(Some(&expected), &mut ctx, &mut errors);
        // Check consistency of inferred type with the declared type annotation
        if let Ok(ref te) = typed_expr {
            let actual = extract_type(te);
            if actual != TCType::from_stream_type(&expected) {
                errors.push(SemanticError::TypeError(format!(
                    "Variable {:?} has declared type {:?}, but expression has type {:?}",
                    var, expected, actual
                )));
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
    use crate::lang::dsrv::ast::SExpr;
    use crate::{DsrvSpecification, VarName};
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
            exprs,
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
}
