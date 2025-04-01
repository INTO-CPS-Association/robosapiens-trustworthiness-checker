use std::collections::BTreeMap;

use winnow::Parser;
use winnow::Result;
use winnow::combinator::*;

use super::super::core::parser::*;
use super::super::distribution_constraints::parser::dist_constraints;
use super::super::dynamic_lola::parser as dl_parser;
use super::ast::DistLangSpecification;
use crate::LOLASpecification;

pub fn dist_lang_specification(s: &mut &str) -> Result<DistLangSpecification> {
    seq!((
        _: loop_ms_or_lb_or_lc,
        dl_parser::input_decls,
        _: loop_ms_or_lb_or_lc,
        dl_parser::output_decls,
        _: loop_ms_or_lb_or_lc,
        dist_constraints,
        _: loop_ms_or_lb_or_lc,
        dl_parser::var_decls,
        _: loop_ms_or_lb_or_lc,
    ))
    .map(|(input_vars, output_vars, dist_constraints, exprs)| {
        let lola_spec = LOLASpecification {
            input_vars: input_vars.iter().map(|(name, _)| name.clone()).collect(),
            output_vars: output_vars.iter().map(|(name, _)| name.clone()).collect(),
            exprs: exprs.into_iter().collect(),
            type_annotations: input_vars
                .iter()
                .chain(output_vars.iter())
                .cloned()
                .filter_map(|(name, typ)| match typ {
                    Some(typ) => Some((name, typ)),
                    None => None,
                })
                .collect(),
        };
        DistLangSpecification {
            lola_spec,
            dist_constraints: dist_constraints.into_iter().fold(
                BTreeMap::new(),
                |mut acc, (name, constraint)| {
                    acc.entry(name).or_insert_with(Vec::new).push(constraint);
                    acc
                },
            ),
        }
    })
    .parse_next(s)
}

#[cfg(test)]
mod tests {
    use crate::{
        SExpr,
        core::Value,
        lang::distribution_constraints::ast::{
            DistConstraint, DistConstraintBody, DistConstraintType,
        },
    };
    use std::collections::BTreeMap;

    use winnow::error::ContextError;

    use super::*;
    use test_log::test;

    use crate::lang::dynamic_lola::ast::{NumericalBinOp, SBinOp};

    #[test]
    fn test_parse_lola_count() -> Result<(), ContextError> {
        let mut input = "\
            in y\n\
            out x\n\
            x = 1 + (x)[-1, 0]";
        let count_spec = LOLASpecification {
            input_vars: vec!["y".into()],
            output_vars: vec!["x".into()],
            exprs: BTreeMap::from([(
                "x".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(1))),
                    Box::new(SExpr::SIndex(
                        Box::new(SExpr::Var("x".into())),
                        -1,
                        Value::Int(0),
                    )),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::new(),
        };
        let spec_parsed = dist_lang_specification(&mut input)?;
        assert_eq!(spec_parsed.lola_spec, count_spec);
        // assert_eq!(lola_specification(&mut (*input).into())?, count_spec);
        Ok(())
    }

    #[test]
    fn test_parse_lola_count_can_run_constraint() -> Result<(), ContextError> {
        let mut input = "\
            in y\n\
            out x\n\
            can_run x: source(y)\n\
            x = 1 + (x)[-1, 0]";
        let count_spec = LOLASpecification {
            input_vars: vec!["y".into()],
            output_vars: vec!["x".into()],
            exprs: BTreeMap::from([(
                "x".into(),
                SExpr::BinOp(
                    Box::new(SExpr::Val(Value::Int(1))),
                    Box::new(SExpr::SIndex(
                        Box::new(SExpr::Var("x".into())),
                        -1,
                        Value::Int(0),
                    )),
                    SBinOp::NOp(NumericalBinOp::Add),
                ),
            )]),
            type_annotations: BTreeMap::new(),
        };
        let dist_constraints_expected = BTreeMap::from([(
            "x".into(),
            vec![DistConstraint(
                DistConstraintType::CanRun,
                DistConstraintBody::Source("y".into()),
            )],
        )]);
        let spec_parsed = dist_lang_specification(&mut input)?;
        assert_eq!(spec_parsed.lola_spec, count_spec);
        // assert_eq!(lola_specification(&mut (*input).into())?, count_spec);
        assert_eq!(spec_parsed.dist_constraints, dist_constraints_expected);
        Ok(())
    }
}
