use crate::core::{Specification, VarName};
use crate::lang::distribution_constraints::ast::DistConstraint;
use crate::lang::dynamic_lola::ast::{LOLASpecification, SExpr};
use std::{collections::BTreeMap, fmt::Debug};

#[derive(Clone, PartialEq)]
pub struct DistLangSpecification {
    pub lola_spec: LOLASpecification,
    pub dist_constraints: BTreeMap<VarName, Vec<DistConstraint>>,
}

impl Debug for DistLangSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Distribution constraints ordered lexicographically by name
        let dist_constraints_by_name: BTreeMap<String, &Vec<DistConstraint>> = self
            .dist_constraints
            .iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let dist_constraints_formatted = format!(
            "{{{}}}",
            dist_constraints_by_name
                .iter()
                .map(|(k, v)| format!("{:?}: {:?}", VarName::new(k), v))
                .collect::<Vec<String>>()
                .join(", ")
        );

        write!(
            f,
            "DistLangSpecification {{ lola_spec: {:?}, dist_constraints: {} }}",
            self.lola_spec, dist_constraints_formatted
        )
    }
}

impl Specification for DistLangSpecification {
    type Expr = SExpr;

    fn input_vars(&self) -> Vec<VarName> {
        self.lola_spec.input_vars.clone()
    }

    fn output_vars(&self) -> Vec<VarName> {
        self.lola_spec.output_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExpr> {
        Some(self.lola_spec.exprs.get(var)?.clone())
    }
}
