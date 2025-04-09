use std::collections::HashSet;

use tracing::info;

use crate::lang::dynamic_lola::ast::LOLASpecification;

use crate::VarName;
use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};

pub trait LocalitySpec {
    fn local_vars(&self) -> Vec<VarName>;
}

impl LocalitySpec for Vec<VarName> {
    fn local_vars(&self) -> Vec<VarName> {
        self.clone()
    }
}
impl<W> LocalitySpec for (NodeName, &GenericLabelledDistributionGraph<W>) {
    /// Returns the local variables of the node.
    /// Panics if the node does not exist in the graph.
    fn local_vars(&self) -> Vec<VarName> {
        let node_index = self.1.get_node_index_by_name(&self.0).unwrap();
        self.1
            .monitors_at_node(node_index)
            .expect(format!("Node index {:?} does not exist in the graph", node_index).as_str())
            .clone()
    }
}
impl<W> LocalitySpec for (NodeName, GenericLabelledDistributionGraph<W>) {
    fn local_vars(&self) -> Vec<VarName> {
        (self.0.clone(), &self.1).local_vars()
    }
}
impl LocalitySpec for Box<dyn LocalitySpec> {
    fn local_vars(&self) -> Vec<VarName> {
        self.as_ref().local_vars()
    }
}

pub trait Localisable {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self;
}

impl Localisable for LOLASpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        let local_vars = locality_spec.local_vars();
        let mut exprs = self.exprs.clone();
        let mut output_vars = self.output_vars.clone();
        let input_vars = self.input_vars.clone();

        let mut to_remove = vec![];
        for v in output_vars.iter() {
            if !local_vars.contains(v) {
                to_remove.push(v.clone());
            }
        }
        output_vars.retain(|v| local_vars.contains(v));
        exprs.retain(|v, _| local_vars.contains(v));
        let expr_input_vars: HashSet<_> = exprs.iter().map(|(_, e)| e.inputs()).flatten().collect();
        info!("Expr input vars: {:?}", expr_input_vars);
        // We keep the order from the original input vars,
        // but remove variable that are not needed locally
        let new_input_vars: Vec<_> = input_vars
            .iter()
            .cloned()
            .chain(to_remove.into_iter())
            .filter(|v| expr_input_vars.contains(v))
            .collect();
        info!("Old input vars: {:?}", input_vars);
        info!("New input vars: {:?}", new_input_vars);

        LOLASpecification::new(
            new_input_vars,
            output_vars,
            exprs,
            self.type_annotations.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use crate::lang::dynamic_lola::ast::SExpr;
    use crate::lola_fixtures::spec_simple_add_decomposable;
    use crate::lola_specification;
    use proptest::prelude::*;
    use test_log::test;
    use winnow::Parser;

    use crate::lang::dynamic_lola::ast::generation::arb_boolean_lola_spec;

    use super::*;

    #[test]
    fn test_localise_specification_1() {
        let spec = LOLASpecification::new(
            vec!["a".into(), "b".into()],
            vec!["c".into(), "d".into(), "e".into()],
            vec![
                ("c".into(), SExpr::Var("a".into())),
                ("d".into(), SExpr::Not(Box::new(SExpr::Var("a".into())))),
                ("e".into(), SExpr::Not(Box::new(SExpr::Var("d".into())))),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
        );
        let restricted_vars = vec!["c".into(), "e".into()];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            LOLASpecification::new(
                vec!["a".into(), "d".into()],
                vec!["c".into(), "e".into()],
                vec![
                    ("c".into(), SExpr::Var("a".into())),
                    ("e".into(), SExpr::Not(Box::new(SExpr::Var("d".into())))),
                ]
                .into_iter()
                .collect(),
                BTreeMap::new(),
            )
        )
    }

    #[test]
    fn test_localise_specification_2() {
        let spec = LOLASpecification::new(
            vec!["a".into()],
            vec!["i".into()],
            vec![].into_iter().collect(),
            BTreeMap::new(),
        );
        let restricted_vars = vec![];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            LOLASpecification::new(
                vec![],
                vec![],
                vec![].into_iter().collect(),
                BTreeMap::new(),
            )
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = lola_specification
            .parse(spec_simple_add_decomposable())
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_eq!(
            local_spec1,
            LOLASpecification::new(
                vec!["x".into(), "y".into()],
                vec!["w".into()],
                vec![(
                    "w".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("x".into())),
                        Box::new(SExpr::Var("y".into())),
                        "+".into()
                    )
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
            )
        );

        assert_eq!(
            local_spec2,
            LOLASpecification::new(
                vec!["z".into(), "w".into()],
                vec!["v".into()],
                vec![(
                    "v".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("z".into())),
                        Box::new(SExpr::Var("w".into())),
                        "+".into()
                    )
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
            )
        );
    }

    proptest! {
        #[test]
        fn test_localise_specification_prop(
            spec in arb_boolean_lola_spec(),
            restricted_vars in prop::collection::hash_set("[a-z]", 0..5)
        ) {
            let restricted_vars: Vec<VarName> = restricted_vars.into_iter().map(|s| s.into()).collect();
            let localised_spec = spec.localise(&restricted_vars);

            for var in localised_spec.output_vars.iter() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(spec.exprs.contains_key(var));
            }
            for var in localised_spec.input_vars.iter() {
                assert!(spec.input_vars.contains(var)
                    || spec.output_vars.contains(var));
            }
        }
    }
}
