use static_assertions::assert_obj_safe;
use std::collections::HashSet;
use std::fmt::Debug;

use tracing::debug;

use crate::lang::dsrv::ast::DsrvSpecification;

use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};
use crate::{SExpr, Specification, VarName};

pub trait LocalitySpec: Debug {
    fn local_vars(&self) -> Vec<VarName>;
}

assert_obj_safe!(LocalitySpec);

impl LocalitySpec for Vec<VarName> {
    fn local_vars(&self) -> Vec<VarName> {
        self.clone()
    }
}
impl<W: Debug> LocalitySpec for (NodeName, &GenericLabelledDistributionGraph<W>) {
    /// Returns the local variables of the node.
    /// Panics if the node does not exist in the graph.
    fn local_vars(&self) -> Vec<VarName> {
        let node_index = self.1.get_node_index_by_name(&self.0).unwrap();
        self.1
            .monitors_at_node(node_index)
            .unwrap_or_else(|| panic!("Node index {:?} does not exist in the graph", node_index))
            .clone()
    }
}
impl<W: Debug + Clone> LocalitySpec for (NodeName, GenericLabelledDistributionGraph<W>) {
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

fn replace_var(var: &VarName, var_expr: &SExpr, repl_expr: &SExpr) -> SExpr {
    // Replaces all occurrences of var in the repl_expr with var_expr
    match repl_expr {
        SExpr::Var(v) if v == var => var_expr.clone(),
        SExpr::Var(_) => repl_expr.clone(),
        SExpr::BinOp(lhs, rhs, op) => SExpr::BinOp(
            Box::new(replace_var(var, var_expr, lhs)),
            Box::new(replace_var(var, var_expr, rhs)),
            op.clone(),
        ),
        SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
            Box::new(replace_var(var, var_expr, sexpr2)),
        ),
        SExpr::SIndex(sexpr, idx) => {
            SExpr::SIndex(Box::new(replace_var(var, var_expr, sexpr)), *idx)
        }
        SExpr::Val(value) => SExpr::Val(value.clone()),
        SExpr::Dynamic(sexpr, stream_type_ascription) => SExpr::Dynamic(
            Box::new(replace_var(var, var_expr, sexpr)),
            stream_type_ascription.clone(),
        ),
        SExpr::RestrictedDynamic(sexpr, stream_type_ascription, eco_vec) => {
            SExpr::RestrictedDynamic(
                Box::new(replace_var(var, var_expr, sexpr)),
                stream_type_ascription.clone(),
                eco_vec.clone(),
            )
        }
        SExpr::Defer(sexpr, stream_type_ascription, eco_vec) => SExpr::Defer(
            Box::new(replace_var(var, var_expr, sexpr)),
            stream_type_ascription.clone(),
            eco_vec.clone(),
        ),
        SExpr::Update(sexpr, sexpr1) => SExpr::Update(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Default(sexpr, sexpr1) => SExpr::Default(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::When(sexpr) => SExpr::When(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Latch(sexpr, sexpr1) => SExpr::Latch(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Init(sexpr, sexpr1) => SExpr::Init(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Not(sexpr) => SExpr::Not(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::List(eco_vec) => {
            let vec = eco_vec
                .iter()
                .map(|e| replace_var(var, var_expr, e))
                .collect();
            SExpr::List(vec)
        }
        SExpr::LIndex(sexpr, sexpr1) => SExpr::LIndex(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LAppend(sexpr, sexpr1) => SExpr::LAppend(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LConcat(sexpr, sexpr1) => SExpr::LConcat(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LHead(sexpr) => SExpr::LHead(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::LTail(sexpr) => SExpr::LTail(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::LLen(sexpr) => SExpr::LLen(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Map(btree_map) => SExpr::Map(
            btree_map
                .iter()
                .map(|(k, v)| (k.clone(), replace_var(var, var_expr, v)))
                .collect(),
        ),
        SExpr::MGet(sexpr, eco_string) => SExpr::MGet(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::MInsert(sexpr, eco_string, sexpr1) => SExpr::MInsert(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::MRemove(sexpr, eco_string) => SExpr::MRemove(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::MHasKey(sexpr, eco_string) => SExpr::MHasKey(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::Sin(sexpr) => SExpr::Sin(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Cos(sexpr) => SExpr::Cos(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Tan(sexpr) => SExpr::Tan(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Abs(sexpr) => SExpr::Abs(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::MonitoredAt(var_name, node_name) => {
            if var_name == var {
                panic!("Localisation of monitored_at expression with aux variable not allowed")
            } else {
                SExpr::MonitoredAt(var_name.clone(), node_name.clone())
            }
        }
        SExpr::Dist(_, _) => {
            unimplemented!("Dist currently unsupported")
        }
    }
}

fn inline_aux(spec: DsrvSpecification) -> DsrvSpecification {
    // Inlines auxiliary variables by replacing them with their definitions in the expressions
    let mut new_spec = spec.clone();
    let aux_vars = spec.aux_vars();
    for aux in aux_vars.iter() {
        // TODO: Does not handle recursion - we should check if aux_expr contains aux
        let aux_expr = spec.exprs.get(aux).expect(
            format!(
                "Aux variable {:?} does not have a definition in the expressions",
                aux
            )
            .as_str(),
        );
        for (_, repl_expr) in new_spec.exprs.iter_mut() {
            let new_expr = replace_var(aux, aux_expr, &*repl_expr);
            *repl_expr = new_expr;
        }
    }
    for aux in aux_vars.iter() {
        new_spec.exprs.remove(aux);
        new_spec.output_vars.retain(|v| v != aux);
        new_spec.type_annotations.remove(aux);
    }
    new_spec.aux_info = vec![];
    new_spec
}

impl Localisable for DsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        let spec = inline_aux(self.clone());
        let local_vars = locality_spec.local_vars();
        let mut exprs = spec.exprs.clone();
        let mut output_vars = spec.output_vars.clone();
        let mut aux_info = spec.aux_info.clone();
        let input_vars = spec.input_vars.clone();

        let mut to_remove = vec![];
        for v in output_vars.iter() {
            if !local_vars.contains(v) {
                to_remove.push(v.clone());
            }
        }
        output_vars.retain(|v| local_vars.contains(v));
        aux_info.retain(|v| local_vars.contains(v));
        exprs.retain(|v, _| local_vars.contains(v));
        let expr_input_vars: HashSet<_> = exprs.values().flat_map(|e| e.inputs()).collect();
        debug!("Expr input vars: {:?}", expr_input_vars);
        // We keep the order from the original input vars,
        // but remove variable that are not needed locally
        let new_input_vars: Vec<_> = input_vars
            .iter()
            .cloned()
            .chain(to_remove)
            .filter(|v| expr_input_vars.contains(v))
            .collect();
        debug!("Old input vars: {:?}", input_vars);
        debug!("New input vars: {:?}", new_input_vars);

        DsrvSpecification::new(
            new_input_vars,
            output_vars,
            exprs,
            spec.type_annotations.clone(),
            aux_info,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use crate::dsrv_fixtures::spec_simple_add_decomposable;
    use crate::dsrv_specification;
    use crate::lang::dsrv::ast::SExpr;
    use proptest::prelude::*;
    use test_log::test;
    use winnow::Parser;

    use crate::lang::dsrv::ast::generation::arb_boolean_dsrv_spec;

    use super::*;

    #[test]
    fn test_localise_specification_1() {
        let spec = DsrvSpecification::new(
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
            vec![],
        );
        let restricted_vars = vec!["c".into(), "e".into()];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            DsrvSpecification::new(
                vec!["a".into(), "d".into()],
                vec!["c".into(), "e".into()],
                vec![
                    ("c".into(), SExpr::Var("a".into())),
                    ("e".into(), SExpr::Not(Box::new(SExpr::Var("d".into())))),
                ]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_2() {
        let spec = DsrvSpecification::new(
            vec!["a".into()],
            vec!["i".into()],
            vec![].into_iter().collect(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec![];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            DsrvSpecification::new(
                vec![],
                vec![],
                vec![].into_iter().collect(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = dsrv_specification
            .parse(spec_simple_add_decomposable())
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_eq!(
            local_spec1,
            DsrvSpecification::new(
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
                vec![],
            )
        );

        assert_eq!(
            local_spec2,
            DsrvSpecification::new(
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
                vec![],
            )
        );
    }

    #[test]
    fn test_localise_spec_with_aux() {
        // Tests that localisation correctly handles auxiliary variables
        // Note that these must be specified similarly to output variables
        let spec = dsrv_specification
            .parse(
                "   in x
                    in y
                    in z
                    out w
                    out v
                    aux tmp
                    w = x + y
                    tmp = z + w
                    v = tmp",
            )
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_eq!(
            local_spec1,
            DsrvSpecification::new(
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
                vec![],
            )
        );

        assert_eq!(
            local_spec2,
            DsrvSpecification::new(
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
                vec![],
            )
        );
    }

    #[test]
    fn test_replace_var_simple() {
        let x: VarName = "x".into();
        let expr = SExpr::Var(x.clone());
        let replacement = SExpr::Val(42.into());

        let result = replace_var(&x, &replacement, &expr);

        assert_eq!(result, replacement);
    }

    #[test]
    fn test_replace_var_nested() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();

        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone())),
            Box::new(SExpr::BinOp(
                Box::new(SExpr::Var(y.clone())),
                Box::new(SExpr::Var(x.clone())),
                "+".into(),
            )),
            "*".into(),
        );

        let replacement = SExpr::Val(1.into());

        let result = replace_var(&x, &replacement, &expr);

        let expected = SExpr::BinOp(
            Box::new(SExpr::Val(1.into())),
            Box::new(SExpr::BinOp(
                Box::new(SExpr::Var(y)),
                Box::new(SExpr::Val(1.into())),
                "+".into(),
            )),
            "*".into(),
        );

        assert_eq!(result, expected);
    }

    #[test]
    fn test_inline_aux_single_aux() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();
        let tmp: VarName = "tmp".into();
        let z: VarName = "z".into();

        let spec = DsrvSpecification::new(
            vec![x.clone(), y.clone()],
            vec![tmp.clone(), z.clone()],
            vec![
                (
                    tmp.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(x.clone())),
                        Box::new(SExpr::Var(y.clone())),
                        "+".into(),
                    ),
                ),
                (
                    z.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(tmp.clone())),
                        Box::new(SExpr::Var(x.clone())),
                        "*".into(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![tmp.clone()],
        );

        let result = inline_aux(spec);

        let expected_exprs = vec![(
            z.clone(),
            SExpr::BinOp(
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var(x.clone())),
                    Box::new(SExpr::Var(y.clone())),
                    "+".into(),
                )),
                Box::new(SExpr::Var(x.clone())),
                "*".into(),
            ),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            DsrvSpecification::new(vec![x, y], vec![z], expected_exprs, BTreeMap::new(), vec![])
        );
    }

    proptest! {
        #[test]
        fn test_localise_specification_prop(
            spec in arb_boolean_dsrv_spec(),
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
