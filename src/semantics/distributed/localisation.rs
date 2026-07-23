//! Localisation and auxiliary-expression expansion for distributed DSRV specifications.

use static_assertions::assert_obj_safe;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Debug};

use contiguous_tree::CloneTreeError;
use tracing::debug;

use crate::lang::dsrv::ast::{DependencyKind, DsrvSpecification, ExprView};
use crate::lang::dsrv::span::Span;

use crate::VarName;
use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};

/// A failure while resolving the variables assigned to a locality.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LocalitySpecError {
    #[error("locality node `{node}` does not exist in the distribution graph")]
    UnknownNode { node: NodeName },
}

pub trait LocalitySpec: Debug {
    fn local_vars(&self) -> Result<Vec<VarName>, LocalitySpecError>;
}

assert_obj_safe!(LocalitySpec);

impl LocalitySpec for Vec<VarName> {
    fn local_vars(&self) -> Result<Vec<VarName>, LocalitySpecError> {
        Ok(self.clone())
    }
}
impl<W: Debug> LocalitySpec for (NodeName, &GenericLabelledDistributionGraph<W>) {
    fn local_vars(&self) -> Result<Vec<VarName>, LocalitySpecError> {
        let node_index = self.1.get_node_index_by_name(&self.0).ok_or_else(|| {
            LocalitySpecError::UnknownNode {
                node: self.0.clone(),
            }
        })?;
        Ok(self
            .1
            .monitors_at_node(node_index)
            .cloned()
            .unwrap_or_default())
    }
}
impl<W: Debug> LocalitySpec for (NodeName, GenericLabelledDistributionGraph<W>) {
    fn local_vars(&self) -> Result<Vec<VarName>, LocalitySpecError> {
        (self.0.clone(), &self.1).local_vars()
    }
}

impl LocalitySpec for Box<dyn LocalitySpec> {
    fn local_vars(&self) -> Result<Vec<VarName>, LocalitySpecError> {
        self.as_ref().local_vars()
    }
}

pub trait Localisable {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self;
}

/// A failure while localising a DSRV specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DsrvLocalisationError {
    Locality(LocalitySpecError),
    MissingAuxDefinition { variable: VarName },
    MonitoredAtAux { variable: VarName, node: NodeName },
    Dist,
    CyclicReplacement { replacement_spans: Vec<Span> },
}

impl fmt::Display for DsrvLocalisationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Locality(error) => fmt::Display::fmt(error, formatter),
            Self::MissingAuxDefinition { variable } => {
                write!(
                    formatter,
                    "aux variable `{variable}` does not have a definition"
                )
            }
            Self::MonitoredAtAux { variable, node } => write!(
                formatter,
                "localisation of monitored_at({variable}, {node}) is not allowed because `{variable}` is an aux variable"
            ),
            Self::Dist => formatter.write_str("dist(...) is unsupported during DSRV localisation"),
            Self::CyclicReplacement { replacement_spans } => write!(
                formatter,
                "cyclic aux replacement expansion through spans {replacement_spans:?}"
            ),
        }
    }
}

impl std::error::Error for DsrvLocalisationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Locality(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LocalitySpecError> for DsrvLocalisationError {
    fn from(error: LocalitySpecError) -> Self {
        Self::Locality(error)
    }
}

fn dependency_closure(
    spec: &DsrvSpecification,
    roots: impl IntoIterator<Item = VarName>,
) -> BTreeSet<VarName> {
    let mut reachable = BTreeSet::new();
    let mut pending = roots.into_iter().collect::<Vec<_>>();
    while let Some(var) = pending.pop() {
        if !reachable.insert(var.clone()) {
            continue;
        }
        if let Some(root) = spec.exprs.get(&var) {
            root.visit_dependencies(|kind, dependency| {
                // Placement references are not runtime stream dependencies, but aux variables used
                // in monitored_at must remain visible so localisation can reject them structurally.
                if matches!(kind, DependencyKind::Stream) || spec.aux_vars.contains(dependency) {
                    pending.push(dependency.clone());
                }
            });
        }
    }
    reachable
}

fn prune_to_dependency_closure(
    mut spec: DsrvSpecification,
    roots: &[VarName],
) -> DsrvSpecification {
    let reachable = dependency_closure(&spec, roots.iter().cloned());
    let root_set = roots.iter().cloned().collect::<BTreeSet<_>>();
    spec.output_vars.retain(|var| root_set.contains(var));
    spec.aux_vars.retain(|var| reachable.contains(var));
    spec.exprs.retain(|var| reachable.contains(var));
    spec.type_annotations
        .retain(|var, _| reachable.contains(var));
    spec
}

fn try_inline_aux(spec: DsrvSpecification) -> Result<DsrvSpecification, DsrvLocalisationError> {
    let aux_vars = spec.aux_vars.clone();
    for aux in &aux_vars {
        if !spec.exprs.contains_key(aux) {
            return Err(DsrvLocalisationError::MissingAuxDefinition {
                variable: aux.clone(),
            });
        }
    }

    let exprs = spec
        .exprs
        .try_rewrite_selected_with(
            |var| spec.output_vars.contains(var) && !aux_vars.contains(var),
            |expression| match expression.view() {
                ExprView::Var(var) if aux_vars.contains(var) => Ok(spec.var_expr_ref(var)),
                ExprView::MonitoredAt(var, node) if aux_vars.contains(var) => {
                    Err(DsrvLocalisationError::MonitoredAtAux {
                        variable: var.clone(),
                        node: node.clone(),
                    })
                }
                ExprView::Dist(_, _) => Err(DsrvLocalisationError::Dist),
                _ => Ok(None),
            },
        )
        .map_err(|error| match error {
            CloneTreeError::Policy(error) => error,
            CloneTreeError::ReplacementCycle { cursors } => {
                DsrvLocalisationError::CyclicReplacement {
                    replacement_spans: cursors.into_iter().map(|cursor| cursor.span()).collect(),
                }
            }
        })?;

    let output_vars = spec.output_vars.difference(&aux_vars).cloned().collect();
    let type_annotations = spec
        .type_annotations
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();

    Ok(DsrvSpecification::from_expression_forest(
        spec.input_vars,
        output_vars,
        exprs,
        type_annotations,
        std::iter::empty(),
    ))
}

#[cfg(test)]
fn inline_aux(spec: DsrvSpecification) -> DsrvSpecification {
    try_inline_aux(spec).unwrap_or_else(|error| panic!("Failed to inline aux variables: {error}"))
}

fn finish_localisation(
    mut spec: DsrvSpecification,
    original: &DsrvSpecification,
    local_set: &BTreeSet<VarName>,
) -> DsrvSpecification {
    debug_assert!(spec.exprs.keys().all(|var| local_set.contains(var)));
    let needed_inputs = spec
        .exprs
        .values()
        .flat_map(|expression| expression.stream_dependencies())
        .collect::<HashSet<_>>();
    spec.input_vars = original
        .input_vars
        .iter()
        .chain(
            original
                .output_vars
                .iter()
                .chain(original.aux_vars.iter())
                .filter(|var| !local_set.contains(*var)),
        )
        .filter(|var| needed_inputs.contains(*var))
        .cloned()
        .collect();
    spec.output_vars.retain(|var| local_set.contains(var));
    spec.aux_vars.retain(|var| local_set.contains(var));
    spec.stream_vars = spec
        .output_vars
        .iter()
        .cloned()
        .chain(spec.aux_vars.iter().cloned())
        .collect();

    debug!("Local expression inputs: {:?}", needed_inputs);
    spec
}

impl DsrvSpecification {
    /// Localise this specification without panicking on unsupported aux expansion.
    pub fn try_localise(
        &self,
        locality_spec: &impl LocalitySpec,
    ) -> Result<Self, DsrvLocalisationError> {
        let local_vars = locality_spec.local_vars()?;
        let spec = try_inline_aux(prune_to_dependency_closure(self.clone(), &local_vars))?;
        let local_set = local_vars.into_iter().collect::<BTreeSet<_>>();
        Ok(finish_localisation(spec, self, &local_set))
    }
}

impl Localisable for DsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        self.try_localise(locality_spec)
            .unwrap_or_else(|error| panic!("Failed to localise DSRV specification: {error}"))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;
    use std::vec;

    use contiguous_tree::TreeCursorExt;
    use petgraph::graph::DiGraph;

    use crate::core::BinaryOperator;
    use crate::distributed::distribution_graphs::GenericDistributionGraph;
    use crate::dsrv_fixtures::spec_simple_add_decomposable;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::span::strip_span_ref;
    use proptest::prelude::*;
    use test_log::test;

    use super::*;
    use crate::lang::dsrv::test_support::arb_boolean_dsrv_spec;

    fn locality_graph() -> GenericLabelledDistributionGraph<u64> {
        let mut graph: DiGraph<NodeName, u64> = DiGraph::new();
        let node = graph.add_node("A".into());
        GenericLabelledDistributionGraph {
            dist_graph: Rc::new(GenericDistributionGraph {
                central_monitor: node,
                graph,
            }),
            var_names: Vec::new(),
            node_labels: BTreeMap::new(),
        }
    }

    #[test]
    fn graph_locality_treats_missing_labels_as_empty_and_reports_unknown_nodes() {
        let graph = locality_graph();
        assert_eq!(
            (NodeName::from("A"), &graph).local_vars().unwrap(),
            Vec::<VarName>::new()
        );

        let unknown = NodeName::from("missing");
        assert_eq!(
            (unknown.clone(), &graph).local_vars(),
            Err(LocalitySpecError::UnknownNode {
                node: unknown.clone()
            })
        );

        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            Vec::new(),
        );
        assert_eq!(
            spec.try_localise(&(unknown.clone(), &graph)),
            Err(DsrvLocalisationError::Locality(
                LocalitySpecError::UnknownNode { node: unknown }
            ))
        );
    }

    fn assert_specs_eq_ignoring_spans(actual: &DsrvSpecification, expected: &DsrvSpecification) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        assert_eq!(actual.type_annotations, expected.type_annotations);

        let actual_exprs = actual
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        let expected_exprs = expected
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_exprs, expected_exprs);
    }

    #[test]
    fn test_localise_specification_1() {
        let spec = DsrvSpecification::new(
            BTreeSet::from(["a".into(), "b".into()]),
            BTreeSet::from(["c".into(), "d".into(), "e".into()]),
            vec![
                ("c".into(), Expr::Var("a".into())),
                ("d".into(), Expr::Not(Box::new(Expr::Var("a".into())))),
                ("e".into(), Expr::Not(Box::new(Expr::Var("d".into())))),
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
                BTreeSet::from(["a".into(), "d".into()]),
                BTreeSet::from(["c".into(), "e".into()]),
                vec![
                    ("c".into(), Expr::Var("a".into())),
                    ("e".into(), Expr::Not(Box::new(Expr::Var("d".into())))),
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
            BTreeSet::from(["a".into()]),
            BTreeSet::from(["i".into()]),
            BTreeMap::<VarName, Expr>::new(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec![];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            DsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, Expr>::new(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = spec_simple_add_decomposable()
            .parse::<DsrvSpecification>()
            .unwrap();

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &DsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![(
                    "w".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("x".into())),
                        Box::new(Expr::Var("y".into())),
                        BinaryOperator::Add,
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &DsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![(
                    "v".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("z".into())),
                        Box::new(Expr::Var("w".into())),
                        BinaryOperator::Add,
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_localise_spec_with_aux() {
        // Tests that localisation correctly handles auxiliary variables
        // Note that these must be specified similarly to output variables
        let spec = "   in x
                    in y
                    in z
                    out w
                    out v
                    aux tmp
                    w = x + y
                    tmp = z + w
                    v = tmp"
            .parse::<DsrvSpecification>()
            .unwrap();

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &DsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![(
                    "w".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("x".into())),
                        Box::new(Expr::Var("y".into())),
                        BinaryOperator::Add,
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &DsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![(
                    "v".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("z".into())),
                        Box::new(Expr::Var("w".into())),
                        BinaryOperator::Add,
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_inline_aux_single_aux() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();
        let tmp: VarName = "tmp".into();
        let z: VarName = "z".into();

        let spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from([tmp.clone(), z.clone()]),
            vec![
                (
                    tmp.clone(),
                    Expr::BinOp(
                        Box::new(Expr::Var(x.clone())),
                        Box::new(Expr::Var(y.clone())),
                        BinaryOperator::Add,
                    ),
                ),
                (
                    z.clone(),
                    Expr::BinOp(
                        Box::new(Expr::Var(tmp.clone())),
                        Box::new(Expr::Var(x.clone())),
                        BinaryOperator::Multiply,
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
            Expr::BinOp(
                Box::new(Expr::BinOp(
                    Box::new(Expr::Var(x.clone())),
                    Box::new(Expr::Var(y.clone())),
                    BinaryOperator::Add,
                )),
                Box::new(Expr::Var(x.clone())),
                BinaryOperator::Multiply,
            ),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            DsrvSpecification::new(
                BTreeSet::from([x, y]),
                BTreeSet::from([z]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    fn test_inline_aux_transitive_chain() {
        let i: VarName = "i".into();
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let h3: VarName = "h3".into();
        let out: VarName = "out".into();

        let spec = DsrvSpecification::new(
            BTreeSet::from([i.clone()]),
            BTreeSet::from([h1.clone(), h2.clone(), h3.clone(), out.clone()]),
            vec![
                (h1.clone(), Expr::Var(i.clone())),
                (
                    h2.clone(),
                    Expr::BinOp(
                        Box::new(Expr::Var(h1.clone())),
                        Box::new(Expr::Val(1)),
                        BinaryOperator::Add,
                    ),
                ),
                (
                    h3.clone(),
                    Expr::BinOp(
                        Box::new(Expr::Var(h2.clone())),
                        Box::new(Expr::Val(2)),
                        BinaryOperator::Add,
                    ),
                ),
                (out.clone(), Expr::Var(h3.clone())),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1.clone(), h2.clone(), h3.clone()],
        );

        let result = inline_aux(spec);

        // Auxiliary definitions are expanded transitively into the output expression.
        let expected_exprs = vec![(
            out.clone(),
            Expr::BinOp(
                Box::new(Expr::BinOp(
                    Box::new(Expr::Var(i.clone())),
                    Box::new(Expr::Val(1)),
                    BinaryOperator::Add,
                )),
                Box::new(Expr::Val(2)),
                BinaryOperator::Add,
            ),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            DsrvSpecification::new(
                BTreeSet::from([i]),
                BTreeSet::from([out]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    #[should_panic(expected = "cyclic aux replacement expansion")]
    fn test_inline_aux_cycle_panics() {
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let out: VarName = "out".into();

        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([h1.clone(), h2.clone(), out.clone()]),
            vec![
                (h1.clone(), Expr::Var(h2.clone())),
                (h2.clone(), Expr::Var(h1.clone())),
                (out.clone(), Expr::Var(h1.clone())),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1, h2],
        );

        let _ = inline_aux(spec);
    }

    #[test]
    fn try_localise_reports_missing_aux_definition() {
        let missing: VarName = "missing".into();
        let output: VarName = "output".into();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([missing.clone(), output.clone()]),
            BTreeMap::from([(output.clone(), Expr::Var(missing.clone()))]),
            BTreeMap::new(),
            [missing.clone()],
        );

        assert_eq!(
            spec.try_localise(&vec![output]).unwrap_err(),
            DsrvLocalisationError::MissingAuxDefinition { variable: missing }
        );
    }

    #[test]
    fn try_localise_reports_monitored_at_aux() {
        let helper: VarName = "helper".into();
        let output: VarName = "output".into();
        let spec = "aux helper\nout output\nhelper = true\noutput = monitored_at(helper, A)"
            .parse::<DsrvSpecification>()
            .unwrap();

        assert!(matches!(
            spec.try_localise(&vec![output]),
            Err(DsrvLocalisationError::MonitoredAtAux { variable, .. }) if variable == helper
        ));
    }

    #[test]
    fn try_localise_reports_dist() {
        let output: VarName = "output".into();
        let spec = "out output\noutput = dist(A, B)"
            .parse::<DsrvSpecification>()
            .unwrap();

        assert_eq!(
            spec.try_localise(&vec![output]).unwrap_err(),
            DsrvLocalisationError::Dist
        );
    }

    #[test]
    fn try_localise_reports_cyclic_aux_replacement() {
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let output: VarName = "output".into();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([first.clone(), second.clone(), output.clone()]),
            BTreeMap::from([
                (first.clone(), Expr::Var(second.clone())),
                (second.clone(), Expr::Var(first.clone())),
                (output.clone(), Expr::Var(first.clone())),
            ]),
            BTreeMap::new(),
            [first, second],
        );

        assert!(matches!(
            spec.try_localise(&vec![output]),
            Err(DsrvLocalisationError::CyclicReplacement { .. })
        ));
    }

    #[test]
    fn localisation_finalisation_preserves_compact_rewrite_storage() {
        let helper: VarName = "helper".into();
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let spec = DsrvSpecification::new(
            BTreeSet::from(["input".into()]),
            BTreeSet::from([helper.clone(), first.clone(), second.clone()]),
            BTreeMap::from([
                (
                    helper.clone(),
                    Expr::Not(Box::new(Expr::Var("input".into()))),
                ),
                (first.clone(), Expr::Var(helper.clone())),
                (
                    second.clone(),
                    Expr::BinOp(
                        Box::new(Expr::Var(helper.clone())),
                        Box::new(Expr::Val(true)),
                        BinaryOperator::And,
                    ),
                ),
            ]),
            BTreeMap::new(),
            [helper],
        );

        let local_set = BTreeSet::from([first, second]);
        let pruned = prune_to_dependency_closure(
            spec.clone(),
            &local_set.iter().cloned().collect::<Vec<_>>(),
        );
        let rewritten = try_inline_aux(pruned).unwrap();
        let rewritten_name = rewritten
            .roots()
            .next()
            .expect("local roots should exist")
            .0
            .clone();
        let rewritten_root = rewritten.var_expr(&rewritten_name).unwrap();

        let localised = finish_localisation(rewritten, &spec, &local_set);
        let final_root = localised.var_expr(&rewritten_name).unwrap();
        assert!(
            final_root.shares_storage_with(&rewritten_root),
            "finalisation must not rebuild syntax"
        );

        let reachable_nodes = localised
            .roots()
            .map(|(_, root)| root.postorder().len())
            .sum::<usize>();
        assert_eq!(localised.nodes().count(), reachable_nodes);
        let roots = localised.roots().map(|(_, root)| root).collect::<Vec<_>>();
        assert!(
            roots
                .windows(2)
                .all(|roots| roots[0].shares_storage_with(roots[1]))
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
