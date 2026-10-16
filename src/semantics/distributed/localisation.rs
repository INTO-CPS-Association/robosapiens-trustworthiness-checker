//! Localisation of distributed DSRV specifications to the streams one node monitors.

use static_assertions::assert_obj_safe;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Debug};

use contiguous_tree::TreeCursorExt;
use tracing::debug;

use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, Declaration, DependencyKind, DsrvSpecification, ExprBuilder,
    ExprForestMap, ExprId, ExprView,
};
use crate::lang::dsrv::type_checker::TCType;

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

pub trait TryLocalisable: Localisable {
    type Error: std::error::Error + Send + Sync + 'static;

    fn try_localise(&self, locality_spec: &impl LocalitySpec) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// A failure while localising a DSRV specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DsrvLocalisationError {
    Locality(LocalitySpecError),
    MissingAuxDefinition { variable: VarName },
    MonitoredAtAux { variable: VarName, node: NodeName },
    Dist,
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

/// The streams a node computes itself: its local outputs and every auxiliary
/// stream they read, directly or through other auxiliary streams. Whatever
/// else those equations read is computed elsewhere and arrives as an input.
struct LocalStreams {
    outputs: BTreeSet<VarName>,
    aux: BTreeSet<VarName>,
}

impl LocalStreams {
    fn collect(
        elaborated: &CheckedDsrvSpecification,
        local_vars: &[VarName],
    ) -> Result<Self, DsrvLocalisationError> {
        let spec = elaborated.unchecked();
        let outputs = local_vars
            .iter()
            .filter(|var| spec.output_vars.contains(*var) && !spec.aux_vars.contains(*var))
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut aux = BTreeSet::new();
        let mut pending = outputs.iter().cloned().collect::<Vec<_>>();
        while let Some(var) = pending.pop() {
            let Some(root) = elaborated.var_expr_ref(&var) else {
                if spec.aux_vars.contains(&var) {
                    return Err(DsrvLocalisationError::MissingAuxDefinition { variable: var });
                }
                continue;
            };
            for node in root.postorder() {
                match node.expr().view() {
                    // An auxiliary stream is computed on every node that
                    // reads it, so it has no placement to ask about.
                    ExprView::MonitoredAt(variable, monitor)
                        if spec.aux_vars.contains(variable) =>
                    {
                        return Err(DsrvLocalisationError::MonitoredAtAux {
                            variable: variable.clone(),
                            node: monitor.clone(),
                        });
                    }
                    ExprView::Dist(_, _) => return Err(DsrvLocalisationError::Dist),
                    _ => {}
                }
            }
            root.expr().visit_dependencies(|kind, dependency| {
                if matches!(kind, DependencyKind::Stream)
                    && spec.aux_vars.contains(dependency)
                    && aux.insert(dependency.clone())
                {
                    pending.push(dependency.clone());
                }
            });
        }
        Ok(Self { outputs, aux })
    }

    fn contains(&self, var: &VarName) -> bool {
        self.outputs.contains(var) || self.aux.contains(var)
    }
}

/// Copy the equations of `local` into fresh storage, so the localised
/// specification holds no node it does not run, and record each node's type
/// in allocation order.
fn local_equations(
    elaborated: &CheckedDsrvSpecification,
    local: &LocalStreams,
) -> (ExprForestMap<VarName>, Vec<TCType>) {
    let names = elaborated
        .unchecked()
        .exprs
        .keys()
        .filter(|name| local.contains(name))
        .cloned()
        .collect::<Vec<_>>();
    let mut builder = ExprBuilder::with_capacities(0, names.len());
    let mut types = Vec::new();
    let roots = builder
        .try_rewrite_forest(
            names.iter().map(|name| {
                elaborated
                    .var_expr_ref(name)
                    .expect("every retained equation is elaborated")
            }),
            |mut node| {
                let source = node.source();
                let kind = node.source_node().rebuild(source.kind().clone());
                let id = node.alloc(kind, source.expr().metadata().clone())?;
                types.push(source.typ().clone());
                Ok::<_, contiguous_tree::BuildError<ExprId>>(id)
            },
        )
        .expect("the identity rewrite keeps every node's children");
    let forest = builder
        .finish_forest(roots)
        .expect("localisation emits one complete tree per equation");
    let exprs = ExprForestMap::new(names, forest)
        .expect("localisation keeps the sorted, unique equation names");
    (exprs, types)
}

/// The declarations of the localised specification, in the original order.
/// Each stream it reads from elsewhere becomes an input, declared where that
/// stream was.
fn local_declarations(
    original: &DsrvSpecification,
    local: &LocalStreams,
    input_vars: &BTreeSet<VarName>,
    exprs: &ExprForestMap<VarName>,
) -> Vec<Declaration> {
    let mut declared_inputs = BTreeSet::new();
    original
        .declarations()
        .iter()
        .filter_map(|entry| match entry {
            Declaration::Input {
                name,
                annotation,
                span,
            }
            | Declaration::Output {
                name,
                annotation,
                span,
            }
            | Declaration::Aux {
                name,
                annotation,
                span,
            } if input_vars.contains(name) && declared_inputs.insert(name.clone()) => {
                Some(Declaration::Input {
                    name: name.clone(),
                    annotation: annotation.clone(),
                    span: *span,
                })
            }
            Declaration::Output { name, .. } if local.outputs.contains(name) => Some(entry.clone()),
            Declaration::Aux { name, .. } if local.aux.contains(name) => Some(entry.clone()),
            Declaration::Equation { name, .. } if exprs.contains_key(name) => Some(entry.clone()),
            _ => None,
        })
        .collect()
}

fn localise_checked(
    elaborated: &CheckedDsrvSpecification,
    local: &LocalStreams,
) -> CheckedDsrvSpecification {
    let original = elaborated.unchecked();
    let (exprs, types) = local_equations(elaborated, local);

    let needed_inputs = exprs
        .values()
        .flat_map(|expression| expression.stream_dependencies())
        .filter(|var| !local.contains(var))
        .collect::<HashSet<_>>();
    let input_vars = original
        .declarations()
        .iter()
        .filter_map(|entry| match entry {
            Declaration::Input { name, .. }
            | Declaration::Output { name, .. }
            | Declaration::Aux { name, .. } => Some(name),
            Declaration::Equation { .. } | Declaration::TypeAlias { .. } => None,
        })
        .filter(|var| needed_inputs.contains(*var))
        .cloned()
        .collect::<BTreeSet<_>>();
    debug!("Local expression inputs: {:?}", input_vars);

    let declarations = local_declarations(original, local, &input_vars, &exprs);
    let type_annotations = original
        .type_annotations
        .iter()
        .filter(|(name, _)| local.contains(name) || input_vars.contains(*name))
        .map(|(name, annotation)| (name.clone(), annotation.clone()))
        .collect();

    let mut expr_types = exprs.annotations_builder();
    assert_eq!(
        types.len(),
        exprs.nodes().len(),
        "one type per emitted node"
    );
    for (node, typ) in exprs.nodes().zip(types) {
        expr_types
            .insert(node, typ)
            .expect("an emitted node belongs to the localised forest");
    }
    let expr_types = expr_types
        .finish()
        .expect("localisation types every node it emits");

    let mut localised = DsrvSpecification::from_expression_forest_with_entries(
        input_vars,
        local.outputs.clone(),
        exprs,
        type_annotations,
        local.aux.iter().cloned(),
        declarations,
    );
    localised.source_context = original.source_context.clone();
    localised.sources = original.sources.clone();
    CheckedDsrvSpecification::new(localised, expr_types, elaborated.check_mode())
}

impl ElaboratedDsrvSpecification {
    /// Keep only what the local node monitors, as an elaborated specification
    /// whose nodes keep their types.
    ///
    /// Aux streams the local outputs read are kept, so
    /// recursive and stateful ones behave as they do centrally.
    ///
    /// Checking and elaboration happen before dependency pruning, so pruning
    /// cannot erase duplicate or conflicting declarations, or hide an
    /// ill-typed equation that no node monitors. Every dialect is localised:
    /// the dialects are nested, and a Full or Core specification simply
    /// places no streams explicitly.
    pub fn try_localise(
        &self,
        locality_spec: &impl LocalitySpec,
    ) -> Result<Self, DsrvLocalisationError> {
        let local_vars = locality_spec.local_vars()?;
        let elaborated = self.checked();
        let local = LocalStreams::collect(elaborated, &local_vars)?;
        Ok(Self::from_rewritten(localise_checked(elaborated, &local)))
    }
}

impl Localisable for ElaboratedDsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        self.try_localise(locality_spec)
            .unwrap_or_else(|error| panic!("Failed to localise DSRV specification: {error}"))
    }
}

impl TryLocalisable for ElaboratedDsrvSpecification {
    type Error = DsrvLocalisationError;

    fn try_localise(&self, locality_spec: &impl LocalitySpec) -> Result<Self, Self::Error> {
        ElaboratedDsrvSpecification::try_localise(self, locality_spec)
    }
}

#[cfg(test)]
mod tests {
    use crate::dsrv_fixtures::WithoutWarnings;
    use std::collections::{BTreeMap, BTreeSet};
    use std::rc::Rc;
    use std::vec;

    use contiguous_tree::TreeCursorExt;
    use petgraph::graph::DiGraph;

    use crate::core::{BinaryOperator, Semantics};
    use crate::dataflow::DataflowMonitor;
    use crate::distributed::distribution_graphs::GenericDistributionGraph;
    use crate::dsrv_fixtures::{elaborated, spec_simple_add_decomposable};
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::span::strip_span_ref;
    use crate::{TypeCheckOptions, Value};
    use proptest::prelude::*;
    use test_log::test;

    use super::*;
    use crate::lang::dsrv::test_support::arb_boolean_dsrv_spec;

    fn elaborate(spec: DsrvSpecification) -> ElaboratedDsrvSpecification {
        spec.check_and_elaborate(TypeCheckOptions::GRADUAL)
            .without_warnings()
            .expect("test specification should check")
    }

    #[test]
    fn localisation_keeps_the_checking_policy() {
        let source = "in x: Int\naux a: Int\nout y: Int\nout z: Int\na = x + 1\ny = a * 2\nz = x";
        for options in [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL] {
            let spec = crate::dsrv_fixtures::elaborated_with(source, options);
            let localised = spec.try_localise(&vec![VarName::new("y")]).unwrap();
            assert_eq!(localised.check_mode(), options.mode);
            assert_eq!(localised.source().check_mode(), options.mode);
            assert_eq!(localised.checked().check_mode(), options.mode);
        }
    }

    // Localising keeps the aux definitions a local output reads, and a kept
    // `if` keeps the policy it was written under. That records a need; it
    // does not make the distributed runtime able to meet it.
    #[test]
    fn localisation_keeps_the_if_policy_without_claiming_runtime_support() {
        use crate::core::{RuntimeCapability, ensure_runtime_support};
        use crate::runtime::builder::DistValueConfig;
        use crate::semantics::{DistributedSemantics, MonitoringSemantics};

        let distributed =
            <DistributedSemantics as MonitoringSemantics<DistValueConfig>>::RUNTIME_CAPABILITIES;
        for (header, lazy) in [("", false), ("use experimental::lazy_if\n", true)] {
            let source = format!(
                "language distributed\n{header}in c: Bool\nin x: Int\naux a: Int\n\
                 out y: Int\nout z: Int\na = if c then x else 0\ny = a + 1\nz = x"
            );
            let spec = crate::dsrv_fixtures::elaborated(&source);
            let localised = spec.try_localise(&vec![VarName::new("y")]).unwrap();
            assert_eq!(tree(&localised).aux_vars_in_order(), [VarName::new("a")]);
            let refused = ensure_runtime_support(&localised, distributed, "distributed");
            if lazy {
                let refusal = refused.expect_err("the kept `if` is still lazy");
                assert_eq!(refusal.requirement.capability, RuntimeCapability::LazyIf);
                ensure_runtime_support(
                    &localised,
                    crate::dataflow::RUNTIME_CAPABILITIES,
                    "dataflow",
                )
                .unwrap();
            } else {
                refused.unwrap();
            }
        }
    }

    #[test]
    fn localisation_reuses_the_submitted_models_checked_tree() {
        let source = "use experimental::{casts}\nin x: Int\nout y: Int\ny = x as Int";
        let report = ElaboratedDsrvSpecification::parse_with(source, TypeCheckOptions::STRICT)
            .expect("the submitted model parses");
        assert_eq!(
            report
                .warnings()
                .iter()
                .map(|warning| warning.code())
                .collect::<Vec<_>>(),
            ["dsrv.redundant-cast"]
        );
        let (submitted, _) = report.into_parts();
        let submitted = submitted.expect("the submitted model checks");

        let localised = submitted
            .try_localise(&vec![VarName::new("y")])
            .expect("the checked model localises without another analysis report");
        assert_eq!(localised.checked().unchecked().exprs.len(), 1);
    }

    /// The elaborated tree of a localised specification.
    fn tree(spec: &ElaboratedDsrvSpecification) -> &DsrvSpecification {
        spec.checked().unchecked()
    }

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
            elaborate(spec)
                .try_localise(&(unknown.clone(), &graph))
                .map(|_| ()),
            Err(DsrvLocalisationError::Locality(
                LocalitySpecError::UnknownNode { node: unknown }
            ))
        );
    }

    #[test]
    fn checking_refuses_duplicate_declarations_before_localisation_could_prune_them() {
        let spec = "in x: Int\nin x: Bool\nout y\ny = x"
            .parse::<DsrvSpecification>()
            .unwrap();

        assert!(
            spec.check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
    }

    fn assert_specs_eq_ignoring_spans(actual: &DsrvSpecification, expected: &DsrvSpecification) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        // Annotations are compared only where the expected specification
        // declares them; the rest are the types gradual checking inferred.
        for (name, annotation) in &expected.type_annotations {
            assert_eq!(actual.type_annotations.get(name), Some(annotation));
        }

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
        let localised_spec = elaborate(spec).localise(&restricted_vars);
        assert_specs_eq_ignoring_spans(
            tree(&localised_spec),
            &DsrvSpecification::new(
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
            ),
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
        let localised_spec = elaborate(spec).localise(&restricted_vars);
        assert_specs_eq_ignoring_spans(
            tree(&localised_spec),
            &DsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, Expr>::new(),
                BTreeMap::new(),
                vec![],
            ),
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = elaborated(spec_simple_add_decomposable());

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            tree(&local_spec1),
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
            tree(&local_spec2),
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
        // An auxiliary stream a local output reads is computed locally, as an
        // auxiliary stream; what it reads from elsewhere becomes an input.
        let spec = elaborated(
            "   in x
                    in y
                    in z
                    out w
                    out v
                    aux tmp
                    w = x + y
                    tmp = z + w
                    v = tmp",
        );

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            tree(&local_spec1),
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
            tree(&local_spec2),
            &DsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![
                    ("v".into(), Expr::Var("tmp".into())),
                    (
                        "tmp".into(),
                        Expr::BinOp(
                            Box::new(Expr::Var("z".into())),
                            Box::new(Expr::Var("w".into())),
                            BinaryOperator::Add,
                        ),
                    ),
                ]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec!["tmp".into()],
            ),
        );
    }

    #[test]
    fn localisation_preserves_entry_order_and_builds_a_fresh_local_boundary() {
        let source = "in remote\n\
                      out b\n\
                      b = helper + remote\n\
                      in other\n\
                      aux helper\n\
                      helper = other\n\
                      out c\n\
                      c = remote";
        let specification = elaborated(source);
        let localised = specification
            .try_localise(&vec![VarName::new("b"), VarName::new("c")])
            .expect("localisation should keep the helper");
        let localised_tree = tree(&localised);

        assert_eq!(
            localised_tree.input_vars_in_order(),
            [VarName::new("remote"), VarName::new("other")]
        );
        assert_eq!(
            localised_tree.output_vars_in_order(),
            [VarName::new("b"), VarName::new("c")]
        );
        assert_eq!(localised_tree.aux_vars_in_order(), [VarName::new("helper")]);
        assert_eq!(
            localised_tree
                .declarations()
                .iter()
                .map(|entry| entry.stream().expect("a stream declaration").name())
                .collect::<Vec<_>>(),
            ["remote", "b", "b", "other", "helper", "helper", "c", "c"]
        );
        let b = localised_tree.var_expr(&VarName::new("b")).unwrap();
        assert_eq!(b.to_string(), "(helper + remote)");

        let mut monitor =
            DataflowMonitor::compile_with_semantics(localised.clone(), Semantics::Untimed).unwrap();
        let mut output = [Value::NoVal, Value::NoVal];
        monitor
            .evaluate(&[Value::Int(10), Value::Int(2)], &mut output)
            .unwrap();
        assert_eq!(output, [Value::Int(12), Value::Int(10)]);
        let mut checked_monitor = DataflowMonitor::compile_checked(localised).unwrap();
        let mut checked_output = [Value::NoVal, Value::NoVal];
        checked_monitor
            .evaluate(&[Value::Int(10), Value::Int(2)], &mut checked_output)
            .unwrap();
        assert_eq!(checked_output, output);
    }

    #[test]
    fn localisation_keeps_the_type_of_every_elaborated_node() {
        let spec = elaborated(
            "in x: Int\nin flag: Bool\nout y: Float\naux h: Int\n\
             h = if flag then x + 1 else x\ny = if h > 0 then 1.5 else 2.5",
        );
        let localised = spec
            .try_localise(&vec![VarName::new("y")])
            .expect("localisation should keep h");
        let root = localised.var_expr_ref(&VarName::new("y")).unwrap();
        assert_eq!(root.typ(), &TCType::Float);
        assert_eq!(root.expr().to_string(), "(if (h > 0) then 1.5 else 2.5)");
        for name in ["h", "y"] {
            let name = VarName::new(name);
            let before = spec.var_expr_ref(&name).unwrap();
            let after = localised.var_expr_ref(&name).unwrap();
            for (before, after) in before.postorder().zip(after.postorder()) {
                assert!(before.kind().same_payload(after.kind()));
                assert_eq!(before.typ(), after.typ());
                assert_eq!(before.expr().span(), after.expr().span());
            }
        }
    }

    /// The localised form copies nodes within one program, so each copy
    /// keeps its origin, and it shares the program's archive.
    #[test]
    fn localisation_keeps_every_copied_nodes_origin_and_the_archive() {
        use crate::lang::dsrv::modules::ModuleCollector;

        let mut collector = ModuleCollector::new(
            "use experimental::{modules, functions}\nmod lib\nuse lib\n\
             in x: Int\naux h: Int\nout y: Int\nh = lib::twice(x)\ny = h + 1",
        )
        .unwrap();
        collector
            .supply("use experimental::{modules, functions}\ndef twice(n: Int) -> Int = n * 2\n")
            .unwrap();
        let spec = elaborate(
            crate::lang::dsrv::expand::expand_program(
                collector.finish().unwrap(),
                Default::default(),
            )
            .unwrap(),
        );
        let localised = spec.try_localise(&vec![VarName::new("y")]).unwrap();
        assert!(crate::lang::dsrv::ast::AstShared::ptr_eq(
            tree(&spec).sources(),
            tree(&localised).sources()
        ));
        let original = tree(&spec)
            .nodes()
            .map(|node| (node.span(), node.origin()))
            .collect::<Vec<_>>();
        let copied = tree(&localised)
            .nodes()
            .map(|node| (node.span(), node.origin()))
            .collect::<Vec<_>>();
        assert!(copied.iter().all(|node| original.contains(node)));
        assert!(copied.iter().any(|(_, origin)| origin.definition.is_some()));
    }

    #[test]
    fn localisation_keeps_a_transitive_chain_of_aux_streams() {
        let spec = elaborated(
            "in i\nin unused\nout result\nout other\naux h1\naux h2\naux h3\naux far\n\
             h1 = i\nh2 = h1 + 1\nh3 = h2 + 2\nfar = unused\nresult = h3\nother = far",
        );
        let localised = spec.try_localise(&vec![VarName::new("result")]).unwrap();
        let localised = tree(&localised);

        assert_eq!(localised.input_vars_in_order(), [VarName::new("i")]);
        assert_eq!(localised.output_vars_in_order(), [VarName::new("result")]);
        // `far` is read only by an output computed elsewhere.
        assert_eq!(
            localised.aux_vars_in_order(),
            [VarName::new("h1"), VarName::new("h2"), VarName::new("h3")]
        );
        assert_eq!(
            localised.exprs.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["h1", "h2", "h3", "result"].map(VarName::new))
        );
    }

    /// A recursive auxiliary stream cannot be expanded into its readers, but
    /// it is an ordinary stream to compute locally.
    #[test]
    fn localisation_keeps_a_recursive_aux_stream() {
        let spec = elaborated(
            "in x: Int\naux sum: Int\nout y: Int\nout z: Int\n\
             sum = default(sum[1], 0) + x\ny = sum\nz = x",
        );
        let localised = spec.try_localise(&vec![VarName::new("y")]).unwrap();
        assert_eq!(tree(&localised).aux_vars_in_order(), [VarName::new("sum")]);
        assert_eq!(tree(&localised).input_vars_in_order(), [VarName::new("x")]);

        let mut monitor = DataflowMonitor::compile_checked(localised).unwrap();
        let mut outputs = Vec::new();
        for x in [1, 2, 3] {
            let mut output = [Value::NoVal];
            monitor.evaluate(&[Value::Int(x)], &mut output).unwrap();
            outputs.push(output[0].clone());
        }
        assert_eq!(outputs, [1, 3, 6].map(Value::Int));
    }

    /// Two outputs reading one auxiliary stream share it, rather than each
    /// computing its own copy.
    #[test]
    fn localised_outputs_share_an_aux_stream() {
        let spec = elaborated(
            "in x: Int\naux previous: Int\nout a: Int\nout b: Int\n\
             previous = default(x[1], 0)\na = previous + 1\nb = previous * 2",
        );
        let localised = spec
            .try_localise(&vec![VarName::new("a"), VarName::new("b")])
            .unwrap();
        let localised_tree = tree(&localised);
        let indexes = localised_tree
            .nodes()
            .filter(|node| matches!(node.view(), ExprView::SIndex(..)))
            .count();
        assert_eq!(indexes, 1, "the history of x is kept once");

        let mut monitor = DataflowMonitor::compile_checked(localised).unwrap();
        let mut outputs = Vec::new();
        for x in [5, 7] {
            let mut output = [Value::NoVal, Value::NoVal];
            monitor.evaluate(&[Value::Int(x)], &mut output).unwrap();
            outputs.push(output);
        }
        assert_eq!(
            outputs,
            [
                [Value::Int(1), Value::Int(0)],
                [Value::Int(6), Value::Int(10)]
            ]
        );
    }

    #[test]
    fn checking_refuses_cyclic_aux_before_localisation_sees_it() {
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

        assert!(
            spec.check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
    }

    #[test]
    fn checking_refuses_a_missing_aux_definition_before_localisation() {
        let missing: VarName = "missing".into();
        let output: VarName = "output".into();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([missing.clone(), output.clone()]),
            BTreeMap::from([(output.clone(), Expr::Var(missing.clone()))]),
            BTreeMap::new(),
            [missing.clone()],
        );

        assert!(
            spec.check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
    }

    #[test]
    fn try_localise_reports_monitored_at_aux() {
        let helper: VarName = "helper".into();
        let output: VarName = "output".into();
        let spec = elaborated(
            "language distributed\naux helper\nout output\nhelper = true\noutput = monitored_at(helper, A)",
        );

        assert!(matches!(
            spec.try_localise(&vec![output]).map(|_| ()),
            Err(DsrvLocalisationError::MonitoredAtAux { variable, .. }) if variable == helper
        ));
    }

    #[test]
    fn try_localise_reports_monitored_at_aux_inside_a_kept_aux() {
        let spec = elaborated(
            "language distributed\naux helper\naux probe\nout output\n\
             helper = true\nprobe = monitored_at(helper, A)\noutput = probe",
        );

        assert!(matches!(
            spec.try_localise(&vec![VarName::new("output")]).map(|_| ()),
            Err(DsrvLocalisationError::MonitoredAtAux { variable, .. })
                if variable == VarName::new("helper")
        ));
    }

    #[test]
    fn try_localise_reports_dist() {
        let output: VarName = "output".into();
        let spec = elaborated("language distributed\nout output\noutput = dist(A, B)");

        assert_eq!(
            spec.try_localise(&vec![output]).map(|_| ()).unwrap_err(),
            DsrvLocalisationError::Dist
        );
    }

    #[test]
    fn localisation_stores_only_the_equations_it_keeps() {
        let helper: VarName = "helper".into();
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let spec = DsrvSpecification::new(
            BTreeSet::from(["input".into()]),
            BTreeSet::from([first.clone(), second.clone(), "elsewhere".into()]),
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
                ("elsewhere".into(), Expr::Var("input".into())),
            ]),
            BTreeMap::new(),
            [helper.clone()],
        );
        let elaborated = elaborate(spec);
        let localised = elaborated.try_localise(&vec![first, second]).unwrap();
        let localised = tree(&localised);

        assert_eq!(localised.aux_vars_in_order(), [helper]);
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
            let Ok(admitted) = spec
                .clone()
                .check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
            else {
                return Ok(());
            };
            let localised_spec = admitted.try_localise(&restricted_vars).unwrap();
            let localised_spec = tree(&localised_spec);

            for var in localised_spec.output_vars.iter() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.aux_vars.iter() {
                assert!(spec.aux_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(restricted_vars.contains(var) || localised_spec.aux_vars.contains(var));
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
