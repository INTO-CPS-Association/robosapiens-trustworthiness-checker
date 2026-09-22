//! Localisation and auxiliary-expression expansion for distributed DSRV specifications.

use static_assertions::assert_obj_safe;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Debug};

use contiguous_tree::{RewriteError, TreeCursor, TreeNodeMut};
use tracing::debug;

use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::ast::{
    CheckedDsrvSpecification, CheckedExprRef, Declaration, DependencyKind, DsrvSpecification,
    ExprBuilder, ExprForestMap, ExprId, ExprRewriteNode, ExprView,
};
use crate::lang::dsrv::span::Span;
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
    spec.stream_vars = spec
        .output_vars
        .iter()
        .chain(spec.aux_vars.iter())
        .cloned()
        .collect();
    spec.declarations.retain(|entry| match entry {
        Declaration::Input { name, .. } => spec.input_vars.contains(name),
        Declaration::Output { name, .. } => spec.output_vars.contains(name),
        Declaration::Aux { name, .. } => spec.aux_vars.contains(name),
        Declaration::Equation { name, .. } => spec.exprs.contains_key(name),
        // The localised specification keeps the namespace, so its aliases.
        Declaration::TypeAlias { .. } => true,
    });
    spec
}

/// Inline every auxiliary stream into the outputs that use it, rewriting the
/// elaborated tree so that each emitted node keeps the type it had there.
fn try_inline_aux(
    spec: DsrvSpecification,
    elaborated: &CheckedDsrvSpecification,
) -> Result<CheckedDsrvSpecification, DsrvLocalisationError> {
    let aux_vars = spec.aux_vars.clone();
    for aux in &aux_vars {
        if !spec.exprs.contains_key(aux) {
            return Err(DsrvLocalisationError::MissingAuxDefinition {
                variable: aux.clone(),
            });
        }
    }

    let names = spec
        .exprs
        .keys()
        .filter(|var| spec.output_vars.contains(*var) && !aux_vars.contains(*var))
        .cloned()
        .collect::<Vec<_>>();
    let mut inliner = AuxInliner {
        elaborated,
        aux_vars: &aux_vars,
        types: Vec::new(),
        expanding: Vec::new(),
    };
    let mut builder = ExprBuilder::with_capacities(0, names.len());
    let roots = builder
        .try_rewrite_forest(
            names.iter().map(|name| {
                elaborated
                    .var_expr_ref(name)
                    .expect("every retained equation is elaborated")
            }),
            |mut node| {
                let source = node.source();
                inliner.emit(&mut node, source, true)
            },
        )
        .map_err(|error| match error {
            RewriteError::Convert(error) => error,
            error => panic!("aux inlining emits one subtree per node: {error}"),
        })?;
    let forest = builder
        .finish_forest(roots)
        .expect("aux inlining emits one complete tree per output");
    let exprs = ExprForestMap::new(names, forest)
        .expect("aux inlining keeps the sorted, unique output names");
    let mut expr_types = exprs.annotations_builder();
    assert_eq!(inliner.types.len(), exprs.nodes().len());
    for (node, typ) in exprs.nodes().zip(inliner.types) {
        expr_types
            .insert(node, typ)
            .expect("an emitted node belongs to the inlined forest");
    }
    let expr_types = expr_types
        .finish()
        .expect("aux inlining types every node it emits");

    let output_vars = spec.output_vars.difference(&aux_vars).cloned().collect();
    let type_annotations = spec
        .type_annotations
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();
    let declarations = spec
        .declarations
        .into_iter()
        .filter(|entry| match entry {
            Declaration::Aux { .. } => false,
            Declaration::Output { name, .. } => !aux_vars.contains(name),
            Declaration::Equation { name, .. } => exprs.contains_key(name),
            Declaration::Input { .. } | Declaration::TypeAlias { .. } => true,
        })
        .collect();

    let mut inlined = DsrvSpecification::from_expression_forest_with_entries(
        spec.input_vars,
        output_vars,
        exprs,
        type_annotations,
        std::iter::empty(),
        declarations,
    );
    inlined.source_context = spec.source_context;
    inlined.sources = spec.sources;
    Ok(CheckedDsrvSpecification::new(
        inlined,
        expr_types,
        elaborated.check_mode(),
    ))
}

/// Emits one destination subtree per source node, replacing each use of an
/// auxiliary stream by a copy of its definition.
struct AuxInliner<'a> {
    elaborated: &'a CheckedDsrvSpecification,
    aux_vars: &'a BTreeSet<VarName>,
    /// The type of each emitted node, in allocation order.
    types: Vec<TCType>,
    /// The auxiliary uses being expanded, outermost first.
    expanding: Vec<(VarName, Span)>,
}

impl<'a> AuxInliner<'a> {
    /// Emit `cursor` as one subtree. With `children_emitted`, `cursor` is the
    /// source node being rewritten and the rewrite has emitted its children.
    fn emit(
        &mut self,
        node: &mut ExprRewriteNode<'_, '_, CheckedExprRef<'a>>,
        cursor: CheckedExprRef<'a>,
        children_emitted: bool,
    ) -> Result<ExprId, DsrvLocalisationError> {
        match cursor.expr().view() {
            ExprView::Var(var) if self.aux_vars.contains(var) => {
                let span = cursor.expr().span();
                if let Some(start) = self.expanding.iter().position(|(name, _)| name == var) {
                    let mut replacement_spans = self.expanding[start..]
                        .iter()
                        .map(|(_, span)| *span)
                        .collect::<Vec<_>>();
                    replacement_spans.push(span);
                    return Err(DsrvLocalisationError::CyclicReplacement { replacement_spans });
                }
                let definition = self
                    .elaborated
                    .var_expr_ref(var)
                    .expect("every auxiliary stream has a definition");
                self.expanding.push((var.clone(), span));
                let root = self.emit(node, definition, false)?;
                self.expanding.pop();
                return Ok(root);
            }
            ExprView::MonitoredAt(var, monitor) if self.aux_vars.contains(var) => {
                return Err(DsrvLocalisationError::MonitoredAtAux {
                    variable: var.clone(),
                    node: monitor.clone(),
                });
            }
            ExprView::Dist(_, _) => return Err(DsrvLocalisationError::Dist),
            _ => {}
        }

        let kind = if children_emitted {
            node.source_node().rebuild(cursor.kind().clone())
        } else {
            let mut emitted = Vec::new();
            for child in cursor.child_ids() {
                emitted.push((child, self.emit(node, cursor.child(child), false)?));
            }
            let mut kind = cursor.kind().clone();
            kind.for_each_child_id_mut(|child| {
                *child = emitted
                    .iter()
                    .find(|(source, _)| source == child)
                    .map(|(_, emitted)| *emitted)
                    .expect("every child was emitted");
            });
            kind
        };
        let id = node
            .alloc(kind, cursor.expr().metadata().clone())
            .expect("emitted children are the trailing roots");
        self.types.push(cursor.typ().clone());
        Ok(id)
    }
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
    let input_order = original
        .declarations()
        .iter()
        .filter_map(|entry| match entry {
            Declaration::Input { name, .. }
            | Declaration::Output { name, .. }
            | Declaration::Aux { name, .. } => Some(name),
            Declaration::Equation { .. } | Declaration::TypeAlias { .. } => None,
        })
        .filter(|var| !local_set.contains(*var))
        .filter(|var| needed_inputs.contains(var))
        .cloned()
        .collect::<Vec<_>>();
    spec.input_vars = input_order.iter().cloned().collect();

    spec.output_vars.retain(|var| local_set.contains(var));
    spec.aux_vars.retain(|var| local_set.contains(var));
    spec.stream_vars = spec
        .output_vars
        .iter()
        .chain(spec.aux_vars.iter())
        .cloned()
        .collect();
    let mut declared_inputs = BTreeSet::new();
    spec.declarations = original
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
            } if spec.input_vars.contains(name) && declared_inputs.insert(name.clone()) => {
                Some(Declaration::Input {
                    name: name.clone(),
                    annotation: annotation.clone(),
                    span: *span,
                })
            }
            Declaration::Output { name, .. } if spec.output_vars.contains(name) => {
                Some(entry.clone())
            }
            Declaration::Aux { name, .. } if spec.aux_vars.contains(name) => Some(entry.clone()),
            Declaration::Equation { name, .. } if spec.exprs.contains_key(name) => {
                Some(entry.clone())
            }
            _ => None,
        })
        .collect();

    debug!("Local expression inputs: {:?}", needed_inputs);
    spec
}

impl ElaboratedDsrvSpecification {
    /// Keep only what the local node monitors, as an elaborated specification
    /// whose nodes keep their types.
    ///
    /// Checking and elaboration happen before dependency pruning or auxiliary
    /// expansion, so those rewrites cannot erase duplicate or conflicting
    /// declarations, or hide an ill-typed equation that no node monitors.
    /// Every dialect is localised: the dialects are nested, and a Full or Core
    /// specification simply places no streams explicitly.
    pub fn try_localise(
        &self,
        locality_spec: &impl LocalitySpec,
    ) -> Result<Self, DsrvLocalisationError> {
        let local_vars = locality_spec.local_vars()?;
        let elaborated = self.checked();
        let original = elaborated.unchecked();
        let inlined = try_inline_aux(
            prune_to_dependency_closure(original.clone(), &local_vars),
            elaborated,
        )?;
        let local_set = local_vars.into_iter().collect::<BTreeSet<_>>();
        let localised =
            inlined.map_specification(|spec| finish_localisation(spec, original, &local_set));
        Ok(Self::from_rewritten(localised))
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
    use crate::lang::dsrv::ast::{Declaration, Expr};
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

    // Localising inlines aux definitions, and an inlined `if` keeps the
    // policy it was written under. That records a need; it does not make the
    // distributed runtime able to meet it.
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
            assert!(tree(&localised).aux_vars.is_empty());
            let refused = ensure_runtime_support(&localised, distributed, "distributed");
            if lazy {
                let refusal = refused.expect_err("the inlined `if` is still lazy");
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

    fn inline_aux(spec: DsrvSpecification) -> DsrvSpecification {
        let spec = elaborate(spec);
        try_inline_aux(tree(&spec).clone(), spec.checked())
            .unwrap_or_else(|error| panic!("Failed to inline aux variables: {error}"))
            .unchecked()
            .clone()
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
        // Tests that localisation correctly handles auxiliary variables
        // Note that these must be specified similarly to output variables
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
            .expect("localisation should inline the helper");
        let localised_tree = tree(&localised);

        assert_eq!(
            localised_tree.input_vars_in_order(),
            [VarName::new("remote"), VarName::new("other")]
        );
        assert_eq!(
            localised_tree.output_vars_in_order(),
            [VarName::new("b"), VarName::new("c")]
        );
        assert_eq!(localised_tree.aux_vars_in_order(), []);
        assert_eq!(
            localised_tree
                .declarations()
                .iter()
                .map(|entry| entry.stream().expect("a stream declaration").name())
                .collect::<Vec<_>>(),
            ["remote", "b", "b", "other", "c", "c"]
        );
        assert!(
            localised_tree
                .declarations()
                .iter()
                .all(|entry| !matches!(entry, Declaration::Aux { .. }))
        );
        let b = localised_tree.var_expr(&VarName::new("b")).unwrap();
        assert_eq!(b.to_string(), "(other + remote)");

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
            .expect("localisation should inline h");
        let root = localised.var_expr_ref(&VarName::new("y")).unwrap();
        assert_eq!(root.typ(), &TCType::Float);
        assert_eq!(
            root.expr().to_string(),
            "(if ((if flag then (x + 1) else x) > 0) then 1.5 else 2.5)"
        );
        // Each inlined node has the type it had in the definition of h.
        let inlined = spec.var_expr_ref(&VarName::new("h")).unwrap();
        let copied = root
            .postorder()
            .filter(|node| matches!(node.expr().view(), ExprView::If(..)))
            .find(|node| node.typ() == &TCType::Int)
            .expect("the inlined definition of h");
        for (before, after) in inlined.postorder().zip(copied.postorder()) {
            assert!(before.kind().same_payload(after.kind()));
            assert_eq!(before.typ(), after.typ());
            assert_eq!(before.expr().span(), after.expr().span());
        }
    }

    /// Inlining an auxiliary stream copies its nodes within one program, so
    /// each copy keeps its origin, and the localised form shares the
    /// program's archive.
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
    fn test_inline_aux_single_aux() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();
        let tmp: VarName = "tmp".into();
        let z: VarName = "z".into();

        let spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from([z.clone()]),
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

        assert_specs_eq_ignoring_spans(
            &result,
            &DsrvSpecification::new(
                BTreeSet::from([x, y]),
                BTreeSet::from([z]),
                expected_exprs,
                BTreeMap::new(),
                vec![],
            ),
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
            BTreeSet::from([out.clone()]),
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

        assert_specs_eq_ignoring_spans(
            &result,
            &DsrvSpecification::new(
                BTreeSet::from([i]),
                BTreeSet::from([out]),
                expected_exprs,
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn checking_refuses_cyclic_aux_before_it_could_be_inlined() {
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
    fn try_localise_reports_dist() {
        let output: VarName = "output".into();
        let spec = elaborated("language distributed\nout output\noutput = dist(A, B)");

        assert_eq!(
            spec.try_localise(&vec![output]).map(|_| ()).unwrap_err(),
            DsrvLocalisationError::Dist
        );
    }

    #[test]
    fn checking_refuses_cyclic_aux_before_localisation() {
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
        let _ = output;

        assert!(
            spec.check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
    }

    #[test]
    fn localisation_finalisation_preserves_compact_rewrite_storage() {
        let helper: VarName = "helper".into();
        let first: VarName = "first".into();
        let second: VarName = "second".into();
        let spec = DsrvSpecification::new(
            BTreeSet::from(["input".into()]),
            BTreeSet::from([first.clone(), second.clone()]),
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
        let elaborated = elaborate(spec);
        let spec = tree(&elaborated);

        let local_set = BTreeSet::from([first, second]);
        let pruned = prune_to_dependency_closure(
            spec.clone(),
            &local_set.iter().cloned().collect::<Vec<_>>(),
        );
        let rewritten = try_inline_aux(pruned, elaborated.checked()).unwrap();
        let rewritten_name = rewritten
            .unchecked()
            .roots()
            .next()
            .expect("local roots should exist")
            .0
            .clone();
        let rewritten_root = rewritten.unchecked().var_expr(&rewritten_name).unwrap();

        let localised = rewritten
            .map_specification(|rewritten| finish_localisation(rewritten, spec, &local_set));
        let localised = localised.unchecked();
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
