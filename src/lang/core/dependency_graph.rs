use std::collections::{BTreeMap, BTreeSet};

use contiguous_tree::TreeCursorExt;
use petgraph::algo::toposort;
use petgraph::dot::{Config, Dot};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeIndex;
use petgraph::visit::{EdgeRef, IntoNodeReferences};
use tracing::debug;

use crate::lang::dsrv::ast::{CheckedExpr, DsrvSpecification, Expr, ExprRef, ExprView};
use crate::semantics::AsyncConfig;
use crate::{Specification, VarName};

pub trait DependencyGraphExpr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DependencyGraphRoots {
    #[default]
    Outputs,
    AllStreams,
}

pub trait DependencyGraphSpec {
    fn dependency_graph_for(&self, roots: DependencyGraphRoots) -> DepGraph;

    fn dependency_graph(&self) -> DepGraph {
        self.dependency_graph_for(DependencyGraphRoots::Outputs)
    }
}

impl DependencyGraphExpr for Expr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph {
        sexpr_dependencies(self, root)
    }
}

impl DependencyGraphExpr for CheckedExpr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph {
        sexpr_dependencies(self.expr(), root)
    }
}

// Interface for resolving dependencies.
pub trait DependencyResolver<AC>
where
    AC: AsyncConfig,
{
    // Adds a new dependency to the resolver
    fn add_dependency(&mut self, var: &VarName, expr: &AC::Expr);

    // Remove dependency to the resolver
    fn remove_dependency(&mut self, var: &VarName, expr: &AC::Expr);

    // Returns how long the variable needs to be saved before it can be forgotten
    fn longest_time_dependency(&self, var: &VarName) -> u64;

    // Calls `longest_time_dependency` on all variables
    fn longest_time_dependencies(&self) -> BTreeMap<VarName, u64>;
}

// Graph weights are Vecs of time indices
type Weight = u64;
type Node = VarName;
// Graphs are directed
type GraphType = DiGraph<Node, Weight>;

#[derive(Clone, Debug)]
pub struct DepGraph {
    graph: GraphType,
}

impl DepGraph {
    pub fn from_dependencies<Dependencies>(
        dependencies: impl IntoIterator<Item = (VarName, Dependencies)>,
    ) -> Self
    where
        Dependencies: IntoIterator<Item = VarName>,
    {
        let mut graph = Self::empty_graph();
        let dependencies = dependencies
            .into_iter()
            .map(|(consumer, inputs)| (consumer, inputs.into_iter().collect::<Vec<_>>()))
            .collect::<BTreeMap<_, _>>();
        let mut nodes = BTreeMap::new();
        for name in dependencies.keys().chain(dependencies.values().flatten()) {
            if !nodes.contains_key(name) {
                nodes.insert(name.clone(), graph.graph.add_node(name.clone()));
            }
        }
        for (consumer, inputs) in dependencies {
            for input in inputs {
                graph.graph.add_edge(nodes[&consumer], nodes[&input], 0);
            }
        }
        graph
    }

    /// Orders streams after their dependencies. Graph edges point from each consumer to the
    /// variables it reads, so `toposort` is reversed before input-only nodes are discarded.
    pub fn topological_streams(
        &self,
        stream_vars: &BTreeSet<VarName>,
    ) -> Result<Vec<VarName>, VarName> {
        let mut ordered =
            toposort(&self.graph, None).map_err(|cycle| self.graph[cycle.node_id()].clone())?;
        ordered.reverse();
        Ok(ordered
            .into_iter()
            .map(|node| self.graph[node].clone())
            .filter(|name| stream_vars.contains(name))
            .collect())
    }

    pub fn from_sexprs<Expr>(exprs: BTreeMap<VarName, Expr>) -> Self
    where
        Expr: DependencyGraphExpr,
    {
        let mut graph = Self::empty_graph();
        let mut nodes = BTreeMap::new();
        for (var, expr) in exprs {
            let expr_deps = expr.dependency_graph_for_root(&var);
            graph.merge_graph_with_nodes(&expr_deps, &mut nodes);
        }
        debug!("Constructed dependency graph: {:?}", graph.as_dot_graph());
        graph
    }

    pub fn resolver_from_spec<AC>(spec: AC::Spec) -> impl DependencyResolver<AC>
    where
        AC: AsyncConfig,
        AC::Expr: DependencyGraphExpr,
        AC::Spec: DependencyGraphSpec,
    {
        spec.dependency_graph()
    }

    pub fn longest_time_dependency(&self, name: &VarName) -> u64 {
        let node = self
            .graph
            .node_indices()
            .find(|i| self.graph[*i] == *name)
            .unwrap_or_default();
        self.graph
            .edges_directed(node, petgraph::Direction::Incoming)
            .map(|edge| edge.weight().clone())
            .max()
            .unwrap_or_default()
    }

    pub fn longest_time_dependencies(&self) -> BTreeMap<VarName, u64> {
        let mut map = BTreeMap::new();
        for (node, name) in self.graph.node_references() {
            let longest_dep = self
                .graph
                .edges_directed(node, petgraph::Direction::Incoming)
                .map(|edge| edge.weight().clone())
                .max()
                .unwrap_or_default();
            map.insert(name.clone(), longest_dep);
        }
        map
    }

    #[allow(dead_code)]
    pub fn as_dot_graph(&self) -> Dot<'_, &GraphType> {
        self.as_dot_graph_with_config(&[])
    }

    #[allow(dead_code)]
    fn as_dot_graph_with_config<'a>(&'a self, config: &'a [Config]) -> Dot<'a, &'a GraphType> {
        Dot::with_config(&self.graph, config)
    }

    fn find_edges(&self, from: NodeIndex, to: NodeIndex) -> impl Iterator<Item = EdgeIndex> {
        self.graph.edge_indices().filter(move |x| {
            let edge = self.graph.edge_endpoints(*x).unwrap();
            edge.0 == from && edge.1 == to
        })
    }

    fn find_edge_with_weight(
        &self,
        from: NodeIndex,
        to: NodeIndex,
        weight: Weight,
    ) -> Option<EdgeIndex> {
        self.find_edges(from, to)
            .find(|x| self.graph.edge_weight(*x).unwrap() == &weight)
    }

    // Similar to creating a new graph with the union of the nodes and edges,
    // but this is done in-place
    fn merge_graphs(&mut self, other: &DepGraph) {
        let mut node_map = BTreeMap::new();
        for node in self.graph.node_indices() {
            node_map.insert(self.graph[node].clone(), node);
        }
        self.merge_graph_with_nodes(other, &mut node_map);
    }

    /// Merge a sequence of expression graphs while reusing the name-to-node map.
    ///
    /// Building a specification graph calls this once per stream. Keeping the map
    /// outside the loop avoids rescanning every node already accumulated for every
    /// subsequent stream.
    fn merge_graph_with_nodes(
        &mut self,
        other: &DepGraph,
        nodes: &mut BTreeMap<VarName, NodeIndex>,
    ) {
        for (_, name) in other.graph.node_references() {
            nodes
                .entry(name.clone())
                .or_insert_with(|| self.graph.add_node(name.clone()));
        }

        for edge in other.graph.edge_references() {
            let source = nodes[&other.graph[edge.source()]];
            let target = nodes[&other.graph[edge.target()]];
            self.graph.add_edge(source, target, *edge.weight());
        }
    }

    fn diff_graphs(&mut self, other: &DepGraph) {
        let mut node_map = BTreeMap::new();

        // Add all nodes from `self` into the map
        for node in self.graph.node_indices() {
            let node_value = self.graph[node].clone();
            node_map.insert(node_value, node);
        }

        // Add nodes from `other` if they are not already in `self`
        for (_, name) in other.graph.node_references() {
            node_map
                .entry(name.clone())
                .or_insert_with(|| self.graph.add_node(name.clone()));
        }

        // Remove the edge if it exists in `self`
        for edge in other.graph.edge_references() {
            // This is the index of the node in `self` if it exists
            let source_index = node_map[&other.graph[edge.source()]];
            let target_index = node_map[&other.graph[edge.target()]];
            let weight = *edge.weight();

            // Remove the edge if it exists in `self`
            if let Some(edge_idx) = self.find_edge_with_weight(source_index, target_index, weight) {
                self.graph.remove_edge(edge_idx);
            }
        }
    }

    fn empty_graph() -> Self {
        Self {
            graph: GraphType::new(),
        }
    }

    #[allow(dead_code)]
    /// Check if the graph is productive (i.e. has no cycles in which zero time
    /// passes). This is necessary to check that the runtime will not
    /// deadlock when processing them.
    pub fn is_productive(&self) -> bool {
        todo!(
            "Can be useful semantic check, but old implementation was wrong with nested time dependencies."
        )
    }
}

// ExprKind specific dependency resolution:
// Traverses the sexpr and returns a DepGraph of its dependencies to other variables
// NOTE: The graph returned here may have multiple edges to the same node.
// Can be combined by calling `combine_edges`. This is not done in this function for efficiency
fn add_sexpr_dependencies(
    expr: &Expr,
    root_name: &Node,
    graph: &mut DepGraph,
    nodes: &mut BTreeMap<VarName, NodeIndex>,
) {
    fn visit(
        expr: ExprRef<'_>,
        steps: &[Weight],
        graph: &mut DepGraph,
        nodes: &mut BTreeMap<VarName, NodeIndex>,
        current_node: NodeIndex,
        current_idx: u64,
    ) {
        debug!(
            "Visiting {:?} with steps {:?} and current_idx {}",
            expr.kind(),
            steps,
            current_idx
        );
        match expr.view() {
            ExprView::Var(name) => {
                let node = *nodes
                    .entry(name.clone())
                    .or_insert_with(|| graph.graph.add_node(name.clone()));
                if steps.is_empty() {
                    graph.graph.add_edge(current_node, node, 0);
                } else {
                    for weight in steps {
                        graph.graph.add_edge(current_node, node, *weight);
                    }
                }
            }
            ExprView::SIndex(child, offset) => {
                let current_idx = current_idx + offset;
                let mut nested_steps = steps.to_vec();
                nested_steps.push(current_idx);
                visit(
                    child,
                    &nested_steps,
                    graph,
                    nodes,
                    current_node,
                    current_idx,
                );
            }
            _ => {
                for child in expr.children() {
                    visit(child, steps, graph, nodes, current_node, current_idx);
                }
            }
        }
    }

    debug!(
        "sexpr_dependencies for {}: {:?}",
        root_name,
        expr.as_ref().kind()
    );
    let root_node = *nodes
        .entry(root_name.clone())
        .or_insert_with(|| graph.graph.add_node(root_name.clone()));
    visit(expr.as_ref(), &[], graph, nodes, root_node, 0);
}

fn sexpr_dependencies(expr: &Expr, root_name: &Node) -> DepGraph {
    let mut graph = DepGraph::empty_graph();
    add_sexpr_dependencies(expr, root_name, &mut graph, &mut BTreeMap::new());
    graph
}

impl DependencyGraphSpec for DsrvSpecification {
    fn dependency_graph_for(&self, roots: DependencyGraphRoots) -> DepGraph {
        let mut graph = DepGraph::empty_graph();
        let mut nodes = BTreeMap::new();
        let roots = match roots {
            DependencyGraphRoots::Outputs => self.output_vars(),
            DependencyGraphRoots::AllStreams => self.stream_vars(),
        };
        for var in roots {
            if let Some(expr) = self.var_expr(&var) {
                add_sexpr_dependencies(&expr, &var, &mut graph, &mut nodes);
            }
        }
        debug!("Constructed dependency graph: {:?}", graph.as_dot_graph());
        graph
    }
}

impl DependencyGraphSpec for crate::lang::dsrv::ast::CheckedDsrvSpecification {
    fn dependency_graph_for(&self, roots: DependencyGraphRoots) -> DepGraph {
        self.unchecked().dependency_graph_for(roots)
    }
}

impl<AC> DependencyResolver<AC> for DepGraph
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
{
    fn add_dependency(&mut self, var: &VarName, expr: &AC::Expr) {
        let expr_deps = expr.dependency_graph_for_root(var);
        self.merge_graphs(&expr_deps);
    }

    fn remove_dependency(&mut self, name: &VarName, expr: &AC::Expr) {
        let expr_deps = expr.dependency_graph_for_root(name);
        self.diff_graphs(&expr_deps);
    }

    fn longest_time_dependency(&self, name: &VarName) -> u64 {
        DepGraph::longest_time_dependency(self, name)
    }

    fn longest_time_dependencies(&self) -> BTreeMap<VarName, u64> {
        DepGraph::longest_time_dependencies(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DsrvSpecification;
    use crate::dsrv_fixtures::*;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::parser::parse_str;

    fn test_parser(input: &mut &str) -> anyhow::Result<DsrvSpecification> {
        parse_str(input)
    }

    fn specs() -> BTreeMap<&'static str, &'static str> {
        BTreeMap::from([
            ("single_no_inp", "out x\nx = 42"),
            ("single_inp_past", "in a\nout x\nx = a[1]"),
            ("multi_out_past", "in a\nout x\nout y\nx = a\ny = a[1]"),
            ("multi_dependent", "in a\nout x\nout y\nx = a\ny = x"),
            (
                "multi_dependent_past",
                "in a\nout x\nout y\nx = a[1]\ny = x[1]",
            ),
            ("multi_same_dependent", "in a\nout x\nx = a + a[1]"),
            ("recursion", "out z\nz = default(z[1], 0)"),
        ])
    }

    fn find_node(graph: &GraphType, name: &'static str) -> NodeIndex {
        graph
            .node_indices()
            .find(|i| graph[*i] == name.into())
            .unwrap()
    }

    fn get_weights(graph: &GraphType, from: NodeIndex, to: NodeIndex) -> Vec<Weight> {
        graph
            .edges_directed(from, petgraph::Direction::Outgoing)
            .filter(|edge| edge.target() == to)
            .map(|edge| *edge.weight())
            .collect()
    }

    fn get_graph(graph: impl DependencyResolver<TestConfig> + 'static) -> GraphType {
        <dyn std::any::Any>::downcast_ref::<DepGraph>(&graph)
            .unwrap()
            .graph
            .clone()
    }

    #[test]
    fn test_graph_empty() {
        let graph = DepGraph::empty_graph();
        assert_eq!(graph.graph.node_count(), 0);
        assert_eq!(graph.graph.edge_count(), 0);
    }

    #[test]
    fn test_graph_simple() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_graph_index_past() {
        let mut spec = specs()["single_inp_past"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1]);
    }

    #[test]
    fn test_graph_multi_out_past() {
        let mut spec = specs()["multi_out_past"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        let y = find_node(&graph, "y");
        assert!(graph.contains_edge(x, a));
        assert!(graph.contains_edge(y, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![0]);
        let weight = get_weights(&graph, y, a);
        assert_eq!(weight, vec![1]);
    }

    #[test]
    fn test_graph_multi_dependent() {
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        let y = find_node(&graph, "y");
        assert!(graph.contains_edge(x, a));
        assert!(graph.contains_edge(y, x));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![0]);
        let weight = get_weights(&graph, y, x);
        assert_eq!(weight, vec![0]);
    }

    #[test]
    fn test_graph_multi_dependent_past() {
        let mut spec = specs()["multi_dependent_past"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        let y = find_node(&graph, "y");
        assert!(graph.contains_edge(x, a));
        assert!(graph.contains_edge(y, x));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1]);
        let weight = get_weights(&graph, y, x);
        assert_eq!(weight, vec![1]);
    }

    #[test]
    fn test_graph_multi_same_dependent() {
        let mut spec = specs()["multi_same_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 2);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1, 0]);
    }

    #[test]
    fn test_graph_recursion() {
        let mut spec = specs()["recursion"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_sexprs(spec.exprs).graph;
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 1);
        let z = find_node(&graph, "z");
        assert!(graph.contains_edge(z, z));
        let weight = get_weights(&graph, z, z);
        assert_eq!(weight, vec![1]);
    }

    #[test]
    fn test_time_simple() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        let expected = BTreeMap::from([("x".into(), 0)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_index_past() {
        let mut spec = specs()["single_inp_past"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_multi_out_past() {
        let mut spec = specs()["multi_out_past"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"y".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 0), ("y".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_multi_dependent() {
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"y".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 0);
        let expected = BTreeMap::from([("x".into(), 0), ("y".into(), 0), ("a".into(), 0)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_multi_dependent_past() {
        let mut spec = specs()["multi_dependent_past"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 1);
        assert_eq!(dep.longest_time_dependency(&"y".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 1), ("y".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_multi_same_dependent() {
        let mut spec = specs()["multi_same_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_recursion() {
        let mut spec = specs()["recursion"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"z".into()), 1);
        let expected = BTreeMap::from([("z".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_add_dep_simple() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(&"new".into(), &Expr::Val(42));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_dep_new_edge() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(&"a".into(), &Expr::Var("x".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(a, x));
        let weight = get_weights(&graph, a, x);
        assert_eq!(weight, vec![0]);
    }

    #[test]
    fn test_add_dep_new_edge_existing() {
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(&"a".into(), &Expr::Var("y".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        let y = find_node(&graph, "y");
        assert!(graph.contains_edge(x, a));
        assert!(graph.contains_edge(y, x));
        assert!(graph.contains_edge(a, y));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![0]);
        let weight = get_weights(&graph, y, x);
        assert_eq!(weight, vec![0]);
        let weight = get_weights(&graph, a, y);
        assert_eq!(weight, vec![0]);
    }

    #[test]
    fn test_add_dep_add_weight() {
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(&"x".into(), &Expr::Var("a".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![0, 0]); // The new weight was added correctly
    }

    #[test]
    fn test_add_dep_add_weight_past() {
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(
            &"x".into(),
            &Expr::SIndex(Box::new(Expr::Var("a".into())), 1),
        );
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1, 0]); // The new weight was added correctly
    }

    #[test]
    fn test_rm_dep_removes_edge() {
        // Case where the last weight is removed so we remove the entire edge
        let mut spec = specs()["multi_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.remove_dependency(&"y".into(), &Expr::Var("x".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 1);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        let y = find_node(&graph, "y");
        assert!(graph.contains_edge(x, a));
        assert!(!graph.contains_edge(y, x));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![0]);
    }

    #[test]
    fn test_rm_dep_removes_weight() {
        // Case where we still have a weight left after removing dependency
        let mut spec = specs()["multi_same_dependent"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.remove_dependency(&"x".into(), &Expr::Var("a".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1]);
    }

    #[test]
    fn test_double_sindex() {
        // Tests that multiple indices are aggregated correctly.

        // Parentheses unfortunately needed due to parser bugs
        let mut spec = "in x\nout z\nz = (x[1])[1]";
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 2);
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_double_sindex2() {
        // Tests that multiple indices are aggregated correctly.
        // In this case by treating indirect dependencies correctly.
        // Here, it could be said that there is a dependency of 2 for x,
        // but we don't since the dependecy is stored in an intermediate variable (y).

        // Parentheses unfortunately needed due to parser bugs
        let mut spec = "in x\nout y\nout z\ny = x[1]\nz = y[1]";
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 1);
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_binary_regression() {
        // Regression test for a bug where time dependencies of the lhs in binary operations were carried over to rhs.
        let mut spec = spec_acc_monitor();
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"z".into()), 1);
    }

    #[test]
    fn auxiliary_roots_are_included_only_when_requested() {
        let mut source = "in x\nout z\naux history\nz = x\nhistory = x[3]";
        let spec = test_parser(&mut source).unwrap();

        let output_only = spec.dependency_graph();
        let with_aux = spec.dependency_graph_for(DependencyGraphRoots::AllStreams);

        assert_eq!(output_only.longest_time_dependency(&"x".into()), 0);
        assert_eq!(with_aux.longest_time_dependency(&"x".into()), 3);
    }
}
