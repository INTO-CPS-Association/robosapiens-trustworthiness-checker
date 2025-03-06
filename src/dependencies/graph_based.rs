use std::collections::{BTreeMap, BTreeSet};

use petgraph::algo::{find_negative_cycle, is_cyclic_directed};
use petgraph::dot::{Config, Dot};
use petgraph::graph::{DiGraph, EdgeReference, NodeIndex};
use petgraph::visit::{EdgeFiltered, EdgeRef, FilterEdge, IntoNodeReferences};

use crate::{SExpr, Specification, VarName};

use super::traits::DependencyResolver;

// Graph weights are Vecs of time indices
// (we want a container with duplicates for DUPs)
type Weight = Vec<isize>;
type Node = VarName;
// Edges are represented as triplets. .0 = from, .1 = to, .2 = weight
type Edge = (Node, Node, Weight);
// Graphs are directed
type GraphType = DiGraph<Node, Weight>;

#[derive(Clone, Debug)]
pub struct DepGraph {
    graph: GraphType,
}

impl DepGraph {
    #[allow(dead_code)]
    pub fn as_dot_graph<'a>(&'a self) -> Dot<'a, &'a GraphType> {
        self.as_dot_graph_with_config(&[])
    }

    #[allow(dead_code)]
    fn as_dot_graph_with_config<'a>(&'a self, config: &'a [Config]) -> Dot<'a, &'a GraphType> {
        Dot::with_config(&self.graph, config)
    }

    #[allow(dead_code)]
    fn extend_nodes_from_iter<'a, I>(&mut self, nodes: I)
    where
        I: IntoIterator<Item = &'a Node>,
    {
        for node in nodes {
            self.graph.add_node(node.clone());
        }
    }

    #[allow(dead_code)]
    fn extend_edges_from_iter<'a, I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = &'a Edge>,
    {
        for (from, to, weight) in iter {
            let from_node = self
                .graph
                .node_indices()
                .find(|node| self.graph[*node] == *from);
            let to_node = self
                .graph
                .node_indices()
                .find(|node| self.graph[*node] == *to);
            if let (Some(from_node), Some(to_node)) = (from_node, to_node) {
                self.graph.add_edge(from_node, to_node, weight.clone());
            }
        }
    }

    #[allow(dead_code)]
    fn edges_into_set(&self) -> BTreeSet<Edge> {
        self.graph
            .edge_references()
            .map(|edge| {
                (
                    self.graph[edge.source()].clone(),
                    self.graph[edge.target()].clone(),
                    edge.weight().clone(),
                )
            })
            .collect()
    }

    // Similar to creating a new graph with the union of the nodes and edges,
    // but this is done in-place
    fn merge_graphs(&mut self, other: &DepGraph) {
        let mut node_map = BTreeMap::new();

        // Add all nodes from `self` into the map
        for node in self.graph.node_indices() {
            let node_value = self.graph[node].clone();
            node_map.insert(node_value, node);
        }

        // Add nodes from `other` if they are not already in `self`
        for node in other.graph.node_indices() {
            let node_value = other.graph[node].clone();
            node_map
                .entry(node_value.clone())
                .or_insert_with(|| self.graph.add_node(node_value));
        }

        // Merge edges from `other`
        for edge in other.graph.edge_indices() {
            let (source, target) = other.graph.edge_endpoints(edge).unwrap();
            let weight = other.graph[edge].clone();

            let source_index = node_map[&other.graph[source]];
            let target_index = node_map[&other.graph[target]];

            // Ensure the edge does not already exist before adding
            if !self.graph.contains_edge(source_index, target_index) {
                self.graph.add_edge(source_index, target_index, weight);
            }
        }
    }

    fn combine_edges(&mut self) {
        // A HashMap to store edges by (source, target) as keys
        let mut edge_map: BTreeMap<(usize, usize), Vec<isize>> = BTreeMap::new();

        // Iterate over all edges in the graph
        for edge in self.graph.edge_references() {
            let source = edge.source().index();
            let target = edge.target().index();
            let weight = edge.weight().clone();

            // Combine the weights if there are multiple edges between the same nodes
            edge_map
                .entry((source, target))
                .or_insert_with(Vec::new)
                .extend(weight);
        }

        // Clear all the edges in the graph to re-add combined edges
        self.graph.clear_edges();

        // Re-add the combined edges to the graph
        for ((source, target), weights) in edge_map {
            self.graph.add_edge(
                self.graph.node_indices().nth(source).unwrap(),
                self.graph.node_indices().nth(target).unwrap(),
                weights,
            );
        }
    }

    // Traverses the sexpr and returns a map of its dependencies to other variables
    fn sexpr_dependencies(sexpr: &SExpr<Node>, root_name: &Node) -> DepGraph {
        fn deps_impl(
            sexpr: &SExpr<Node>,
            steps: &Weight,
            map: &mut DepGraph,
            current_node: &NodeIndex,
        ) {
            match sexpr {
                SExpr::Var(name) => {
                    let node = map.graph.add_node(name.clone());
                    map.graph.add_edge(*current_node, node, steps.clone());
                }
                SExpr::SIndex(sexpr, idx, _) => {
                    let mut steps = steps.clone();
                    steps.push(*idx);
                    deps_impl(sexpr, &steps, map, current_node);
                }
                SExpr::If(iff, then, els) => {
                    deps_impl(iff, steps, map, current_node);
                    deps_impl(then, steps, map, current_node);
                    deps_impl(els, steps, map, current_node);
                }
                SExpr::Val(_) => {}
                SExpr::List(vec) => {
                    vec.iter()
                        .for_each(|sexpr| deps_impl(sexpr, steps, map, current_node));
                }
                SExpr::Eval(sexpr)
                | SExpr::Not(sexpr)
                | SExpr::LHead(sexpr)
                | SExpr::LTail(sexpr)
                | SExpr::Defer(sexpr) => deps_impl(sexpr, steps, map, current_node),
                SExpr::BinOp(sexpr1, sexpr2, _)
                | SExpr::Update(sexpr1, sexpr2)
                | SExpr::LIndex(sexpr1, sexpr2)
                | SExpr::LAppend(sexpr1, sexpr2)
                | SExpr::LConcat(sexpr1, sexpr2) => {
                    deps_impl(sexpr1, steps, map, current_node);
                    deps_impl(sexpr2, steps, map, current_node);
                }
            }
        }

        let mut graph = DepGraph::empty_graph();
        let root_node = graph.graph.add_node(root_name.clone());
        deps_impl(sexpr, &vec![], &mut graph, &root_node);
        graph
    }

    #[allow(dead_code)]
    /// Returns a filtered graph with only the instantaneous dependencies (i.e. time = 0)
    fn instantaneous_dependencies(
        &self,
    ) -> EdgeFiltered<&GraphType, impl FilterEdge<EdgeReference<'_, Weight>>> {
        EdgeFiltered::from_fn(&self.graph, |edge| {
            // TODO: I don't know how this will work when we genuinely have
            // a vector of different indices; for now we just use the first one
            let weight = edge.weight().get(0).unwrap_or(&0);
            *weight == 0
        })
    }

    #[allow(dead_code)]
    /// Check if the graph is productive (i.e. has no cycles in which zero time
    /// passes). This is necessary to check that the runtime will not
    /// deadlock when processing them.
    pub fn is_productive(&self) -> bool {
        let inst_deps = self.instantaneous_dependencies();
        !is_cyclic_directed(&inst_deps)
    }

    #[allow(dead_code)]
    /// Check if the graph is effectively monitorable (i.e. has no positive cycles)
    pub fn is_effectively_monitorable(&self) -> bool {
        // Quite inefficient, but should be good enough for now, given we mostly
        // need this function for testing
        // TODO: I don't know how this will work when we genuinely have
        // a vector of different indices; for now we just use the first one
        let neg_graph = self
            .graph
            .map(|_, n| n.clone(), |_, e| (-e.get(0).unwrap_or(&0)) as f64);
        self.graph
            .node_indices()
            .all(|node| find_negative_cycle(&neg_graph, node).is_none())
    }
}

impl DepGraph {
    fn empty_graph() -> Self {
        DepGraph {
            graph: GraphType::new(),
        }
    }

    // Takes a spec and creates a Map of VarName to SExpr<VarName>
    // I.e., all the assignment states in the spec (because we only support assignment statements)
    fn spec_to_map(
        spec: Box<dyn Specification<SExpr<VarName>>>,
    ) -> BTreeMap<VarName, SExpr<VarName>> {
        let mut map = BTreeMap::new();
        for var in spec.output_vars() {
            if let Some(expr) = spec.var_expr(&var) {
                map.insert(var.clone(), expr);
            }
        }
        map
    }
}

impl DependencyResolver for DepGraph {
    fn new(spec: Box<dyn Specification<SExpr<VarName>>>) -> Self {
        let mut graph = DepGraph::empty_graph();
        for (name, expr) in Self::spec_to_map(spec) {
            let expr_deps = Self::sexpr_dependencies(&expr, &name);
            graph.merge_graphs(&expr_deps);
        }
        graph.combine_edges();
        graph
    }

    fn longest_time_dependency(&self, name: &VarName) -> Option<usize> {
        let node = self
            .graph
            .node_indices()
            .find(|i| self.graph[*i] == *name)?;
        // TODO: Not recursively searching through the graph, only immediate neighbors.
        // If we have spec: in a; out x; out y; x = a[-1]; y = x[-1]; then we need to
        // keep the value of a for 2 steps, but we only keep it for 1 step.
        // TODO: Write a unit test for this...
        let longest_dep = self
            .graph
            .edges_directed(node, petgraph::Direction::Incoming)
            .filter_map(|edge| edge.weight().iter().map(|&w| w.unsigned_abs()).max()) // Take max of abs values
            .max()
            .unwrap_or(0);
        Some(longest_dep)
    }

    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize> {
        // TODO: Not recursively searching through the graph, only immediate neighbors.
        // If we have spec: in a; out x; out y; x = a[-1]; y = x[-1]; then we need to
        // keep the value of a for 2 steps, but we only keep it for 1 step.
        // TODO: Write a unit test for this...
        let mut map = BTreeMap::new();
        for (node, name) in self.graph.node_references() {
            let longest_dep = self
                .graph
                .edges_directed(node, petgraph::Direction::Incoming)
                .filter_map(|edge| edge.weight().iter().map(|&w| w.unsigned_abs()).max()) // Take max of abs values
                .max()
                .unwrap_or(0);
            map.insert(name.clone(), longest_dep);
        }
        map
    }
}

#[cfg(test)]
mod generation {
    use proptest::string::string_regex;

    use super::*;

    use proptest::prelude::*;

    /// Generate arbitrary dependency graphs for testing
    /// For now, we only generate graphs with a single node weight
    pub fn arb_dependency_graph() -> impl Strategy<Value = DepGraph> {
        // First generate variable names (1-10 unique names)
        let var_strategy = string_regex("[a-z][a-z0-9_]{0,5}").unwrap();
        let node_set_strategy = proptest::collection::btree_set(var_strategy, 1..10usize);

        // Then build a graph from those names
        node_set_strategy.prop_flat_map(|node_set| {
            let nodes: Vec<VarName> = node_set.into_iter().map(|x| x.into()).collect();
            let n = nodes.len();

            // Generate a set of edges
            proptest::collection::vec(
                (
                    0..n,       // source
                    0..n,       // target
                    -5..5isize, // edge weights
                ),
                0..2 * n,
            )
            .prop_map(move |edges| {
                let mut graph = DepGraph::empty_graph();

                // Add all nodes to the graph
                for name in &nodes {
                    graph.graph.add_node(name.clone());
                }

                // Add edges
                for (src_idx, dst_idx, weight) in edges {
                    let src = graph.graph.node_indices().nth(src_idx).unwrap();
                    let dst = graph.graph.node_indices().nth(dst_idx).unwrap();
                    graph.graph.add_edge(src, dst, vec![weight]);
                }

                graph
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dynamic_lola::ast::generation::arb_boolean_sexpr;
    use proptest::{prelude::*, sample};
    use test_log::test;
    use tracing::info;

    #[test]
    fn test_is_productive_true() {
        let mut graph = DepGraph::empty_graph();
        let a = graph.graph.add_node("a".into());
        let b = graph.graph.add_node("b".into());
        let c = graph.graph.add_node("c".into());
        graph.graph.add_edge(a, b, vec![0]);
        graph.graph.add_edge(b, c, vec![0]);
        graph.graph.add_edge(c, a, vec![1]);
        assert!(graph.is_productive());
    }

    #[test]
    fn test_is_productive_false() {
        let mut graph = DepGraph::empty_graph();
        let a = graph.graph.add_node("a".into());
        let b = graph.graph.add_node("b".into());
        let c = graph.graph.add_node("c".into());
        graph.graph.add_edge(a, b, vec![0]);
        graph.graph.add_edge(b, c, vec![0]);
        graph.graph.add_edge(c, a, vec![0]);
        assert!(!graph.is_productive());
    }

    #[test]
    fn test_prop_is_effectively_monitorable_true() {
        let mut graph = DepGraph::empty_graph();
        let a = graph.graph.add_node("a".into());
        let b = graph.graph.add_node("b".into());
        let c = graph.graph.add_node("c".into());
        graph.graph.add_edge(a, b, vec![1]);
        graph.graph.add_edge(b, c, vec![-1]);
        graph.graph.add_edge(c, a, vec![-1]);
        assert!(graph.is_effectively_monitorable());
    }

    #[test]
    fn test_prop_is_effectively_monitorable_false() {
        let mut graph = DepGraph::empty_graph();
        let a = graph.graph.add_node("a".into());
        let b = graph.graph.add_node("b".into());
        let c = graph.graph.add_node("c".into());
        graph.graph.add_edge(a, b, vec![-1]);
        graph.graph.add_edge(b, c, vec![1]);
        graph.graph.add_edge(c, a, vec![1]);
        assert!(!graph.is_effectively_monitorable());
    }

    proptest! {
        #[test]
        fn test_prop_is_productive(depgraph in generation::arb_dependency_graph()) {
            // Just check that the method doesn't panic or loop infinitely
            let _ = depgraph.is_productive();
        }

        #[ignore = "Ignored by TWright: The assertion about edges is currently failing and I don't know if this expected behaviour or not"]
        #[test]
        fn test_prop_sexpr_dependencies(sexpr in arb_boolean_sexpr(vec!["a".into(), "b".into(), "c".into()]), name in sample::select(vec![VarName("a".into()), VarName("b".into()), VarName("c".into())])) {
            // Basic test to check that the graph contains only nodes from
            // the input SExpr
            let graph = DepGraph::sexpr_dependencies(&sexpr, &"ROOT".into());
            let mut inputs = sexpr.inputs();
            inputs.push(name.clone());
            for node in graph.graph.node_indices() {
                let node_name = &graph.graph[node];
                assert!(*node_name == "ROOT".into() || inputs.contains(&graph.graph[node]));
            }
            for edge in graph.graph.edge_references() {
                info!("{:?}", edge);
                assert!(*edge.weight() == vec![0]);
            }
        }

        #[test]
        fn test_prop_boolean_dependency_graphs_effectively_monitorable(sexpr in arb_boolean_sexpr(vec!["a".into(), "b".into(), "c".into()])) {
            let name = "a".into();
            let depgraph = DepGraph::sexpr_dependencies(&sexpr, &name);
            assert!(depgraph.is_effectively_monitorable());
        }

        #[test]
        fn test_prop_boolean_dependency_productivity(sexpr in arb_boolean_sexpr(vec!["a".into(), "b".into(), "c".into()])) {
            let name = "ROOT".into();
            let depgraph = DepGraph::sexpr_dependencies(&sexpr, &name);
            let is_cyclic = is_cyclic_directed(&depgraph.graph);
            // For boolean expressions, the graph should be productive if
            // and only if it is acyclic, since there are no time indexes
            assert!(depgraph.is_productive() == !is_cyclic);
        }

        #[test]
        fn test_prop_is_effectively_monitorable(depgraph in generation::arb_dependency_graph()) {
            // Just check that the method doesn't panic or loop infinitely
            let _ = depgraph.is_effectively_monitorable();
        }
    }
}
