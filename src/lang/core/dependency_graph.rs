use std::collections::BTreeMap;

use petgraph::dot::{Config, Dot};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeIndex;
use petgraph::visit::{EdgeRef, IntoNodeReferences};

use crate::semantics::AsyncConfig;
use crate::{SExpr, Specification, VarName};

// Interface for resolving dependencies.
pub trait DependencyResolver<AC, S>
where
    AC: AsyncConfig,
    S: Specification<Expr = AC::Expr>,
{
    // Generates the dependency structure from the given expressions
    fn new(spec: S) -> Self;

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
    pub fn from_spec<S>(spec: S) -> Self
    where
        S: Specification<Expr = SExpr>,
    {
        let mut graph = Self::empty_graph();
        for var in spec.output_vars() {
            if let Some(expr) = spec.var_expr(&var) {
                let expr_deps = sexpr_dependencies(&expr, &var);
                graph.merge_graphs(&expr_deps);
            }
        }
        graph
    }

    pub fn resolver_from_sexpr_spec<AC, S>(spec: S) -> impl DependencyResolver<AC, S>
    where
        AC: AsyncConfig<Expr = SExpr>,
        S: Specification<Expr = AC::Expr>,
    {
        Self::from_spec(spec)
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

        // Add the edges:
        for edge in other.graph.edge_references() {
            let source_index = node_map[&other.graph[edge.source()]];
            let target_index = node_map[&other.graph[edge.target()]];
            self.graph
                .add_edge(source_index, target_index, *edge.weight());
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

// SExpr specific dependency resolution:
// Traverses the sexpr and returns a DepGraph of its dependencies to other variables
// NOTE: The graph returned here may have multiple edges to the same node.
// Can be combined by calling `combine_edges`. This is not done in this function for efficiency
fn sexpr_dependencies(sexpr: &SExpr, root_name: &Node) -> DepGraph {
    fn deps_impl(
        sexpr: &SExpr,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
    ) {
        match sexpr {
            SExpr::Var(name) => {
                let node = map.graph.add_node(name.clone());
                if steps.is_empty() {
                    map.graph.add_edge(*current_node, node, 0);
                } else {
                    steps.iter().for_each(|w| {
                        map.graph.add_edge(*current_node, node, *w);
                    });
                }
            }
            SExpr::SIndex(sexpr, idx) => {
                steps.push(*idx);
                deps_impl(sexpr, steps, map, current_node);
            }
            SExpr::If(iff, then, els) => {
                deps_impl(iff, steps, map, current_node);
                deps_impl(then, steps, map, current_node);
                deps_impl(els, steps, map, current_node);
            }
            SExpr::Val(_) | SExpr::MonitoredAt(_, _) | SExpr::Dist(_, _) => {}
            SExpr::List(vec) => {
                vec.iter()
                    .for_each(|sexpr| deps_impl(sexpr, steps, map, current_node));
            }
            SExpr::Map(m) => {
                m.iter()
                    .for_each(|(_, v)| deps_impl(v, steps, map, current_node));
            }
            SExpr::Dynamic(sexpr, _)
            | SExpr::RestrictedDynamic(sexpr, _, _)
            | SExpr::Not(sexpr)
            | SExpr::LHead(sexpr)
            | SExpr::LTail(sexpr)
            | SExpr::MGet(sexpr, _)
            | SExpr::MRemove(sexpr, _)
            | SExpr::MHasKey(sexpr, _)
            | SExpr::LLen(sexpr)
            | SExpr::IsDefined(sexpr)
            | SExpr::When(sexpr)
            | SExpr::Defer(sexpr, _, _)
            | SExpr::Sin(sexpr)
            | SExpr::Cos(sexpr)
            | SExpr::Tan(sexpr)
            | SExpr::Abs(sexpr) => deps_impl(sexpr, steps, map, current_node),
            SExpr::BinOp(sexpr1, sexpr2, _)
            | SExpr::Default(sexpr1, sexpr2)
            | SExpr::Update(sexpr1, sexpr2)
            | SExpr::LIndex(sexpr1, sexpr2)
            | SExpr::LAppend(sexpr1, sexpr2)
            | SExpr::LConcat(sexpr1, sexpr2)
            | SExpr::Latch(sexpr1, sexpr2)
            | SExpr::Init(sexpr1, sexpr2)
            | SExpr::MInsert(sexpr1, _, sexpr2) => {
                deps_impl(sexpr1, steps, map, current_node);
                deps_impl(sexpr2, steps, map, current_node);
            }
        }
    }

    let mut graph = DepGraph::empty_graph();
    let root_node = graph.graph.add_node(root_name.clone());
    deps_impl(sexpr, &mut vec![], &mut graph, &root_node);
    graph
}

impl<AC, S> DependencyResolver<AC, S> for DepGraph
where
    AC: AsyncConfig<Expr = SExpr>,
    S: Specification<Expr = AC::Expr>,
{
    fn new(spec: S) -> Self {
        DepGraph::from_spec(spec)
    }

    fn add_dependency(&mut self, var: &VarName, expr: &AC::Expr) {
        let expr_deps = sexpr_dependencies(expr, var);
        self.merge_graphs(&expr_deps);
    }

    fn remove_dependency(&mut self, name: &VarName, expr: &AC::Expr) {
        let expr_deps = sexpr_dependencies(expr, name);
        self.diff_graphs(&expr_deps);
    }

    fn longest_time_dependency(&self, name: &VarName) -> u64 {
        // Default = 0 if no dependencies is correct

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

    fn longest_time_dependencies(&self) -> BTreeMap<VarName, u64> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LOLASpecification;
    use crate::lang::core::parser::SpecParser;
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::dsrv_fixtures::TestConfig;

    fn test_parser(input: &mut &str) -> anyhow::Result<LOLASpecification> {
        <LALRParser as SpecParser<LOLASpecification>>::parse(input)
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

    fn get_graph(
        graph: impl DependencyResolver<TestConfig, LOLASpecification> + 'static,
    ) -> GraphType {
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
        let graph = DepGraph::from_spec(spec).graph;
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_graph_index_past() {
        let mut spec = specs()["single_inp_past"];
        let spec = test_parser(&mut spec).unwrap();
        let graph = DepGraph::from_spec(spec).graph;
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
        let graph = DepGraph::from_spec(spec).graph;
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
        let graph = DepGraph::from_spec(spec).graph;
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
        let graph = DepGraph::from_spec(spec).graph;
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
        let graph = DepGraph::from_spec(spec).graph;
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
        let graph = DepGraph::from_spec(spec).graph;
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
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        let expected = BTreeMap::from([("x".into(), 0)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_index_past() {
        let mut spec = specs()["single_inp_past"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_multi_out_past() {
        let mut spec = specs()["multi_out_past"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
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
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
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
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
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
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        assert_eq!(dep.longest_time_dependency(&"x".into()), 0);
        assert_eq!(dep.longest_time_dependency(&"a".into()), 1);
        let expected = BTreeMap::from([("x".into(), 0), ("a".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_time_recursion() {
        let mut spec = specs()["recursion"];
        let spec = test_parser(&mut spec).unwrap();
        let dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        assert_eq!(dep.longest_time_dependency(&"z".into()), 1);
        let expected = BTreeMap::from([("z".into(), 1)]);
        assert_eq!(dep.longest_time_dependencies(), expected);
    }

    #[test]
    fn test_add_dep_simple() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.add_dependency(&"new".into(), &SExpr::Val(42.into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_dep_new_edge() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.add_dependency(&"a".into(), &SExpr::Var("x".into()));
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
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.add_dependency(&"a".into(), &SExpr::Var("y".into()));
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
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.add_dependency(&"x".into(), &SExpr::Var("a".into()));
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
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.add_dependency(
            &"x".into(),
            &SExpr::SIndex(Box::new(SExpr::Var("a".into())), 1),
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
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.remove_dependency(&"y".into(), &SExpr::Var("x".into()));
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
        let mut dep = DepGraph::resolver_from_sexpr_spec::<TestConfig, _>(spec);
        dep.remove_dependency(&"x".into(), &SExpr::Var("a".into()));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        let a = find_node(&graph, "a");
        let x = find_node(&graph, "x");
        assert!(graph.contains_edge(x, a));
        let weight = get_weights(&graph, x, a);
        assert_eq!(weight, vec![1]);
    }
}
