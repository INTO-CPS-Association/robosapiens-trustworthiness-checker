use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

use petgraph::dot::{Config, Dot};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

use crate::{SExpr, VarName};

type GraphType = DiGraph<VarName, isize>;

#[derive(Debug, Clone)]
pub struct DepGraph {
    graph: GraphType,
    pub time_required: BTreeMap<VarName, usize>,
}

impl DepGraph {
    #[allow(dead_code)]
    pub fn as_dot_graph<'a>(&'a self) -> Dot<'a, &'a GraphType> {
        self.as_dot_graph_with_config(&[])
    }

    #[allow(dead_code)]
    pub fn as_dot_graph_with_config<'a>(&'a self, config: &'a [Config]) -> Dot<'a, &'a GraphType> {
        Dot::with_config(&self.graph, config)
    }

    pub fn extend_nodes_from_set(&mut self, nodes: BTreeSet<VarName>) {
        for node in nodes {
            self.graph.add_node(node);
        }
    }

    pub fn extend_edges_from_set(&mut self, edges: BTreeSet<(VarName, VarName, isize)>) {
        for (from, to, weight) in edges {
            let from_node = self
                .graph
                .node_indices()
                .find(|node| self.graph[*node] == from);
            let to_node = self
                .graph
                .node_indices()
                .find(|node| self.graph[*node] == to);
            if let (Some(from_node), Some(to_node)) = (from_node, to_node) {
                self.graph.add_edge(from_node, to_node, weight);
            }
        }
    }

    pub fn edges_into_set(&self) -> BTreeSet<(VarName, VarName, isize)> {
        self.graph
            .edge_references()
            .map(|edge| {
                (
                    self.graph[edge.source()].clone(),
                    self.graph[edge.target()].clone(),
                    *edge.weight(),
                )
            })
            .collect()
    }

    pub fn merge_graphs(&mut self, other: &DepGraph) {
        // TODO: Perhaps make this mutate self instead
        // TODO: Make add_*_from_set work on iterables
        let mut merged: DepGraph = DepGraph::new();
        let g1_nvals: BTreeSet<VarName> =
            self.node_indices().map(|node| self[node].clone()).collect();
        let g2_nvals: BTreeSet<VarName> = other
            .node_indices()
            .map(|node| other[node].clone())
            .collect();
        Self::extend_nodes_from_set(&mut merged, g1_nvals.union(&g2_nvals).cloned().collect());

        let g1_edges = Self::edges_into_set(self);
        let g2_edges = Self::edges_into_set(other);
        Self::extend_edges_from_set(&mut merged, g1_edges.union(&g2_edges).cloned().collect());
        self.graph = merged.graph;
    }

    // Traverses the sexpr and returns a map of its dependencies to other variables
    fn sexpr_dependencies(sexpr: &SExpr<VarName>, root_name: &VarName) -> DepGraph {
        fn deps_impl(
            sexpr: &SExpr<VarName>,
            steps: isize,
            map: &mut DepGraph,
            current_node: &NodeIndex,
        ) {
            match sexpr {
                SExpr::Var(name) => {
                    let node = map.add_node(name.clone());
                    map.add_edge(*current_node, node, steps);
                }
                SExpr::SIndex(sexpr, idx, _) => {
                    deps_impl(sexpr, *idx, map, current_node);
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

        let mut graph = DepGraph::new();
        let root_node = graph.add_node(root_name.clone());
        deps_impl(sexpr, 0, &mut graph, &root_node);
        graph
    }

    // Traverses the sexpr and returns a Map of the time required to save all variables involved
    // (in absolute time)
    fn sexpr_time_required(sexpr: &SExpr<VarName>) -> BTreeMap<VarName, usize> {
        fn time_req_impl(sexpr: &SExpr<VarName>, steps: usize, map: &mut BTreeMap<VarName, usize>) {
            match sexpr {
                SExpr::Var(name) => {
                    map.entry(name.clone())
                        .and_modify(|existing_depth| *existing_depth = (*existing_depth).max(steps))
                        .or_insert(steps);
                }
                SExpr::SIndex(sexpr, idx, _) => {
                    time_req_impl(sexpr, steps + idx.unsigned_abs(), map);
                }
                SExpr::If(iff, then, els) => {
                    time_req_impl(iff, steps, map);
                    time_req_impl(then, steps, map);
                    time_req_impl(els, steps, map);
                }
                SExpr::Val(_) => {}
                SExpr::List(vec) => {
                    vec.iter()
                        .for_each(|sexpr| time_req_impl(sexpr, steps, map));
                }
                SExpr::Eval(sexpr)
                | SExpr::Not(sexpr)
                | SExpr::LHead(sexpr)
                | SExpr::LTail(sexpr)
                | SExpr::Defer(sexpr) => time_req_impl(sexpr, steps, map),
                SExpr::BinOp(sexpr1, sexpr2, _)
                | SExpr::Update(sexpr1, sexpr2)
                | SExpr::LIndex(sexpr1, sexpr2)
                | SExpr::LAppend(sexpr1, sexpr2)
                | SExpr::LConcat(sexpr1, sexpr2) => {
                    time_req_impl(sexpr1, steps, map);
                    time_req_impl(sexpr2, steps, map);
                }
            }
        }

        let mut map = BTreeMap::new();
        time_req_impl(sexpr, 0, &mut map);
        map
    }

    pub fn generate_dependencies(&mut self, exprs: &BTreeMap<VarName, SExpr<VarName>>) {
        fn merge_max(map1: &mut BTreeMap<VarName, usize>, map2: BTreeMap<VarName, usize>) {
            for (key, value) in map2 {
                map1.entry(key)
                    .and_modify(|existing| *existing = (*existing).max(value))
                    .or_insert(value);
            }
        }
        // Merge map prioritizing the largest value during conflicts
        // (largest value in this case means expressions going further back in history)
        self.graph = GraphType::new();
        self.time_required = BTreeMap::new();
        for (name, expr) in exprs {
            // Add to dependency graph
            let expr_deps = Self::sexpr_dependencies(expr, name);
            self.merge_graphs(&expr_deps);

            // Add to time graph
            let mut sexpr_times = Self::sexpr_time_required(expr);
            // Add time requirement to lhs of the expression, by adding a dependency on itself to the longest dependency.
            // (Since all our statements in the language are assignments)
            let max_dep = sexpr_times.values().max().cloned().unwrap_or(0);
            sexpr_times.insert(name.clone(), max_dep);
            // Merge with global dependencies
            merge_max(&mut self.time_required, sexpr_times);
        }
    }
}

impl Deref for DepGraph {
    type Target = GraphType;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl DerefMut for DepGraph {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}

impl DepGraph {
    pub fn new() -> Self {
        Self {
            graph: GraphType::new(),
            time_required: BTreeMap::new(),
        }
    }
}

impl Default for DepGraph {
    fn default() -> Self {
        Self::new()
    }
}
