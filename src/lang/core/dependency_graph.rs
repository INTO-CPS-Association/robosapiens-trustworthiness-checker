use std::collections::BTreeMap;

use petgraph::dot::{Config, Dot};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeIndex;
use petgraph::visit::{EdgeRef, IntoNodeReferences};
use tracing::debug;

use crate::lang::dsrv::ast::{SExpr, UntypedDsrvSpecification};
use crate::lang::dsrv::type_checker::{
    SExprAny, SExprBool, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
    TypedDsrvSpecification, TypedListExpr, TypedListExprKind, TypedMapExpr, TypedMapExprKind,
    TypedStructExpr, TypedStructExprKind,
};
use crate::semantics::AsyncConfig;
use crate::{Specification, VarName};

pub trait DependencyGraphExpr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph;
}

pub trait DependencyGraphSpec {
    fn dependency_graph(&self) -> DepGraph;
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
    pub fn from_sexprs<Expr>(exprs: BTreeMap<VarName, Expr>) -> Self
    where
        Expr: DependencyGraphExpr,
    {
        let mut graph = Self::empty_graph();
        for (var, expr) in exprs {
            let expr_deps = expr.dependency_graph_for_root(&var);
            graph.merge_graphs(&expr_deps);
        }
        debug!("Constructed dependency graph: {:?}", graph.as_dot_graph());
        graph
    }

    pub fn from_typed_sexprs(exprs: BTreeMap<VarName, SExprTE>) -> Self {
        let mut graph = Self::empty_graph();
        for (var, expr) in exprs {
            let expr_deps = typed_sexpr_dependencies(&expr, &var);
            graph.merge_graphs(&expr_deps);
        }
        debug!(
            "Constructed typed dependency graph: {:?}",
            graph.as_dot_graph()
        );
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
        current_idx: u64,
    ) {
        debug!(
            "Visiting {:?} with steps {:?} and current_idx {}",
            sexpr, steps, current_idx
        );
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
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_impl(sexpr, steps, map, current_node, new_idx);
            }
            SExpr::If(iff, then, els) => {
                deps_impl(iff, steps, map, current_node, current_idx);
                deps_impl(then, steps, map, current_node, current_idx);
                deps_impl(els, steps, map, current_node, current_idx);
            }
            SExpr::Val(_) | SExpr::MonitoredAt(_, _) | SExpr::Dist(_, _) => {}
            SExpr::List(vec) => {
                vec.iter()
                    .for_each(|sexpr| deps_impl(sexpr, steps, map, current_node, current_idx));
            }
            SExpr::Map(m) | SExpr::Struct(m) | SExpr::ObjectLiteral(m) => {
                m.iter()
                    .for_each(|(_, v)| deps_impl(v, steps, map, current_node, current_idx));
            }
            SExpr::Dynamic(sexpr, _)
            | SExpr::RestrictedDynamic(sexpr, _, _)
            | SExpr::Not(sexpr)
            | SExpr::LHead(sexpr)
            | SExpr::LTail(sexpr)
            | SExpr::MGet(sexpr, _)
            | SExpr::SGet(sexpr, _)
            | SExpr::MRemove(sexpr, _)
            | SExpr::MHasKey(sexpr, _)
            | SExpr::LLen(sexpr)
            | SExpr::IsDefined(sexpr)
            | SExpr::When(sexpr)
            | SExpr::Defer(sexpr, _, _)
            | SExpr::Sin(sexpr)
            | SExpr::Cos(sexpr)
            | SExpr::Tan(sexpr)
            | SExpr::Abs(sexpr) => deps_impl(sexpr, steps, map, current_node, current_idx),
            SExpr::BinOp(sexpr1, sexpr2, _)
            | SExpr::Default(sexpr1, sexpr2)
            | SExpr::Update(sexpr1, sexpr2)
            | SExpr::LIndex(sexpr1, sexpr2)
            | SExpr::LAppend(sexpr1, sexpr2)
            | SExpr::LConcat(sexpr1, sexpr2)
            | SExpr::Latch(sexpr1, sexpr2)
            | SExpr::Init(sexpr1, sexpr2)
            | SExpr::MInsert(sexpr1, _, sexpr2) => {
                // Need to clone on lhs to ensure that these dependencies are not shared with rhs
                deps_impl(sexpr1, &mut steps.clone(), map, current_node, current_idx);
                deps_impl(sexpr2, steps, map, current_node, current_idx);
            }
        }
    }

    debug!("sexr_dependencies for {}: {:?}", root_name, sexpr);
    let mut graph = DepGraph::empty_graph();
    let root_node = graph.graph.add_node(root_name.clone());
    deps_impl(sexpr, &mut vec![], &mut graph, &root_node, 0);
    graph
}

fn typed_sexpr_dependencies(expr: &SExprTE, root_name: &Node) -> DepGraph {
    fn add_var_dep(map: &mut DepGraph, current_node: &NodeIndex, name: &VarName, steps: &[Weight]) {
        let node = map.graph.add_node(name.clone());
        if steps.is_empty() {
            map.graph.add_edge(*current_node, node, 0);
        } else {
            steps.iter().for_each(|w| {
                map.graph.add_edge(*current_node, node, *w);
            });
        }
    }

    fn deps_te(
        expr: &SExprTE,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprTE::Int(e) => deps_int(e, steps, map, current_node, current_idx),
            SExprTE::Float(e) => deps_float(e, steps, map, current_node, current_idx),
            SExprTE::Str(e) => deps_str(e, steps, map, current_node, current_idx),
            SExprTE::Bool(e) => deps_bool(e, steps, map, current_node, current_idx),
            SExprTE::Unit(e) => deps_unit(e, steps, map, current_node, current_idx),
            SExprTE::List(e) => deps_list(e, steps, map, current_node, current_idx),
            SExprTE::Map(e) => deps_map(e, steps, map, current_node, current_idx),
            SExprTE::Struct(e) => deps_struct(e, steps, map, current_node, current_idx),
            SExprTE::Any(e) => deps_dyn(e, steps, map, current_node, current_idx),
        }
    }

    fn deps_dyn(
        expr: &SExprAny,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        _current_idx: u64,
    ) {
        match expr {
            SExprAny::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprAny::Val(_) | SExprAny::Expr(_) => {}
        }
    }

    fn deps_int(
        expr: &SExprInt,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprInt::Cast(e) => deps_te(e, steps, map, current_node, current_idx),
            SExprInt::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprInt::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_int(inner, steps, map, current_node, new_idx);
            }
            SExprInt::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_int(t, steps, map, current_node, current_idx);
                deps_int(e, steps, map, current_node, current_idx);
            }
            SExprInt::Val(_) => {}
            SExprInt::BinOp(a, b, _)
            | SExprInt::Default(a, b)
            | SExprInt::Update(a, b)
            | SExprInt::Latch(a, b)
            | SExprInt::Init(a, b) => {
                deps_int(a, &mut steps.clone(), map, current_node, current_idx);
                deps_int(b, steps, map, current_node, current_idx);
            }
            SExprInt::Abs(e) => deps_int(e, steps, map, current_node, current_idx),
            SExprInt::Defer(e, _, _)
            | SExprInt::Dynamic(e, _)
            | SExprInt::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            SExprInt::LLen(list) | SExprInt::LHeadList(list) => {
                deps_list(list, steps, map, current_node, current_idx)
            }
            SExprInt::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            SExprInt::MGetMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            SExprInt::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_float(
        expr: &SExprFloat,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprFloat::Cast(e) => deps_te(e, steps, map, current_node, current_idx),
            SExprFloat::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprFloat::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_float(inner, steps, map, current_node, new_idx);
            }
            SExprFloat::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_float(t, steps, map, current_node, current_idx);
                deps_float(e, steps, map, current_node, current_idx);
            }
            SExprFloat::Val(_) => {}
            SExprFloat::BinOp(a, b, _)
            | SExprFloat::Default(a, b)
            | SExprFloat::Update(a, b)
            | SExprFloat::Latch(a, b)
            | SExprFloat::Init(a, b) => {
                deps_float(a, &mut steps.clone(), map, current_node, current_idx);
                deps_float(b, steps, map, current_node, current_idx);
            }
            SExprFloat::Sin(e) | SExprFloat::Cos(e) | SExprFloat::Tan(e) | SExprFloat::Abs(e) => {
                deps_float(e, steps, map, current_node, current_idx)
            }
            SExprFloat::Defer(e, _, _)
            | SExprFloat::Dynamic(e, _)
            | SExprFloat::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            SExprFloat::LHeadList(list) => deps_list(list, steps, map, current_node, current_idx),
            SExprFloat::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            SExprFloat::MGetMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            SExprFloat::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_str(
        expr: &SExprStr,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprStr::Cast(e) => deps_te(e, steps, map, current_node, current_idx),
            SExprStr::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprStr::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_str(inner, steps, map, current_node, new_idx);
            }
            SExprStr::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_str(t, steps, map, current_node, current_idx);
                deps_str(e, steps, map, current_node, current_idx);
            }
            SExprStr::Val(_) => {}
            SExprStr::BinOp(a, b, _)
            | SExprStr::Default(a, b)
            | SExprStr::Update(a, b)
            | SExprStr::Latch(a, b)
            | SExprStr::Init(a, b) => {
                deps_str(a, &mut steps.clone(), map, current_node, current_idx);
                deps_str(b, steps, map, current_node, current_idx);
            }
            SExprStr::Defer(e, _, _)
            | SExprStr::Dynamic(e, _)
            | SExprStr::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            SExprStr::LHeadList(list) => deps_list(list, steps, map, current_node, current_idx),
            SExprStr::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            SExprStr::MGetMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            SExprStr::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_bool(
        expr: &SExprBool,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprBool::Cast(e) => deps_te(e, steps, map, current_node, current_idx),
            SExprBool::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprBool::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_bool(inner, steps, map, current_node, new_idx);
            }
            SExprBool::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_bool(t, steps, map, current_node, current_idx);
                deps_bool(e, steps, map, current_node, current_idx);
            }
            SExprBool::Val(_) => {}
            SExprBool::Cmp(_, a, b) => {
                deps_te(a, &mut steps.clone(), map, current_node, current_idx);
                deps_te(b, steps, map, current_node, current_idx);
            }
            SExprBool::BinOp(a, b, _)
            | SExprBool::Default(a, b)
            | SExprBool::Update(a, b)
            | SExprBool::Latch(a, b)
            | SExprBool::Init(a, b) => {
                deps_bool(a, &mut steps.clone(), map, current_node, current_idx);
                deps_bool(b, steps, map, current_node, current_idx);
            }
            SExprBool::Not(e) => deps_bool(e, steps, map, current_node, current_idx),
            SExprBool::IsDefined(e) | SExprBool::When(e) => {
                deps_te(e, steps, map, current_node, current_idx)
            }
            SExprBool::LHeadList(list) => deps_list(list, steps, map, current_node, current_idx),
            SExprBool::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            SExprBool::MGetMap(map_expr, _) | SExprBool::MHasKeyMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            SExprBool::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
            SExprBool::Defer(e, _, _)
            | SExprBool::Dynamic(e, _)
            | SExprBool::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_unit(
        expr: &SExprUnit,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match expr {
            SExprUnit::Cast(e) => deps_te(e, steps, map, current_node, current_idx),
            SExprUnit::Var(name) => add_var_dep(map, current_node, name, steps),
            SExprUnit::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_unit(inner, steps, map, current_node, new_idx);
            }
            SExprUnit::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_unit(t, steps, map, current_node, current_idx);
                deps_unit(e, steps, map, current_node, current_idx);
            }
            SExprUnit::Val(_) => {}
            SExprUnit::Default(a, b)
            | SExprUnit::Update(a, b)
            | SExprUnit::Latch(a, b)
            | SExprUnit::Init(a, b) => {
                deps_unit(a, &mut steps.clone(), map, current_node, current_idx);
                deps_unit(b, steps, map, current_node, current_idx);
            }
            SExprUnit::Defer(e, _, _)
            | SExprUnit::Dynamic(e, _)
            | SExprUnit::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            SExprUnit::LHeadList(list) => deps_list(list, steps, map, current_node, current_idx),
            SExprUnit::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            SExprUnit::MGetMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            SExprUnit::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_list(
        expr: &TypedListExpr,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match &expr.kind {
            TypedListExprKind::Var(name) => add_var_dep(map, current_node, name, steps),
            TypedListExprKind::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_list(inner, steps, map, current_node, new_idx);
            }
            TypedListExprKind::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_list(t, steps, map, current_node, current_idx);
                deps_list(e, steps, map, current_node, current_idx);
            }
            TypedListExprKind::Default(a, b)
            | TypedListExprKind::Update(a, b)
            | TypedListExprKind::Latch(a, b)
            | TypedListExprKind::Init(a, b)
            | TypedListExprKind::LConcat(a, b) => {
                deps_list(a, &mut steps.clone(), map, current_node, current_idx);
                deps_list(b, steps, map, current_node, current_idx);
            }
            TypedListExprKind::Defer(e, _, _)
            | TypedListExprKind::Dynamic(e, _)
            | TypedListExprKind::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            TypedListExprKind::Literal(exprs) => exprs
                .iter()
                .for_each(|e| deps_te(e, steps, map, current_node, current_idx)),
            TypedListExprKind::LTail(e) | TypedListExprKind::LHeadList(e) => {
                deps_list(e, steps, map, current_node, current_idx)
            }
            TypedListExprKind::LAppend(list, elem) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_te(elem, steps, map, current_node, current_idx);
            }
            TypedListExprKind::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
            TypedListExprKind::MGetMap(map_expr, _) => {
                deps_map(map_expr, steps, map, current_node, current_idx)
            }
            TypedListExprKind::SGetStruct(struct_expr, _) => {
                deps_struct(struct_expr, steps, map, current_node, current_idx)
            }
        }
    }

    fn deps_map(
        expr: &TypedMapExpr,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match &expr.kind {
            TypedMapExprKind::Var(name) => add_var_dep(map, current_node, name, steps),
            TypedMapExprKind::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_map(inner, steps, map, current_node, new_idx);
            }
            TypedMapExprKind::Literal(entries) => entries
                .values()
                .for_each(|e| deps_te(e, steps, map, current_node, current_idx)),
            TypedMapExprKind::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_map(t, steps, map, current_node, current_idx);
                deps_map(e, steps, map, current_node, current_idx);
            }
            TypedMapExprKind::Default(a, b)
            | TypedMapExprKind::Update(a, b)
            | TypedMapExprKind::Latch(a, b)
            | TypedMapExprKind::Init(a, b) => {
                deps_map(a, &mut steps.clone(), map, current_node, current_idx);
                deps_map(b, steps, map, current_node, current_idx);
            }
            TypedMapExprKind::Defer(e, _, _)
            | TypedMapExprKind::Dynamic(e, _)
            | TypedMapExprKind::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            TypedMapExprKind::MInsert(map_expr, _, value) => {
                deps_map(map_expr, &mut steps.clone(), map, current_node, current_idx);
                deps_te(value, steps, map, current_node, current_idx);
            }
            TypedMapExprKind::MRemove(e, _) | TypedMapExprKind::MGetMap(e, _) => {
                deps_map(e, steps, map, current_node, current_idx)
            }
            TypedMapExprKind::SGetStruct(e, _) => {
                deps_struct(e, steps, map, current_node, current_idx)
            }
            TypedMapExprKind::LHeadList(e) => deps_list(e, steps, map, current_node, current_idx),
            TypedMapExprKind::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
        }
    }

    fn deps_struct(
        expr: &TypedStructExpr,
        steps: &mut Vec<Weight>,
        map: &mut DepGraph,
        current_node: &NodeIndex,
        current_idx: u64,
    ) {
        match &expr.kind {
            TypedStructExprKind::Var(name) => add_var_dep(map, current_node, name, steps),
            TypedStructExprKind::SIndex(inner, idx) => {
                let new_idx = current_idx + *idx;
                steps.push(new_idx);
                deps_struct(inner, steps, map, current_node, new_idx);
            }
            TypedStructExprKind::Literal(entries) => entries
                .iter()
                .for_each(|(_, e)| deps_te(e, steps, map, current_node, current_idx)),
            TypedStructExprKind::If(c, t, e) => {
                deps_bool(c, steps, map, current_node, current_idx);
                deps_struct(t, steps, map, current_node, current_idx);
                deps_struct(e, steps, map, current_node, current_idx);
            }
            TypedStructExprKind::Default(a, b)
            | TypedStructExprKind::Update(a, b)
            | TypedStructExprKind::Latch(a, b)
            | TypedStructExprKind::Init(a, b) => {
                deps_struct(a, &mut steps.clone(), map, current_node, current_idx);
                deps_struct(b, steps, map, current_node, current_idx);
            }
            TypedStructExprKind::Defer(e, _, _)
            | TypedStructExprKind::Dynamic(e, _)
            | TypedStructExprKind::RestrictedDynamic(e, _, _) => {
                deps_str(e, steps, map, current_node, current_idx)
            }
            TypedStructExprKind::SUpdate(struct_expr, _, value) => {
                deps_struct(
                    struct_expr,
                    &mut steps.clone(),
                    map,
                    current_node,
                    current_idx,
                );
                deps_te(value, steps, map, current_node, current_idx);
            }
            TypedStructExprKind::SGet(e, _) => {
                deps_struct(e, steps, map, current_node, current_idx)
            }
            TypedStructExprKind::MGetMap(e, _) => {
                deps_map(e, steps, map, current_node, current_idx)
            }
            TypedStructExprKind::LHeadList(e) => {
                deps_list(e, steps, map, current_node, current_idx)
            }
            TypedStructExprKind::LIndexList(list, idx) => {
                deps_list(list, &mut steps.clone(), map, current_node, current_idx);
                deps_int(idx, steps, map, current_node, current_idx);
            }
        }
    }

    debug!("typed_sexpr_dependencies for {}: {:?}", root_name, expr);
    let mut graph = DepGraph::empty_graph();
    let root_node = graph.graph.add_node(root_name.clone());
    deps_te(expr, &mut vec![], &mut graph, &root_node, 0);
    graph
}

impl DependencyGraphExpr for SExpr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph {
        sexpr_dependencies(self, root)
    }
}

impl DependencyGraphExpr for crate::lang::dsrv::ast::SpannedExpr {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph {
        self.node.dependency_graph_for_root(root)
    }
}

impl DependencyGraphExpr for SExprTE {
    fn dependency_graph_for_root(&self, root: &VarName) -> DepGraph {
        typed_sexpr_dependencies(self, root)
    }
}

impl DependencyGraphSpec for UntypedDsrvSpecification {
    fn dependency_graph(&self) -> DepGraph {
        let exprs = self
            .output_vars()
            .into_iter()
            .filter_map(|var| self.var_expr(&var).map(|expr| (var.clone(), expr.clone())))
            .collect();
        DepGraph::from_sexprs(exprs)
    }
}

impl DependencyGraphSpec for TypedDsrvSpecification {
    fn dependency_graph(&self) -> DepGraph {
        let exprs = self
            .output_vars()
            .into_iter()
            .filter_map(|var| self.var_expr(&var).map(|expr| (var.clone(), expr.clone())))
            .collect();
        DepGraph::from_typed_sexprs(exprs)
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
    use crate::UntypedDsrvSpecification;
    use crate::dsrv_fixtures::*;
    use crate::lang::core::parser::SpecParser;
    use crate::lang::dsrv::ast::SpannedExpr;
    use crate::lang::dsrv::lalr_parser::LALRParser;

    fn test_parser(input: &mut &str) -> anyhow::Result<UntypedDsrvSpecification> {
        <LALRParser as SpecParser<UntypedDsrvSpecification>>::parse(input)
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
        dep.add_dependency(&"new".into(), &SpannedExpr::Val(42));
        let graph = get_graph(dep);
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_dep_new_edge() {
        let mut spec = specs()["single_no_inp"];
        let spec = test_parser(&mut spec).unwrap();
        let mut dep = DepGraph::resolver_from_spec::<TestConfig>(spec);
        dep.add_dependency(&"a".into(), &SpannedExpr::Var("x".into()));
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
        dep.add_dependency(&"a".into(), &SpannedExpr::Var("y".into()));
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
        dep.add_dependency(&"x".into(), &SpannedExpr::Var("a".into()));
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
            &SpannedExpr::SIndex(Box::new(SpannedExpr::Var("a".into())), 1),
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
        dep.remove_dependency(&"y".into(), &SpannedExpr::Var("x".into()));
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
        dep.remove_dependency(&"x".into(), &SpannedExpr::Var("a".into()));
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
}
