use crate::{SExpr, Specification, VarName};
use enum_inner_method::enum_inner_method;
use std::collections::BTreeMap;
use std::fmt::Debug;
use strum_macros::EnumDiscriminants;
use strum_macros::EnumIter;

use super::{DepGraph, Empty};

#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(DependencyKind), derive(EnumIter))]
#[enum_inner_method (fn longest_time_dependency(&self, v: &VarName) -> Option<usize>)]
#[enum_inner_method (fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize>)]
#[enum_inner_method (fn add_dependency(&mut self, var: &VarName, sexpr: &SExpr<VarName>))]
#[enum_inner_method (fn remove_dependency(&mut self, var: &VarName, sexpr: &SExpr<VarName>))]
pub enum DependencyManager {
    Empty(Empty),
    DepGraph(DepGraph),
}

pub fn create_dependency_manager(
    kind: DependencyKind,
    spec: impl Specification<Expr = SExpr<VarName>>,
) -> DependencyManager {
    match kind {
        DependencyKind::Empty => DependencyManager::Empty(Empty::new(spec)),
        DependencyKind::DepGraph => DependencyManager::DepGraph(DepGraph::new(spec)),
    }
}

// Interface for resolving dependencies.
pub trait DependencyResolver: Send + Sync {
    // Generates the dependency structure from the given expressions
    fn new(spec: impl Specification<Expr = SExpr<VarName>>) -> Self;

    // Adds a new dependency to the resolver
    fn add_dependency(&mut self, var: &VarName, sexpr: &SExpr<VarName>);

    // Remove dependency to the resolver
    fn remove_dependency(&mut self, var: &VarName, sexpr: &SExpr<VarName>);

    // Returns how long the variable needs to be saved before it can be forgotten
    fn longest_time_dependency(&self, var: &VarName) -> Option<usize>;

    // Calls `longest_time_dependency` on all variables
    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize>;
}
