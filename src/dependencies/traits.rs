use crate::{SExpr, Specification, VarName};
use enum_inner_method::enum_inner_method;
use std::collections::BTreeMap;
use std::fmt::Debug;
use strum_macros::EnumDiscriminants;

use super::{DepGraph, Empty};

#[derive(Clone, Debug, EnumDiscriminants)]
#[strum_discriminants(name(DependencyKind))]
#[enum_inner_method (fn longest_time_dependency(&self, v: &VarName) -> Option<usize>)]
#[enum_inner_method (fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize>)]
pub enum DependencyManager {
    Empty(Empty),
    DepGraph(DepGraph),
}

pub fn create_dependency_manager(
    kind: DependencyKind,
    spec: Box<dyn Specification<SExpr<VarName>>>,
) -> DependencyManager {
    match kind {
        DependencyKind::Empty => DependencyManager::Empty(Empty::new(spec)),
        DependencyKind::DepGraph => DependencyManager::DepGraph(DepGraph::new(spec)),
    }
}

// Interface for resolving dependencies.
pub trait DependencyResolver: Send + Sync {
    // TODO: Add dependency, get dependency

    // Generates the dependency structure from the given expressions
    fn new(spec: Box<dyn Specification<SExpr<VarName>>>) -> Self;

    // Returns how long the variable needs to be saved before it can be forgotten
    fn longest_time_dependency(&self, var: &VarName) -> Option<usize>;

    // Calls `longest_time_dependency` on all variables
    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize>;
}
