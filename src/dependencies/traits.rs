use std::collections::BTreeMap;

use crate::{SExpr, Specification, VarName};

// Interface for a dependency store.
pub trait DependencyStore: Send + Sync {
    // TODO: Add dependency, get dependency

    // Generates the dependency graph from the given expressions
    fn new(spec: Box<dyn Specification<SExpr<VarName>>>) -> Box<dyn DependencyStore>
    where
        Self: Sized;

    // Returns how long the variable needs to be saved for before it can be forgotten
    fn longest_time_dependency(&self, var: &VarName) -> Option<usize>;

    // Calls `longest_time_dependency` on all variables
    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize>;
}
