use std::collections::BTreeMap;

use petgraph::graph::DiGraph;

use crate::{SExpr, VarName};

// Interface for a dependency store.
// @types `Identifier` and `Data` are used to define the type of the dependency identifiers and the data associated with each dependency connection.
// E.g., a collection of time indices like in LOLA paper
// @types `Container` is the type of the container that holds the dependencies.
pub trait DependencyStore: Send + Sync {
    // TODO: Add dependency, get dependency
    fn generate_dependencies(&self, exprs: BTreeMap<VarName, SExpr<VarName>>);

    fn container(&self) -> &DiGraph<VarName, Vec<isize>>;
}
