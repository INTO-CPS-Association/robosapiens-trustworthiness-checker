use std::collections::BTreeMap;

use petgraph::graph::DiGraph;

use crate::{SExpr, VarName};

use super::traits::DependencyStore;

#[derive(Default)]
pub struct Empty {
    _empty: DiGraph<VarName, Vec<isize>>,
}

impl Empty {
    pub fn new() -> Box<dyn DependencyStore> {
        Box::new(Empty {
            _empty: DiGraph::new(),
        })
    }
}

impl DependencyStore for Empty {
    fn generate_dependencies(&self, _: BTreeMap<VarName, SExpr<VarName>>) {}

    fn container(&self) -> &DiGraph<VarName, Vec<isize>> {
        &self._empty
    }
}
