use std::collections::{BTreeMap, BTreeSet};

use crate::{SExpr, VarName};

use super::traits::DependencyResolver;

#[derive(Clone, Debug)]
pub struct Empty {
    names: BTreeSet<VarName>,
}

// A DependencyStore that simply saves the VarNames.
// It always returns time infinity for all dependencies.
impl DependencyResolver for Empty {
    fn new(spec: Box<dyn crate::Specification<SExpr<VarName>>>) -> Self {
        let mut names = BTreeSet::new();
        spec.output_vars().iter().for_each(|name| {
            names.insert(name.clone());
        });
        spec.input_vars().iter().for_each(|name| {
            names.insert(name.clone());
        });
        Self { names }
    }

    fn longest_time_dependency(&self, _: &VarName) -> Option<usize> {
        Some(usize::MAX)
    }

    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize> {
        self.names
            .iter()
            .map(|name| (name.clone(), usize::MAX))
            .collect()
    }
}
