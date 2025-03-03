use std::collections::BTreeMap;

use crate::{SExpr, VarName};

use super::traits::DependencyStore;

#[derive(Default)]
pub struct Empty {}

impl Empty {}

impl DependencyStore for Empty {
    fn longest_time_dependency(&self, _: &VarName) -> Option<usize> {
        None
    }

    fn longest_time_dependencies(&self) -> BTreeMap<VarName, usize> {
        BTreeMap::new()
    }

    fn new(_: Box<dyn crate::Specification<SExpr<VarName>>>) -> Box<dyn DependencyStore>
    where
        Self: Sized,
    {
        Box::new(Empty {})
    }
}
