//! Per-root concrete bindings and unpublished node annotations.
use super::StreamTypes;
use super::diagnostics::WarningCollector;
use crate::core::StreamType;
use crate::{
    VarName,
    lang::dsrv::ast::{ExprId, ExprRef},
};
use imbl::OrdMap;
pub(super) struct TypeContext<'a> {
    pub(super) environment: &'a StreamTypes,
    pub(super) local_bindings: OrdMap<VarName, StreamType>,
    pub(super) expr_types: OrdMap<ExprId, StreamType>,
    pub(super) warnings: &'a mut WarningCollector,
}
impl<'a> TypeContext<'a> {
    pub(super) fn new(environment: &'a StreamTypes, warnings: &'a mut WarningCollector) -> Self {
        Self {
            environment,
            local_bindings: OrdMap::new(),
            expr_types: OrdMap::new(),
            warnings,
        }
    }
    pub(super) fn lookup(&self, name: &VarName) -> Option<StreamType> {
        self.local_bindings
            .get(name)
            .or_else(|| self.environment.get(name))
            .cloned()
    }
    pub(super) fn record(&mut self, expr: ExprRef<'_>, typ: &StreamType) {
        self.expr_types.insert(expr.id(), typ.clone());
    }
}
