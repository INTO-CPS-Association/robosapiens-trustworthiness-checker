//! Elaboration: the checked tree becomes the tree runtimes run.
//!
//! Elaboration rewrites every equation of a checked specification with the
//! contiguous-tree rewrite support, one destination subtree per checked node.
//! Each node it emits keeps the span and source context of the node it came
//! from and carries a type. For now the rewrite is the identity on node
//! kinds, so every node's type is the one checking gave it; lowerings and
//! type-directed rewrites extend it without any runtime seeing a node that
//! exists only before elaboration.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};

use super::ast::{
    AstShared, CheckedDsrvSpecification, CheckedExpr, CheckedExprRef, DsrvSpecification,
    ExprBuilder, ExprForestMap, ExprId,
};
use super::diagnostics::SemanticAnalysisReport;
use super::parser::DsrvParseError;
use super::pipeline::{TypeCheckMode, TypeCheckOptions};
use super::type_checker::TCType;
use crate::core::{
    RuntimeCapabilities, RuntimeCapabilityRequirement, Specification, StreamType, VarName,
};

/// A checked specification after elaboration: the tree every runtime runs,
/// with a type for each of its nodes. Only elaboration constructs one, from a
/// specification that passed strict or gradual checking, and every runtime
/// requires one; an evaluation strategy may ignore the types. Printing it
/// prints the source-level specification it was elaborated from.
#[derive(Clone, Debug)]
pub struct ElaboratedDsrvSpecification {
    source: CheckedDsrvSpecification,
    elaborated: CheckedDsrvSpecification,
}

impl ElaboratedDsrvSpecification {
    /// Parse, check and elaborate a specification. A parse failure stops
    /// before checking; otherwise the check is reported.
    pub fn parse_with(
        source: &str,
        options: TypeCheckOptions,
    ) -> Result<SemanticAnalysisReport<Self>, DsrvParseError> {
        CheckedDsrvSpecification::parse_with(source, options)
            .map(|report| report.map_checked(CheckedDsrvSpecification::elaborate))
    }

    /// The checked specification as written, before elaboration.
    pub fn source(&self) -> &CheckedDsrvSpecification {
        &self.source
    }

    /// The elaborated tree with the type of each of its nodes.
    pub fn checked(&self) -> &CheckedDsrvSpecification {
        &self.elaborated
    }

    /// The policy the specification was checked with, which elaboration and
    /// every later rewrite keep.
    pub fn check_mode(&self) -> TypeCheckMode {
        self.elaborated.check_mode()
    }

    pub fn var_expr_ref(&self, var: &VarName) -> Option<CheckedExprRef<'_>> {
        self.elaborated.var_expr_ref(var)
    }

    pub fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.elaborated.var_expr(var)
    }

    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.input_vars()
    }

    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.output_vars()
    }

    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.aux_vars()
    }

    pub fn type_annotations(&self) -> &BTreeMap<VarName, StreamType> {
        self.elaborated.type_annotations()
    }
}

impl ElaboratedDsrvSpecification {
    /// A rewrite of an elaborated tree that keeps every node's type, such as
    /// distributed localisation, is itself elaborated; it has no other source.
    pub(crate) fn from_rewritten(elaborated: CheckedDsrvSpecification) -> Self {
        let elaborated = elaborated.prepare_sites();
        Self {
            source: elaborated.clone(),
            elaborated,
        }
    }
}

impl CheckedDsrvSpecification {
    /// Elaborate this checked specification into the tree runtimes run.
    pub fn elaborate(self) -> ElaboratedDsrvSpecification {
        let elaborated = elaborate_tree(&self).prepare_sites();
        ElaboratedDsrvSpecification {
            source: self,
            elaborated,
        }
    }
}

impl DsrvSpecification {
    /// Check this specification with the requested policy, then elaborate it.
    pub fn check_and_elaborate(
        self,
        options: TypeCheckOptions,
    ) -> SemanticAnalysisReport<ElaboratedDsrvSpecification> {
        self.check(options)
            .map_checked(CheckedDsrvSpecification::elaborate)
    }
}

/// Rewrite every equation of `checked` into fresh storage, recording the type
/// of each emitted node in allocation order.
fn elaborate_tree(checked: &CheckedDsrvSpecification) -> CheckedDsrvSpecification {
    let spec = checked.unchecked();
    let node_count = spec.exprs.nodes().len();
    let mut builder = ExprBuilder::with_capacities(node_count, spec.exprs.len());
    let mut types: Vec<TCType> = Vec::with_capacity(node_count);
    let roots = spec.exprs.keys().map(|name| {
        checked
            .var_expr_ref(name)
            .expect("every equation of a checked specification is typed")
    });
    let roots = builder
        .try_rewrite_forest(roots, |mut node| {
            let source = node.source();
            let kind = node.source_node().rebuild(source.kind().clone());
            let id = node.alloc(kind, source.expr().metadata().clone())?;
            types.push(source.typ().clone());
            Ok::<_, contiguous_tree::BuildError<ExprId>>(id)
        })
        .expect("the identity rewrite keeps every node's children");
    let forest = builder
        .finish_forest(roots)
        .expect("elaboration emits one complete tree per equation");
    let exprs = ExprForestMap::new(spec.exprs.keys().cloned(), forest)
        .expect("elaboration keeps the sorted, unique equation names");

    let mut expr_types = exprs.annotations_builder();
    assert_eq!(
        types.len(),
        exprs.nodes().len(),
        "one type per emitted node"
    );
    for (node, typ) in exprs.nodes().zip(types) {
        expr_types
            .insert(node, typ)
            .expect("an emitted node belongs to the elaborated forest");
    }
    let expr_types = expr_types
        .finish()
        .expect("elaboration types every node it emits");

    let mut elaborated = DsrvSpecification::from_expression_forest_with_entries(
        spec.input_vars.clone(),
        spec.output_vars.clone(),
        exprs,
        spec.type_annotations.clone(),
        spec.aux_vars.iter().cloned(),
        spec.declarations.clone(),
    );
    elaborated.source_context = spec.source_context.clone();
    elaborated.sources = AstShared::clone(&spec.sources);
    CheckedDsrvSpecification::new(elaborated, expr_types, checked.check_mode())
}

impl Display for ElaboratedDsrvSpecification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Specification for ElaboratedDsrvSpecification {
    type Expr = CheckedExpr;

    fn first_unsupported_construct(
        &self,
        supported: RuntimeCapabilities,
    ) -> Option<RuntimeCapabilityRequirement> {
        Specification::first_unsupported_construct(&self.elaborated, supported)
    }

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.input_vars().clone()
    }
    fn input_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.input_vars_in_order()
    }
    fn output_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.output_vars().clone()
    }
    fn output_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.output_vars_in_order()
    }
    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.aux_vars().clone()
    }
    fn aux_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.aux_vars_in_order()
    }
    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.stream_vars().clone()
    }
    fn stream_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.stream_vars_in_order()
    }
    fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.elaborated.var_expr(var)
    }
    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.elaborated.type_annotations().clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::dsrv_fixtures::WithoutWarnings;
    use contiguous_tree::TreeCursorExt;

    use super::*;
    use crate::lang::dsrv::ast::AstShared;

    const SOURCE: &str = "type Pair = Struct<a: Int, b: Int>\n\
        in x: Int\n\
        in p: Pair\n\
        out y: Int\n\
        out z\n\
        aux w: Int\n\
        y = if x > 0 then p.a + w[1] else default(x[1], 0)\n\
        z = List.map(\\v -> v * 2, [x, p.b])\n\
        w = x + 1";

    #[test]
    fn elaboration_keeps_every_node_with_its_span_context_and_type() {
        for options in [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL] {
            let source = SOURCE.replace("out z\n", "out z: List<Int>\n");
            let checked = crate::dsrv_fixtures::checked_with(&source, options);
            let elaborated = checked.clone().elaborate();

            assert_eq!(elaborated.checked().unchecked(), checked.unchecked());
            for name in checked.unchecked().roots().map(|(name, _)| name) {
                let original = checked.var_expr_ref(name).unwrap();
                let rewritten = elaborated.var_expr_ref(name).unwrap();
                assert!(
                    !original.expr().shares_storage_with(rewritten.expr()),
                    "{name} is rewritten into fresh storage"
                );
                assert_eq!(original.postorder().count(), rewritten.postorder().count());
                for (before, after) in original.postorder().zip(rewritten.postorder()) {
                    assert!(before.kind().same_payload(after.kind()));
                    assert_eq!(before.expr().span(), after.expr().span());
                    let (before_context, after_context) = (
                        before.expr().metadata().context.as_ref().unwrap(),
                        after.expr().metadata().context.as_ref().unwrap(),
                    );
                    assert!(AstShared::ptr_eq(before_context, after_context));
                    assert_eq!(before.typ(), after.typ());
                }
            }
            assert!(AstShared::ptr_eq(
                elaborated.checked().unchecked().source_context(),
                checked.unchecked().source_context()
            ));
        }
    }

    #[test]
    fn printing_uses_the_source_level_specification() {
        let checked = crate::dsrv_fixtures::checked_with(SOURCE, TypeCheckOptions::GRADUAL);
        let printed = checked.to_string();
        assert_eq!(checked.elaborate().to_string(), printed);
    }

    #[test]
    fn only_a_checked_specification_is_elaborated() {
        let ill_typed = "out y: Bool\ny = 1".parse::<DsrvSpecification>().unwrap();
        assert!(
            ill_typed
                .clone()
                .check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
        assert!(
            ill_typed
                .check_and_elaborate(TypeCheckOptions::STRICT)
                .without_warnings()
                .is_err()
        );
    }
}
