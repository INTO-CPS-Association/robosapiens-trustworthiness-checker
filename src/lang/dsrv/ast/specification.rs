//! Checked and unchecked DSRV specifications.

use contiguous_tree::TreeCursorExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::marker::PhantomData;

use super::checked::{CheckedTypes, ExprTypes};
use super::{
    AstShared, CheckedExpr, CheckedExprRef, Expr, ExprBuilder, ExprForest, ExprForestMap, ExprRef,
};
use crate::core::{Specification, StreamType, VarName};
use crate::lang::dsrv::span::Span;

/// A declaration-level error in a forest-backed DSRV syntax tree.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DsrvAstError {
    #[error(
        "stream {variable} is assigned more than once (first assignment at {first:?}, duplicate at {duplicate:?})"
    )]
    DuplicateAssignment {
        variable: VarName,
        first: Span,
        duplicate: Span,
    },

    #[error("expression contains duplicate field {field:?}")]
    DuplicateExpressionField { field: ecow::EcoString },

    #[error("invalid expression forest: {0}")]
    InvalidExpressionForest(#[from] contiguous_tree::ForestError),

    #[error("invalid expression map: {0}")]
    InvalidExpressionMap(#[from] contiguous_tree::ForestMapError),
}

#[derive(Clone, Debug)]
pub enum SemanticEntry {
    Input {
        name: VarName,
        annotation: Option<StreamType>,
        span: Span,
    },
    Output {
        name: VarName,
        annotation: Option<StreamType>,
        span: Span,
    },
    Aux {
        name: VarName,
        annotation: Option<StreamType>,
        span: Span,
    },
    Assignment {
        name: VarName,
        span: Span,
    },
}

impl SemanticEntry {
    pub fn name(&self) -> &VarName {
        match self {
            Self::Input { name, .. }
            | Self::Output { name, .. }
            | Self::Aux { name, .. }
            | Self::Assignment { name, .. } => name,
        }
    }

    pub fn annotation(&self) -> Option<&StreamType> {
        match self {
            Self::Input { annotation, .. }
            | Self::Output { annotation, .. }
            | Self::Aux { annotation, .. } => annotation.as_ref(),
            Self::Assignment { .. } => None,
        }
    }

    pub fn span(&self) -> Span {
        match self {
            Self::Input { span, .. }
            | Self::Output { span, .. }
            | Self::Aux { span, .. }
            | Self::Assignment { span, .. } => *span,
        }
    }
}

// Source coordinates diagnose an entry but are not part of its semantic identity.
impl PartialEq for SemanticEntry {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Input {
                    name: a,
                    annotation: aa,
                    ..
                },
                Self::Input {
                    name: b,
                    annotation: ba,
                    ..
                },
            )
            | (
                Self::Output {
                    name: a,
                    annotation: aa,
                    ..
                },
                Self::Output {
                    name: b,
                    annotation: ba,
                    ..
                },
            )
            | (
                Self::Aux {
                    name: a,
                    annotation: aa,
                    ..
                },
                Self::Aux {
                    name: b,
                    annotation: ba,
                    ..
                },
            ) => a == b && aa == ba,
            (Self::Assignment { name: a, .. }, Self::Assignment { name: b, .. }) => a == b,
            _ => false,
        }
    }
}
impl Eq for SemanticEntry {}

/// A forest-backed specification whose declaration-level invariants are unchecked.
pub(crate) struct UnvalidatedDsrvSpecification {
    input_vars: BTreeSet<VarName>,
    output_vars: BTreeSet<VarName>,
    aux_vars: Vec<VarName>,
    expressions: ExprForest,
    entries: Vec<SemanticEntry>,
    type_annotations: BTreeMap<VarName, StreamType>,
}

impl UnvalidatedDsrvSpecification {
    pub(crate) fn new(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        aux_vars: Vec<VarName>,
        expressions: ExprForest,
        entries: Vec<SemanticEntry>,
        type_annotations: BTreeMap<VarName, StreamType>,
    ) -> Self {
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, SemanticEntry::Assignment { .. }))
                .count(),
            expressions.len(),
            "each assignment declaration must describe one expression root"
        );
        Self {
            input_vars,
            output_vars,
            aux_vars,
            expressions,
            entries,
            type_annotations,
        }
    }

    pub(crate) fn validate(self) -> Result<DsrvSpecification, DsrvAstError> {
        let assignments = self
            .entries
            .iter()
            .filter(|entry| matches!(entry, SemanticEntry::Assignment { .. }))
            .collect::<Vec<_>>();
        let names = assignments.iter().map(|entry| entry.name().clone());
        let exprs = match ExprForestMap::from_unsorted(names, self.expressions) {
            Ok(exprs) => exprs,
            Err(contiguous_tree::ForestMapError::DuplicateKey {
                first_index,
                duplicate_index,
            }) => {
                let first = assignments[first_index];
                let duplicate = assignments[duplicate_index];
                return Err(DsrvAstError::DuplicateAssignment {
                    variable: duplicate.name().clone(),
                    first: first.span(),
                    duplicate: duplicate.span(),
                });
            }
            Err(error) => return Err(DsrvAstError::InvalidExpressionMap(error)),
        };

        if let Some(field) = exprs
            .nodes()
            .find_map(|expression| expression.kind().duplicate_key().cloned())
        {
            return Err(DsrvAstError::DuplicateExpressionField { field });
        }

        Ok(DsrvSpecification::from_expression_forest_with_entries(
            self.input_vars,
            self.output_vars,
            exprs,
            self.type_annotations,
            self.aux_vars,
            self.entries,
        ))
    }
}

/// An unchecked DSRV specification.
#[derive(Clone, serde::Serialize)]
pub struct DsrvSpecification {
    pub(crate) input_vars: BTreeSet<VarName>,
    pub(crate) output_vars: BTreeSet<VarName>,
    pub(crate) aux_vars: BTreeSet<VarName>,
    pub(crate) stream_vars: BTreeSet<VarName>,
    pub(crate) exprs: ExprForestMap<VarName>,
    pub(crate) type_annotations: BTreeMap<VarName, StreamType>,
    /// Normalized statement order. Serialization intentionally retains the legacy
    /// set/map projection rather than making this diagnostic sequence persistent.
    #[serde(skip)]
    pub(crate) semantic_entries: Vec<SemanticEntry>,
}

mod language_mode_sealed {
    pub trait Sealed {}
}

pub trait LanguageMode: language_mode_sealed::Sealed + Clone + Debug + 'static {
    #[doc(hidden)]
    fn validate_distribution_constraint(span: Span) -> Result<(), Span>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Local;
impl language_mode_sealed::Sealed for Local {}
impl LanguageMode for Local {
    fn validate_distribution_constraint(span: Span) -> Result<(), Span> {
        Err(span)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Distributed;
impl language_mode_sealed::Sealed for Distributed {}
impl LanguageMode for Distributed {
    fn validate_distribution_constraint(_span: Span) -> Result<(), Span> {
        Ok(())
    }
}

impl PartialEq for DsrvSpecification {
    fn eq(&self, other: &Self) -> bool {
        self.input_vars == other.input_vars
            && self.output_vars == other.output_vars
            && self.aux_vars == other.aux_vars
            && self.stream_vars == other.stream_vars
            && self.type_annotations == other.type_annotations
            && self.semantic_entries == other.semantic_entries
            && self.exprs.len() == other.exprs.len()
            && self.exprs.iter().zip(other.exprs.iter()).all(
                |((left_name, left_expr), (right_name, right_expr))| {
                    left_name == right_name && left_expr.structurally_eq(right_expr)
                },
            )
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedDsrvSpecification<M: LanguageMode> {
    spec: DsrvSpecification,
    mode: PhantomData<M>,
}

impl<M: LanguageMode> ValidatedDsrvSpecification<M> {
    pub(crate) fn new(spec: DsrvSpecification) -> Self {
        Self {
            spec,
            mode: PhantomData,
        }
    }

    pub fn specification(&self) -> &DsrvSpecification {
        &self.spec
    }

    pub fn into_specification(self) -> DsrvSpecification {
        self.spec
    }
}

struct OrderedVars<'a>(&'a [VarName]);

impl Debug for OrderedVars<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0).finish()
    }
}

struct OrderedExprs<'a>(&'a DsrvSpecification);

impl Debug for OrderedExprs<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(
                self.0
                    .semantic_entries
                    .iter()
                    .filter_map(|entry| match entry {
                        SemanticEntry::Assignment { name, .. } => {
                            self.0.exprs.get(name).map(|expression| (name, expression))
                        }
                        _ => None,
                    }),
            )
            .finish()
    }
}

struct OrderedAnnotations<'a>(&'a DsrvSpecification);

impl Debug for OrderedAnnotations<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let declaration_order = self.0.semantic_entries.iter().filter_map(|entry| {
            (!matches!(entry, SemanticEntry::Assignment { .. })).then(|| entry.name())
        });
        let mut seen = BTreeSet::new();
        let mut entries = Vec::new();
        for name in declaration_order {
            if seen.insert(name.clone())
                && let Some(annotation) = self.0.type_annotations.get(name)
            {
                entries.push((name, annotation));
            }
        }
        for (name, annotation) in &self.0.type_annotations {
            if seen.insert(name.clone()) {
                entries.push((name, annotation));
            }
        }
        f.debug_map().entries(entries).finish()
    }
}

impl Debug for DsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DsrvSpecification")
            .field("semantic_entries", &self.semantic_entries)
            .field("input_vars", &OrderedVars(&self.input_vars_in_order()))
            .field("output_vars", &OrderedVars(&self.output_vars_in_order()))
            .field("aux_vars", &OrderedVars(&self.aux_vars_in_order()))
            .field("stream_vars", &OrderedVars(&self.stream_vars_in_order()))
            .field("exprs", &OrderedExprs(self))
            .field("type_annotations", &OrderedAnnotations(self))
            .finish()
    }
}

/// A specification paired with one immutable type for every reachable AST node.
#[derive(Clone, Debug)]
pub struct CheckedDsrvSpecification<M: LanguageMode = Local> {
    pub(super) spec: DsrvSpecification,
    checked: AstShared<CheckedTypes>,
    mode: PhantomData<M>,
}

impl<M: LanguageMode> CheckedDsrvSpecification<M> {
    pub(crate) fn new(spec: DsrvSpecification, expr_types: ExprTypes) -> Self {
        let environment = AstShared::new(spec.type_annotations().clone());
        let checked = AstShared::new(CheckedTypes::new(expr_types, environment));
        Self {
            spec,
            checked,
            mode: PhantomData,
        }
    }

    pub(crate) fn into_mode<N: LanguageMode>(self) -> CheckedDsrvSpecification<N> {
        CheckedDsrvSpecification {
            spec: self.spec,
            checked: self.checked,
            mode: PhantomData,
        }
    }

    pub fn unchecked(&self) -> &DsrvSpecification {
        &self.spec
    }

    pub fn var_expr_ref(&self, var: &VarName) -> Option<CheckedExprRef<'_>> {
        self.spec
            .exprs
            .get(var)
            .map(|expr| expr.with_checked_types(&self.checked))
    }

    pub fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.spec
            .exprs
            .get_owned(var)
            .map(|expr| CheckedExpr::from_checked_types(expr, self.checked.clone()))
    }
    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        self.spec.input_vars()
    }
    pub fn input_vars_in_order(&self) -> Vec<VarName> {
        self.spec.input_vars_in_order()
    }
    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        self.spec.output_vars()
    }
    pub fn output_vars_in_order(&self) -> Vec<VarName> {
        self.spec.output_vars_in_order()
    }
    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        self.spec.aux_vars()
    }
    pub fn aux_vars_in_order(&self) -> Vec<VarName> {
        self.spec.aux_vars_in_order()
    }
    pub fn stream_vars(&self) -> &BTreeSet<VarName> {
        self.spec.stream_vars()
    }
    pub fn stream_vars_in_order(&self) -> Vec<VarName> {
        self.spec.stream_vars_in_order()
    }
    pub fn type_annotations(&self) -> &BTreeMap<VarName, StreamType> {
        self.spec.type_annotations()
    }

    pub fn type_annotation(&self, var: &VarName) -> Option<&StreamType> {
        self.spec.type_annotation(var)
    }
}

impl<M: LanguageMode> Specification for CheckedDsrvSpecification<M> {
    type Expr = CheckedExpr;

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.spec.input_vars().clone()
    }
    fn input_vars_in_order(&self) -> Vec<VarName> {
        self.spec.input_vars_in_order()
    }
    fn output_vars(&self) -> BTreeSet<VarName> {
        self.spec.output_vars().clone()
    }
    fn output_vars_in_order(&self) -> Vec<VarName> {
        self.spec.output_vars_in_order()
    }
    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.spec.aux_vars().clone()
    }
    fn aux_vars_in_order(&self) -> Vec<VarName> {
        self.spec.aux_vars_in_order()
    }
    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.spec.stream_vars().clone()
    }
    fn stream_vars_in_order(&self) -> Vec<VarName> {
        self.spec.stream_vars_in_order()
    }
    fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        CheckedDsrvSpecification::var_expr(self, var)
    }
    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.spec.type_annotations().clone()
    }
}

fn complete_order(
    order: impl IntoIterator<Item = VarName>,
    members: &BTreeSet<VarName>,
) -> Vec<VarName> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::with_capacity(members.len());
    for name in order {
        if members.contains(&name) && seen.insert(name.clone()) {
            ordered.push(name);
        }
    }
    ordered.extend(members.iter().filter(|name| !seen.contains(*name)).cloned());
    ordered
}

impl DsrvSpecification {
    pub fn semantic_entries(&self) -> &[SemanticEntry] {
        &self.semantic_entries
    }

    pub fn roots(&self) -> impl DoubleEndedIterator<Item = (&VarName, ExprRef<'_>)> {
        self.exprs.iter()
    }

    /// Every syntax node in allocation order. Assignment trees occupy disjoint ranges.
    pub fn nodes(&self) -> impl DoubleEndedIterator<Item = ExprRef<'_>> {
        self.exprs.nodes()
    }

    pub(crate) fn from_expression_forest(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        exprs: ExprForestMap<VarName>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
    ) -> Self {
        let input_order = input_vars.iter().cloned().collect::<Vec<_>>();
        let output_order = output_vars.iter().cloned().collect::<Vec<_>>();
        let aux_vars = aux_vars.into_iter().collect::<Vec<_>>();
        let mut entries = Vec::new();
        entries.extend(input_order.iter().map(|name| SemanticEntry::Input {
            name: name.clone(),
            annotation: type_annotations.get(name).cloned(),
            span: Span::default(),
        }));
        entries.extend(output_order.iter().map(|name| SemanticEntry::Output {
            name: name.clone(),
            annotation: type_annotations.get(name).cloned(),
            span: Span::default(),
        }));
        entries.extend(aux_vars.iter().map(|name| SemanticEntry::Aux {
            name: name.clone(),
            annotation: type_annotations.get(name).cloned(),
            span: Span::default(),
        }));
        entries.extend(exprs.keys().map(|name| SemanticEntry::Assignment {
            name: name.clone(),
            span: Span::default(),
        }));
        Self::from_expression_forest_with_entries(
            input_vars,
            output_vars,
            exprs,
            type_annotations,
            aux_vars,
            entries,
        )
    }

    pub(crate) fn from_expression_forest_with_entries(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        exprs: ExprForestMap<VarName>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
        semantic_entries: Vec<SemanticEntry>,
    ) -> Self {
        let aux_vars = aux_vars.into_iter().collect::<BTreeSet<_>>();
        let stream_vars = output_vars
            .iter()
            .chain(aux_vars.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        Self {
            input_vars,
            output_vars,
            aux_vars,
            stream_vars,
            exprs,
            type_annotations,
            semantic_entries,
        }
    }

    /// Build a specification from independently constructed expression roots.
    /// The roots are copied once into compact shared storage.
    pub fn new(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        exprs: BTreeMap<VarName, Expr>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
    ) -> Self {
        // Copy each root independently so a root cannot accidentally include unrelated nodes
        // from its original expression.
        let capacity = exprs.values().map(|expr| expr.as_ref().subtree_len()).sum();
        let mut builder = ExprBuilder::with_capacities(capacity, exprs.len());
        let mut names = Vec::with_capacity(exprs.len());
        let mut roots = Vec::with_capacity(exprs.len());
        for (name, expression) in &exprs {
            names.push(name.clone());
            roots.push(builder.clone_subtree(expression.as_ref()));
        }
        let forest = builder
            .finish_forest(roots)
            .expect("independently cloned expressions form a complete forest");
        let exprs = ExprForestMap::new(names, forest)
            .expect("specification expression names are sorted and unique");
        Self::from_expression_forest(input_vars, output_vars, exprs, type_annotations, aux_vars)
    }

    pub fn var_expr_ref(&self, var: &VarName) -> Option<ExprRef<'_>> {
        self.exprs.get(var)
    }
    pub fn var_expr(&self, var: &VarName) -> Option<Expr> {
        self.exprs.get_owned(var)
    }
    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        &self.input_vars
    }
    pub fn input_vars_in_order(&self) -> Vec<VarName> {
        complete_order(
            self.semantic_entries
                .iter()
                .filter_map(|entry| match entry {
                    SemanticEntry::Input { name, .. } => Some(name.clone()),
                    _ => None,
                }),
            &self.input_vars,
        )
    }
    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        &self.output_vars
    }
    pub fn output_vars_in_order(&self) -> Vec<VarName> {
        complete_order(
            self.semantic_entries
                .iter()
                .filter_map(|entry| match entry {
                    SemanticEntry::Output { name, .. } => Some(name.clone()),
                    _ => None,
                }),
            &self.output_vars,
        )
    }
    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        &self.aux_vars
    }
    pub fn aux_vars_in_order(&self) -> Vec<VarName> {
        complete_order(
            self.semantic_entries
                .iter()
                .filter_map(|entry| match entry {
                    SemanticEntry::Aux { name, .. } => Some(name.clone()),
                    _ => None,
                }),
            &self.aux_vars,
        )
    }
    pub fn stream_vars(&self) -> &BTreeSet<VarName> {
        &self.stream_vars
    }
    pub fn stream_vars_in_order(&self) -> Vec<VarName> {
        complete_order(
            self.semantic_entries
                .iter()
                .filter_map(|entry| match entry {
                    SemanticEntry::Output { name, .. } | SemanticEntry::Aux { name, .. } => {
                        Some(name.clone())
                    }
                    _ => None,
                }),
            &self.stream_vars,
        )
    }
    pub fn type_annotations(&self) -> &BTreeMap<VarName, StreamType> {
        &self.type_annotations
    }
    pub fn type_annotation(&self, var: &VarName) -> Option<&StreamType> {
        self.type_annotations.get(var)
    }
}

impl Specification for DsrvSpecification {
    type Expr = Expr;

    fn input_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::input_vars(self).clone()
    }

    fn input_vars_in_order(&self) -> Vec<VarName> {
        DsrvSpecification::input_vars_in_order(self)
    }

    fn output_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::output_vars(self).clone()
    }

    fn output_vars_in_order(&self) -> Vec<VarName> {
        DsrvSpecification::output_vars_in_order(self)
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::aux_vars(self).clone()
    }

    fn aux_vars_in_order(&self) -> Vec<VarName> {
        DsrvSpecification::aux_vars_in_order(self)
    }

    fn stream_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::stream_vars(self).clone()
    }

    fn stream_vars_in_order(&self) -> Vec<VarName> {
        DsrvSpecification::stream_vars_in_order(self)
    }

    fn var_expr(&self, var: &VarName) -> Option<Expr> {
        self.exprs.get_owned(var)
    }

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        DsrvSpecification::type_annotations(self).clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use contiguous_tree::TreeCursorExt;
    use proptest::prelude::*;
    use tracing::info;

    #[cfg(feature = "thread-safe-ast")]
    use crate::TypeCheckMode;
    use crate::TypeCheckOptions;
    use crate::VarName;
    use crate::core::BinaryOperator;
    use crate::core::{Specification, StreamType, StreamTypeAscription};
    use crate::dsrv_fixtures::{
        spec_simple_add_aux_monitor, spec_simple_add_aux_typed_monitor, spec_simple_add_monitor,
        spec_simple_add_monitor_typed,
    };
    use crate::lang::dsrv::ast::{
        CheckedDsrvSpecification, CheckedExpr, Distributed, DsrvSpecification, ExprBuilder,
        ExprKind, LanguageMode, Local, ReconfigurableExprScope, SemanticEntry, SyntaxLiteral,
    };
    use crate::lang::dsrv::ast::{Expr, ExprView};
    use crate::lang::dsrv::parser::parse_expr;
    use crate::lang::dsrv::test_support::{
        SemanticEntryOracle, arb_boolean_sexpr, arb_duplicate_declaration_case, arb_float_sexpr,
        arb_int_sexpr, arb_mixed_sexpr, arb_ordered_specification_case, arb_string_sexpr,
    };
    use crate::lang::dsrv::type_checker::{SemanticError, TCType, TypeErrorKind};

    fn checked_expression(source: &str) -> CheckedExpr {
        CheckedDsrvSpecification::parse_with(source, TypeCheckOptions::STRICT)
            .unwrap()
            .var_expr(&VarName::new("y"))
            .unwrap()
    }

    fn assert_checked_views<M: LanguageMode>(checked: &CheckedDsrvSpecification<M>) {
        assert_eq!(
            checked.input_vars(),
            &<CheckedDsrvSpecification<M> as Specification>::input_vars(checked)
        );
        assert_eq!(
            checked.output_vars(),
            &<CheckedDsrvSpecification<M> as Specification>::output_vars(checked)
        );
        assert_eq!(
            checked.aux_vars(),
            &<CheckedDsrvSpecification<M> as Specification>::aux_vars(checked)
        );
        assert_eq!(
            checked.stream_vars(),
            &<CheckedDsrvSpecification<M> as Specification>::stream_vars(checked)
        );
        assert_eq!(
            checked.aux_vars_in_order(),
            <CheckedDsrvSpecification<M> as Specification>::aux_vars_in_order(checked)
        );
        assert_eq!(
            checked.input_vars_in_order(),
            <CheckedDsrvSpecification<M> as Specification>::input_vars_in_order(checked)
        );
        assert_eq!(
            checked.output_vars_in_order(),
            <CheckedDsrvSpecification<M> as Specification>::output_vars_in_order(checked)
        );
        assert_eq!(
            checked.stream_vars_in_order(),
            <CheckedDsrvSpecification<M> as Specification>::stream_vars_in_order(checked)
        );
        assert_eq!(
            checked.type_annotations(),
            &<CheckedDsrvSpecification<M> as Specification>::type_annotations(checked)
        );
    }

    #[cfg(feature = "thread-safe-ast")]
    #[test]
    fn specification_moves_and_traverses_on_another_thread() {
        let input = VarName::new("thread_safe_ast_input");
        let output = VarName::new("thread_safe_ast_output");
        let specification = "in thread_safe_ast_input\nout thread_safe_ast_output\nthread_safe_ast_output = thread_safe_ast_input + thread_safe_ast_input"
            .parse::<DsrvSpecification>()
            .unwrap();

        let joined = std::thread::spawn(move || {
            let expression = specification.var_expr_ref(&output).unwrap();
            let ExprView::BinOp(left, right, _) = expression.view() else {
                panic!("expected a binary expression");
            };
            let ExprView::Var(left_name) = left.view() else {
                panic!("expected the left operand to be a variable");
            };
            let ExprView::Var(right_name) = right.view() else {
                panic!("expected the right operand to be a variable");
            };
            assert_eq!(left_name.name(), "thread_safe_ast_input");
            assert_eq!(right_name.name(), "thread_safe_ast_input");
            assert_eq!(expression.free_variables(), BTreeSet::from([input]));
        });

        joined.join().unwrap();
    }

    #[cfg(feature = "thread-safe-ast")]
    #[test]
    fn both_proof_modes_move_across_threads_with_read_only_views() {
        let source = "in thread_mode_input: Int\n\
                      out thread_mode_output: Int\n\
                      thread_mode_output = thread_mode_input + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let validated_local = source.clone().validate::<Local>().unwrap();
        let validated_distributed = source.clone().validate::<Distributed>().unwrap();
        let checked_local = source.clone().type_check(TypeCheckOptions::STRICT).unwrap();
        let checked_distributed = source
            .validate::<Distributed>()
            .unwrap()
            .type_check(TypeCheckMode::Strict)
            .unwrap();

        let joined = std::thread::spawn(move || {
            assert_eq!(
                validated_local.specification().output_vars_in_order(),
                [VarName::new("thread_mode_output")]
            );
            assert_eq!(
                validated_distributed.specification().input_vars_in_order(),
                [VarName::new("thread_mode_input")]
            );
            assert_checked_views(&checked_local);
            assert_checked_views(&checked_distributed);
            assert_eq!(
                checked_local
                    .var_expr_ref(&VarName::new("thread_mode_output"))
                    .unwrap()
                    .typ(),
                &TCType::Int
            );
            assert_eq!(
                checked_distributed
                    .var_expr_ref(&VarName::new("thread_mode_output"))
                    .unwrap()
                    .typ(),
                &TCType::Int
            );
        });
        joined.join().unwrap();
    }

    #[test]
    fn specification_roots_share_storage_and_have_disjoint_ranges() {
        let specification =
            "in x: Int\nout first: Int\nout second: Int\nfirst = x + 1\nsecond = x + 2"
                .parse::<DsrvSpecification>()
                .unwrap();
        let roots = specification
            .roots()
            .map(|(_, expression)| expression)
            .collect::<Vec<_>>();

        assert!(
            roots
                .windows(2)
                .all(|pair| pair[0].shares_storage_with(pair[1]))
        );

        let ranges = roots
            .iter()
            .map(|root| {
                root.postorder()
                    .map(|node| node.id())
                    .collect::<BTreeSet<_>>()
            })
            .collect::<Vec<_>>();
        assert!(ranges[0].is_disjoint(&ranges[1]));
        assert_eq!(
            ranges.iter().map(BTreeSet::len).sum::<usize>(),
            specification.nodes().count()
        );
        for (name, root) in specification.roots() {
            assert!(
                specification
                    .semantic_entries()
                    .iter()
                    .any(|entry| matches!(entry, SemanticEntry::Assignment { name: entry_name, .. } if entry_name == name)),
                "each root must have an assignment entry"
            );
            assert!(
                root.shares_storage_with(specification.var_expr_ref(name).unwrap()),
                "assignment entries must resolve into the shared forest"
            );
        }
    }

    #[test]
    fn checked_equality_ignores_spans_but_includes_types() {
        let compact = checked_expression("out y: Int\ny = 1");
        let spaced = checked_expression("out y: Int\n\ny    =    1");
        let different_type = checked_expression("out y: Float\ny = 1");

        assert_eq!(compact, spaced);
        assert_ne!(compact, different_type);
    }

    #[test]
    fn checked_cursor_preserves_required_types_through_traversal() {
        let expression = checked_expression("out y: Int\ny = 1 + 2");
        let root = expression.as_ref();

        assert_eq!(root.typ(), &TCType::Int);
        let ExprView::BinOp(left, right, _) = root.view() else {
            panic!("expected binary expression");
        };
        assert_eq!(left.typ(), &TCType::Int);
        assert_eq!(right.typ(), &TCType::Int);
        assert!(root.children().all(|child| child.typ() == &TCType::Int));
        assert!(root.postorder().all(|node| {
            node.shared_type_environment() == expression.as_ref().shared_type_environment()
        }));
    }

    #[test]
    fn checked_views_preserve_annotations_in_child_collections() {
        let expression = checked_expression(
            "out y: (Int, List<Int>, Struct<x: Int>)\n\
             y = Tuple(1, List(2), Struct(\"x\": 3))",
        );
        let ExprView::Tuple(mut items) = expression.as_ref().view() else {
            panic!("expected tuple expression");
        };

        assert_eq!(items.next().unwrap().typ(), &TCType::Int);
        let ExprView::List(mut list_items) = items.next().unwrap().view() else {
            panic!("expected list expression");
        };
        assert_eq!(list_items.next().unwrap().typ(), &TCType::Int);
        let ExprView::Struct(fields) = items.next().unwrap().view() else {
            panic!("expected struct expression");
        };
        assert_eq!(fields.get("x").unwrap().typ(), &TCType::Int);
    }

    #[test]
    fn programmatic_keyed_expressions_reject_duplicate_fields_during_checking() {
        let expr = Expr::Map([("x".into(), Expr::Val(1)), ("x".into(), Expr::Val(2))]);
        let mut context = BTreeMap::new();

        let errors = crate::lang::dsrv::type_checker::type_check_expression(
            &expr,
            &StreamType::Map(Box::new(StreamType::Int)),
            &mut context,
        )
        .expect_err("programmatic duplicate fields must remain a checking error");
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::TypeError(type_error)
                if type_error.kind() == &TypeErrorKind::DuplicateField
        )));
    }
    use crate::lang::dsrv::span::{Span, strip_span, strip_span_ref};
    use ecow::{EcoVec, eco_vec};

    #[test]
    fn specification_construction_gives_each_assignment_its_own_type_context() {
        let expr = parse_expr("1 + 2").unwrap();
        let source_nodes = expr.as_ref().subtree_len();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from(["a".into(), "b".into()]),
            BTreeMap::from([(VarName::new("a"), expr.clone()), (VarName::new("b"), expr)]),
            BTreeMap::new(),
            Vec::new(),
        );
        let a = spec.var_expr(&VarName::new("a")).unwrap();
        let b = spec.var_expr(&VarName::new("b")).unwrap();
        assert_eq!(spec.nodes().count(), source_nodes * 2);
        assert_ne!(a.id(), b.id());
    }

    #[test]
    fn nested_runtime_expressions_preserve_their_scopes() {
        let owner = VarName::new("z");
        let dynamic = Expr::Dynamic(
            Box::new(Expr::Var("source".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
        );
        let defer = Expr::Defer(
            Box::new(Expr::Var("source".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["source".into()],
        );
        let expressions = BTreeMap::from([(
            owner.clone(),
            Expr::Tuple(eco_vec![
                Expr::Struct(BTreeMap::from([("dynamic".into(), dynamic)])),
                Expr::ObjectLiteral(BTreeMap::from([("defer".into(), defer)])),
            ]),
        )]);

        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([owner.clone()]),
            expressions,
            BTreeMap::new(),
            Vec::new(),
        );
        let annotated = spec.var_expr(&owner).unwrap();
        let ExprView::Tuple(mut containers) = annotated.as_ref().view() else {
            panic!("expected tuple containing nested runtime expressions");
        };
        let first = containers.next().expect("tuple has first container");
        let second = containers.next().expect("tuple has second container");
        let ExprView::Struct(fields) = first.view() else {
            panic!("expected struct container");
        };
        let dynamic = fields.get("dynamic").unwrap();
        let ExprView::Dynamic(_, _, scope) = dynamic.view() else {
            panic!("expected dynamic expression");
        };
        assert_eq!(scope, &ReconfigurableExprScope::Automatic);

        let ExprView::ObjectLiteral(fields) = second.view() else {
            panic!("expected object-literal container");
        };
        let defer = fields.get("defer").unwrap();
        let ExprView::Defer(_, _, scope) = defer.view() else {
            panic!("expected defer expression");
        };
        assert_eq!(
            scope,
            &ReconfigurableExprScope::Explicit(eco_vec!["source".into()])
        );
    }

    #[test]
    fn expression_equality_is_independent_of_arena_layout() {
        let built = Expr::BinOp(
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val(2)),
            BinaryOperator::Add,
        );

        let mut builder = ExprBuilder::with_capacity(4);
        let unrelated = builder.alloc(ExprKind::Val(99.into()), Span::default());
        let left = builder.alloc(ExprKind::Val(1.into()), Span::default());
        let right = builder.alloc(ExprKind::Val(2.into()), Span::default());
        let root = builder.alloc(
            ExprKind::BinOp(left, right, BinaryOperator::Add),
            Span::default(),
        );
        let mut roots = builder
            .finish_forest([root, unrelated])
            .unwrap()
            .into_roots();
        let laid_out_differently = roots.next().unwrap();

        assert_eq!(built, laid_out_differently);
    }

    #[test]
    fn expression_serialization_uses_language_source_not_arena_internals() {
        let expr = Expr::BinOp(
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val(2)),
            BinaryOperator::Add,
        );
        let json = serde_json::to_value(&expr).unwrap();

        assert_eq!(json, serde_json::Value::String("(1 + 2)".into()));
        let encoded = json.to_string();
        assert!(!encoded.contains("ExprId"));
        assert!(!encoded.contains("nodes"));
    }

    fn assert_oracle_entries(specification: &DsrvSpecification, expected: &[SemanticEntryOracle]) {
        assert_eq!(
            specification.semantic_entries().len(),
            expected.len(),
            "semantic entry count differs for source:\n{}",
            specification
        );
        for (actual, expected) in specification.semantic_entries().iter().zip(expected) {
            match (actual, expected) {
                (
                    SemanticEntry::Input {
                        name, annotation, ..
                    },
                    SemanticEntryOracle::Input {
                        name: expected_name,
                        annotation: expected_annotation,
                    },
                )
                | (
                    SemanticEntry::Output {
                        name, annotation, ..
                    },
                    SemanticEntryOracle::Output {
                        name: expected_name,
                        annotation: expected_annotation,
                    },
                )
                | (
                    SemanticEntry::Aux {
                        name, annotation, ..
                    },
                    SemanticEntryOracle::Aux {
                        name: expected_name,
                        annotation: expected_annotation,
                    },
                ) => {
                    assert_eq!(name.name(), *expected_name);
                    assert_eq!(
                        annotation,
                        &expected_annotation.map(|annotation| match annotation {
                            "Int" => StreamType::Int,
                            "Bool" => StreamType::Bool,
                            other => panic!("unsupported test annotation {other}"),
                        })
                    );
                }
                (
                    SemanticEntry::Assignment { name, .. },
                    SemanticEntryOracle::Assignment {
                        name: expected_name,
                        ..
                    },
                ) => assert_eq!(name.name(), *expected_name),
                (actual, expected) => panic!("entry mismatch: {actual:?} versus {expected:?}"),
            }
        }
    }

    fn stable_filter(
        entries: &[SemanticEntryOracle],
        predicate: impl Fn(&SemanticEntryOracle) -> bool,
    ) -> Vec<VarName> {
        entries
            .iter()
            .filter(|entry| predicate(entry))
            .map(|entry| VarName::new(entry.name()))
            .collect()
    }

    #[test]
    fn semantic_entries_authorize_interleaved_display_and_debug() {
        let source = "in z: Int\n\
                      out b: Int\n\
                      b = a + z\n\
                      in q: Int\n\
                      aux a: Int\n\
                      a = q + 1\n\
                      out c: Int\n\
                      c = z - q";
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let entries = specification.semantic_entries();

        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name().name())
                .collect::<Vec<_>>(),
            ["z", "b", "b", "q", "a", "a", "c", "c"]
        );
        assert!(matches!(
            entries[0],
            SemanticEntry::Input {
                annotation: Some(StreamType::Int),
                ..
            }
        ));
        assert_eq!(
            specification.input_vars_in_order(),
            [VarName::new("z"), VarName::new("q")]
        );
        assert_eq!(
            specification.output_vars_in_order(),
            [VarName::new("b"), VarName::new("c")]
        );
        assert_eq!(specification.aux_vars_in_order(), [VarName::new("a")]);
        assert_eq!(
            specification.stream_vars_in_order(),
            [VarName::new("b"), VarName::new("a"), VarName::new("c")]
        );
        let assignment_order = entries
            .iter()
            .filter_map(|entry| {
                matches!(entry, SemanticEntry::Assignment { .. }).then(|| entry.name().clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            assignment_order,
            [VarName::new("b"), VarName::new("a"), VarName::new("c")]
        );
        assert_eq!(
            specification.to_string(),
            "in z: Int\n\
             out b: Int\n\
             b = (a + z)\n\
             in q: Int\n\
             aux a: Int\n\
             a = (q + 1)\n\
             out c: Int\n\
             c = (z - q)\n"
        );

        let debug = format!("{specification:?}");
        let ordered_markers = [
            "Input { name: VarName::new(\"z\")",
            "Output { name: VarName::new(\"b\")",
            "Assignment { name: VarName::new(\"b\")",
            "Input { name: VarName::new(\"q\")",
            "Aux { name: VarName::new(\"a\")",
            "Assignment { name: VarName::new(\"a\")",
            "Output { name: VarName::new(\"c\")",
            "Assignment { name: VarName::new(\"c\")",
        ];
        let mut previous = 0;
        for marker in ordered_markers {
            let position = debug[previous..]
                .find(marker)
                .unwrap_or_else(|| panic!("debug output lacks ordered marker {marker:?}: {debug}"));
            previous += position + marker.len();
        }
    }

    #[test]
    fn semantic_order_is_independent_of_unrelated_preinterning() {
        let source = "in z: Int\nout b: Int\nb = z + 1\nin q: Int\nout c: Int\nc = q + 2";
        for name in ["c", "q", "b", "z", "unrelated_2", "unrelated_1"] {
            let _ = VarName::new(name);
        }
        let specification = source.parse::<DsrvSpecification>().unwrap();
        assert_eq!(
            specification.input_vars_in_order(),
            [VarName::new("z"), VarName::new("q")]
        );
        assert_eq!(
            specification.output_vars_in_order(),
            [VarName::new("b"), VarName::new("c")]
        );
        assert_eq!(
            specification.to_string(),
            "in z: Int\nout b: Int\nb = (z + 1)\nin q: Int\nout c: Int\nc = (q + 2)\n"
        );
    }

    #[test]
    fn semantic_equality_ignores_spans_but_observes_meaning_and_order() {
        let compact = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let commented = "(* non-ASCII: робо 🤖 *)\r\n\
                         in x: Int\r\n\
                         // output follows\r\n\
                         out y: Int\r\n\
                         y = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        assert_eq!(compact.semantic_entries(), commented.semantic_entries());
        assert_eq!(compact.input_vars(), commented.input_vars());
        assert_eq!(compact.output_vars(), commented.output_vars());
        assert_eq!(compact, commented);
        assert_eq!(compact, compact.clone());

        let reordered = "in x: Int\nout y: Int\ny = x + 1\nout z: Int\nz = x + 2"
            .parse::<DsrvSpecification>()
            .unwrap();
        let reordered_again = "in x: Int\nout z: Int\nz = x + 2\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        assert_eq!(reordered.input_vars(), reordered_again.input_vars());
        assert_eq!(reordered.output_vars(), reordered_again.output_vars());
        assert_ne!(reordered, reordered_again);

        let changed_role = "in x: Int\naux y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let changed_annotation = "in x: Int\nout y: Bool\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let changed_expression = "in x: Int\nout y: Int\ny = x + 2"
            .parse::<DsrvSpecification>()
            .unwrap();
        let changed_operator = "in x: Int\nout y: Int\ny = x - 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        assert_ne!(compact, changed_role);
        assert_ne!(compact, changed_annotation);
        assert_ne!(compact, changed_expression);
        assert_ne!(compact, changed_operator);
    }

    #[test]
    fn annotation_projection_retains_occurrences_but_lookup_is_lossy() {
        let source = "in z: Int\nout a: Int\nin z: Bool\na = z";
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let z_entries = specification
            .semantic_entries()
            .iter()
            .filter(|entry| entry.name() == &VarName::new("z"))
            .collect::<Vec<_>>();
        assert_eq!(z_entries.len(), 2);
        assert_eq!(
            z_entries
                .iter()
                .map(|entry| entry.annotation().cloned())
                .collect::<Vec<_>>(),
            [Some(StreamType::Int), Some(StreamType::Bool)]
        );
        assert_eq!(&source[z_entries[0].span().to_range()], "in z: Int");
        assert_eq!(&source[z_entries[1].span().to_range()], "in z: Bool");
        assert_eq!(
            specification.type_annotation(&VarName::new("z")),
            Some(&StreamType::Bool)
        );
        for errors in [
            specification.clone().validate::<Local>().unwrap_err(),
            specification.clone().validate::<Distributed>().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::DuplicateDeclaration { variable, first, duplicate }
                    if *variable == VarName::new("z")
                        && &source[first.to_range()] == "in z: Int"
                        && &source[duplicate.to_range()] == "in z: Bool"
            )));
        }
        assert_eq!(
            specification.to_string(),
            "in z: Int\nout a: Int\nin z: Bool\na = z\n"
        );
    }

    #[test]
    fn declaration_spans_remain_byte_ranges_after_unicode_comments_and_crlf() {
        let source = "(* комментарий 🤖 *)\r\n\
                      in z: Int\r\n\
                      out a: Int\r\n\
                      in z: Bool\r\n\
                      a = z";
        let specification = source.parse::<DsrvSpecification>().unwrap();
        let z_entries = specification
            .semantic_entries()
            .iter()
            .filter(|entry| entry.name() == &VarName::new("z"))
            .collect::<Vec<_>>();
        assert_eq!(z_entries.len(), 2);
        assert_eq!(&source[z_entries[0].span().to_range()], "in z: Int");
        assert_eq!(&source[z_entries[1].span().to_range()], "in z: Bool");
        for entry in z_entries {
            let span = entry.span();
            assert!(span.start <= span.end);
            assert!(usize::try_from(span.end).unwrap() <= source.len());
        }
        let errors = specification.validate::<Local>().unwrap_err();
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::DuplicateDeclaration { first, duplicate, .. }
                if &source[first.to_range()] == "in z: Int"
                    && &source[duplicate.to_range()] == "in z: Bool"
        )));
    }

    #[test]
    fn programmatic_constructors_keep_storage_and_distinguish_missing_equations() {
        let annotation_only = DsrvSpecification::new(
            BTreeSet::from([VarName::new("input")]),
            BTreeSet::new(),
            BTreeMap::new(),
            BTreeMap::from([(VarName::new("input"), StreamType::Int)]),
            [],
        );
        assert_eq!(annotation_only.to_string(), "in input: Int\n");
        assert_eq!(
            annotation_only.semantic_entries(),
            &[SemanticEntry::Input {
                name: VarName::new("input"),
                annotation: Some(StreamType::Int),
                span: Span::default(),
            }]
        );
        annotation_only
            .clone()
            .validate::<Local>()
            .expect("annotation-only input is structurally valid");

        let missing_equation = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([VarName::new("output")]),
            BTreeMap::new(),
            BTreeMap::new(),
            [],
        );
        assert_eq!(missing_equation.to_string(), "out output\n");
        missing_equation
            .clone()
            .validate::<Local>()
            .expect("missing equations are a compiler boundary");

        let repeated_aux = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([VarName::new("output")]),
            BTreeMap::new(),
            BTreeMap::new(),
            [VarName::new("helper"), VarName::new("helper")],
        );
        assert_eq!(
            repeated_aux.aux_vars(),
            &BTreeSet::from([VarName::new("helper")])
        );
        assert_eq!(
            repeated_aux
                .semantic_entries()
                .iter()
                .filter(|entry| matches!(entry, SemanticEntry::Aux { .. }))
                .count(),
            2
        );
        assert!(
            repeated_aux
                .validate::<Local>()
                .unwrap_err()
                .iter()
                .any(|error| matches!(
                    error,
                    SemanticError::DuplicateDeclaration { variable, .. }
                        if *variable == VarName::new("helper")
                ))
        );
    }

    #[test]
    fn specification_serialization_is_the_legacy_lossy_projection() {
        let specification = "in z: Int\nout b: Int\nb = z + 1\nin q: Int\naux a: Int\na = q + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let serialized = serde_json::to_value(&specification).unwrap();
        let object = serialized
            .as_object()
            .expect("specification is a JSON object");
        assert_eq!(
            object.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "aux_vars".to_owned(),
                "exprs".to_owned(),
                "input_vars".to_owned(),
                "output_vars".to_owned(),
                "stream_vars".to_owned(),
                "type_annotations".to_owned(),
            ])
        );
        assert!(!object.contains_key("semantic_entries"));
        assert!(!serialized.to_string().contains("Span"));
        let serialized_names =
            |variables: &BTreeSet<VarName>| variables.iter().map(VarName::name).collect::<Vec<_>>();
        assert_eq!(
            serialized["input_vars"],
            serde_json::json!(serialized_names(specification.input_vars()))
        );
        assert_eq!(
            serialized["output_vars"],
            serde_json::json!(serialized_names(specification.output_vars()))
        );
        assert_eq!(
            serialized["aux_vars"],
            serde_json::json!(serialized_names(specification.aux_vars()))
        );
        assert_eq!(
            serialized["stream_vars"],
            serde_json::json!(serialized_names(specification.stream_vars()))
        );
        assert_eq!(
            serialized["exprs"],
            serde_json::json!({
                "a": "(q + 1)",
                "b": "(z + 1)"
            })
        );
        assert_eq!(
            serialized["type_annotations"],
            serde_json::json!({
                "a": "Int",
                "b": "Int",
                "q": "Int",
                "z": "Int"
            })
        );

        let same_projection_different_order =
            "in q: Int\naux a: Int\na = q + 1\nin z: Int\nout b: Int\nb = z + 1"
                .parse::<DsrvSpecification>()
                .unwrap();
        assert_ne!(specification, same_projection_different_order);
        assert_eq!(
            serde_json::to_value(&specification).unwrap(),
            serde_json::to_value(&same_projection_different_order).unwrap()
        );
    }

    #[test]
    fn checked_specification_forwards_views_and_keeps_checked_storage_alive() {
        let checked = "in z: Int\nout b: Int\nb = z + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
        let b = VarName::new("b");
        assert_checked_views(&checked);
        assert_eq!(checked.var_expr_ref(&b).unwrap().typ(), &TCType::Int);
        assert_eq!(
            checked.var_expr(&b).unwrap(),
            <CheckedDsrvSpecification as Specification>::var_expr(&checked, &b).unwrap()
        );
        assert!(checked.var_expr_ref(&VarName::new("missing")).is_none());
        assert!(checked.var_expr(&VarName::new("missing")).is_none());
        let expression = checked.var_expr(&b).unwrap();
        let clone = checked.clone();
        drop(checked);
        assert_eq!(expression.typ(), &TCType::Int);
        assert_eq!(clone.var_expr_ref(&b).unwrap().typ(), &TCType::Int);
    }

    #[test]
    fn empty_checked_specification_has_empty_views_and_missing_lookups() {
        let checked = "".parse::<CheckedDsrvSpecification>().unwrap();
        assert!(checked.input_vars().is_empty());
        assert!(checked.output_vars().is_empty());
        assert!(checked.aux_vars().is_empty());
        assert!(checked.stream_vars().is_empty());
        assert!(checked.input_vars_in_order().is_empty());
        assert!(checked.output_vars_in_order().is_empty());
        assert!(checked.aux_vars_in_order().is_empty());
        assert!(checked.stream_vars_in_order().is_empty());
        assert!(checked.type_annotations().is_empty());
        assert!(checked.var_expr(&VarName::new("missing")).is_none());
    }

    #[test]
    fn programmatic_no_val_is_rejected_at_semantic_admission() {
        let output = VarName::new("output");
        let specification = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([output.clone()]),
            BTreeMap::from([(output, Expr::Val(SyntaxLiteral::NoVal))]),
            BTreeMap::new(),
            [],
        );
        let errors = specification
            .validate::<Local>()
            .expect_err("runtime NoVal must not be source syntax");
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::UnsupportedLiteral(message, Some(_))
                if message.contains("runtime states")
        )));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn ordered_specification_properties_follow_an_independent_entry_oracle(
            case in arb_ordered_specification_case()
        ) {
            let specification = case.source.parse::<DsrvSpecification>()
                .expect("ordered generator emits parser-valid source");
            assert_oracle_entries(&specification, &case.entries);

            let expected_inputs = stable_filter(&case.entries, |entry| {
                matches!(entry, SemanticEntryOracle::Input { .. })
            });
            let expected_outputs = stable_filter(&case.entries, |entry| {
                matches!(entry, SemanticEntryOracle::Output { .. })
            });
            let expected_aux = stable_filter(&case.entries, |entry| {
                matches!(entry, SemanticEntryOracle::Aux { .. })
            });
            let expected_streams = case.entries.iter().filter_map(|entry| {
                matches!(entry, SemanticEntryOracle::Output { .. } | SemanticEntryOracle::Aux { .. })
                    .then(|| VarName::new(entry.name()))
            }).collect::<Vec<_>>();
            let expected_assignments = case.entries.iter().filter_map(|entry| {
                matches!(entry, SemanticEntryOracle::Assignment { .. })
                    .then(|| VarName::new(entry.name()))
            }).collect::<Vec<_>>();
            prop_assert_eq!(specification.input_vars_in_order(), expected_inputs);
            prop_assert_eq!(specification.output_vars_in_order(), expected_outputs);
            prop_assert_eq!(specification.aux_vars_in_order(), expected_aux);
            prop_assert_eq!(specification.stream_vars_in_order(), expected_streams);
            prop_assert_eq!(
                specification.semantic_entries().iter().filter_map(|entry| {
                    matches!(entry, SemanticEntry::Assignment { .. })
                        .then(|| entry.name().clone())
                }).collect::<Vec<_>>(),
                expected_assignments
            );

            let permuted_source = case
                .entries
                .iter()
                .rev()
                .map(SemanticEntryOracle::source_line)
                .collect::<Vec<_>>()
                .join("\n");
            let permuted = permuted_source
                .parse::<DsrvSpecification>()
                .expect("permuting parser-valid statements must remain parser-valid");
            prop_assert_eq!(permuted.input_vars(), specification.input_vars());
            prop_assert_eq!(permuted.output_vars(), specification.output_vars());
            prop_assert_eq!(permuted.aux_vars(), specification.aux_vars());
            for entry in &case.entries {
                if let SemanticEntryOracle::Assignment { name, .. } = entry {
                    let name = VarName::new(name);
                    prop_assert_eq!(
                        format!("{}", permuted.var_expr_ref(&name).unwrap()),
                        format!("{}", specification.var_expr_ref(&name).unwrap())
                    );
                }
            }

            let displayed = specification.to_string();
            let reparsed = displayed.parse::<DsrvSpecification>()
                .expect("Display output should be parser-valid");
            prop_assert_eq!(reparsed.semantic_entries(), specification.semantic_entries());
            prop_assert_eq!(reparsed.input_vars(), specification.input_vars());
            prop_assert_eq!(reparsed.output_vars(), specification.output_vars());
            let reparsed_roots = reparsed
                .roots()
                .map(|(name, expression)| (name.clone(), strip_span_ref(expression)))
                .collect::<BTreeMap<_, _>>();
            let original_roots = specification
                .roots()
                .map(|(name, expression)| (name.clone(), strip_span_ref(expression)))
                .collect::<BTreeMap<_, _>>();
            prop_assert_eq!(reparsed_roots, original_roots);
            for entry in &case.entries {
                let name = VarName::new(entry.name());
                if matches!(entry, SemanticEntryOracle::Assignment { .. }) {
                    prop_assert!(specification.var_expr_ref(&name).is_some());
                }
            }
        }

        #[test]
        fn injected_declaration_duplicates_retain_evidence_and_fail_both_modes(
            case in arb_duplicate_declaration_case()
        ) {
            let specification = case.source.parse::<DsrvSpecification>()
                .expect("duplicate generator changes declarations only");
            assert_oracle_entries(&specification, &case.entries);
            let occurrences = specification.semantic_entries().iter().filter(|entry| {
                entry.name() == &VarName::new(&case.duplicate_name)
            }).count();
            prop_assert!(occurrences >= 2);

            for errors in [
                specification.clone().validate::<Local>().unwrap_err(),
                specification.clone().validate::<Distributed>().unwrap_err(),
            ] {
                let duplicate_found = errors.iter().any(|error| matches!(
                    error,
                    SemanticError::DuplicateDeclaration { variable, .. }
                        if *variable == VarName::new(&case.duplicate_name)
                ));
                prop_assert!(duplicate_found);
            }
            assert_oracle_entries(&specification, &case.entries);
            prop_assert!(!specification.to_string().is_empty());
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn test_prop_format_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let _ = format!("{}", e);
        }

        #[test]
        fn test_prop_display_parse_roundtrip(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_expr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_int(e in arb_int_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_expr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_float(e in arb_float_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_expr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_string(e in arb_string_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            info!("Testing roundtrip on {formatted} ({e:?})");
            let parsed = parse_expr(&formatted).expect(format!("Display output {formatted} should be parsable").as_str());
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_free_variables_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            for input in e.as_ref().free_variables() {
                assert!(valid_inputs.contains(&input));
            }
        }

        #[test]
        fn test_prop_display_parse_roundtrip_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_expr(&formatted).expect("Mixed display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_free_variables_works_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            for input in e.as_ref().free_variables() {
                assert!(valid_inputs.contains(&input));
            }
        }
    }

    #[test]
    fn test_display_simple_add() {
        let spec = spec_simple_add_monitor()
            .parse::<DsrvSpecification>()
            .unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_typed() {
        let spec = spec_simple_add_monitor_typed()
            .parse::<DsrvSpecification>()
            .unwrap();
        let res = format!("{}", spec);
        let expected = "in x: Int\nin y: Int\nout z: Int\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_aux() {
        let spec = spec_simple_add_aux_monitor()
            .parse::<DsrvSpecification>()
            .unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\naux u\naux w\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_display_simple_add_aux_typed() {
        let spec = spec_simple_add_aux_typed_monitor()
            .parse::<DsrvSpecification>()
            .unwrap();
        let res = format!("{}", spec);
        let expected =
            "in x: Int\nin y: Int\nout z: Int\naux u: Int\naux w: Int\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    fn assert_display_roundtrips(expr: &Expr) {
        let formatted = format!("{}", expr);
        let parsed = parse_expr(&formatted).expect("parser should parse display output");
        assert_eq!(strip_span(&parsed), strip_span(expr));
    }

    #[test]
    fn test_display_parse_roundtrip_dynamic_type_ascriptions() {
        assert_display_roundtrips(&Expr::Dynamic(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
        ));
        let explicit_dynamic = parse_expr("dynamic(x: Int, {x, y})").unwrap();
        assert_display_roundtrips(&explicit_dynamic);
        assert_display_roundtrips(&Expr::Defer(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            EcoVec::new(),
        ));
        assert_display_roundtrips(&Expr::Defer(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["x".into(), "y".into()],
        ));
    }

    #[test]
    fn test_display_parse_roundtrip_list_literal() {
        let expr = Expr::List(eco_vec![Expr::Val(1), Expr::Val(2)]);
        assert_eq!(format!("{}", expr), "[1, 2]");
        assert_display_roundtrips(&expr);
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mget() {
        let expr = Expr::MGet(Box::new(Expr::Var("records".into())), "target".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_expr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_minsert() {
        let expr = Expr::MInsert(
            Box::new(Expr::Var("m".into())),
            "key".into(),
            Box::new(Expr::Val(42)),
        );
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_expr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mremove() {
        let expr = Expr::MRemove(Box::new(Expr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_expr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mhas_key() {
        let expr = Expr::MHasKey(Box::new(Expr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_expr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_literal_key_quoting() {
        let expr = Expr::Map(BTreeMap::from([("quoted".into(), Expr::Val(true))]));
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_expr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));
    }
}
