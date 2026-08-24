//! Checked and unchecked DSRV specifications.

use contiguous_tree::TreeCursorExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnvalidatedAssignment {
    pub(crate) name: VarName,
    pub(crate) span: Span,
}

/// A forest-backed specification whose declaration-level invariants are unchecked.
pub(crate) struct UnvalidatedDsrvSpecification {
    input_vars: BTreeSet<VarName>,
    output_vars: BTreeSet<VarName>,
    input_order: Vec<VarName>,
    output_order: Vec<VarName>,
    stream_order: Vec<VarName>,
    aux_vars: Vec<VarName>,
    expressions: ExprForest,
    assignments: Vec<UnvalidatedAssignment>,
    type_annotations: BTreeMap<VarName, StreamType>,
}

impl UnvalidatedDsrvSpecification {
    pub(crate) fn new(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        input_order: Vec<VarName>,
        output_order: Vec<VarName>,
        stream_order: Vec<VarName>,
        aux_vars: Vec<VarName>,
        expressions: ExprForest,
        assignments: Vec<UnvalidatedAssignment>,
        type_annotations: BTreeMap<VarName, StreamType>,
    ) -> Self {
        assert_eq!(
            assignments.len(),
            expressions.len(),
            "each assignment declaration must describe one expression root"
        );
        Self {
            input_vars,
            output_vars,
            input_order,
            output_order,
            stream_order,
            aux_vars,
            expressions,
            assignments,
            type_annotations,
        }
    }

    pub(crate) fn validate(self) -> Result<DsrvSpecification, DsrvAstError> {
        let assignment_order = self
            .assignments
            .iter()
            .map(|assignment| assignment.name.clone())
            .collect::<Vec<_>>();
        let names = self
            .assignments
            .iter()
            .map(|assignment| assignment.name.clone());
        let exprs = match ExprForestMap::from_unsorted(names, self.expressions) {
            Ok(exprs) => exprs,
            Err(contiguous_tree::ForestMapError::DuplicateKey {
                first_index,
                duplicate_index,
            }) => {
                let first = &self.assignments[first_index];
                let duplicate = &self.assignments[duplicate_index];
                return Err(DsrvAstError::DuplicateAssignment {
                    variable: duplicate.name.clone(),
                    first: first.span,
                    duplicate: duplicate.span,
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

        Ok(DsrvSpecification::from_expression_forest_with_orders(
            self.input_vars,
            self.output_vars,
            exprs,
            self.type_annotations,
            self.aux_vars,
            self.input_order,
            self.output_order,
            self.stream_order,
            assignment_order,
        ))
    }
}

/// An unchecked DSRV specification.
#[derive(Clone, PartialEq, serde::Serialize)]
pub struct DsrvSpecification {
    pub(crate) input_vars: BTreeSet<VarName>,
    pub(crate) output_vars: BTreeSet<VarName>,
    pub(crate) aux_vars: BTreeSet<VarName>,
    pub(crate) stream_vars: BTreeSet<VarName>,
    pub(crate) exprs: ExprForestMap<VarName>,
    pub(crate) type_annotations: BTreeMap<VarName, StreamType>,
    /// Syntax-derived layout order. These fields are intentionally not serialized:
    /// the set/map fields above remain the compatibility representation, while
    /// parsed specifications retain the order required by positional runtimes.
    #[serde(skip)]
    pub(crate) input_order: Vec<VarName>,
    #[serde(skip)]
    pub(crate) output_order: Vec<VarName>,
    #[serde(skip)]
    pub(crate) aux_order: Vec<VarName>,
    #[serde(skip)]
    pub(crate) stream_order: Vec<VarName>,
    #[serde(skip)]
    pub(crate) assignment_order: Vec<VarName>,
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
                    .assignment_order
                    .iter()
                    .filter_map(|name| self.0.exprs.get(name).map(|expression| (name, expression))),
            )
            .finish()
    }
}

struct OrderedAnnotations<'a>(&'a DsrvSpecification);

impl Debug for OrderedAnnotations<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let declaration_order = self
            .0
            .input_order
            .iter()
            .chain(self.0.output_order.iter())
            .chain(self.0.aux_order.iter());
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
            .field("input_vars", &OrderedVars(&self.input_order))
            .field("output_vars", &OrderedVars(&self.output_order))
            .field("aux_vars", &OrderedVars(&self.aux_order))
            .field("stream_vars", &OrderedVars(&self.stream_order))
            .field("exprs", &OrderedExprs(self))
            .field("type_annotations", &OrderedAnnotations(self))
            .finish()
    }
}

/// A specification paired with one immutable type for every reachable AST node.
#[derive(Clone, Debug)]
pub struct CheckedDsrvSpecification {
    pub(super) spec: DsrvSpecification,
    checked: AstShared<CheckedTypes>,
}

impl CheckedDsrvSpecification {
    pub(crate) fn new(spec: DsrvSpecification, expr_types: ExprTypes) -> Self {
        let environment = AstShared::new(spec.type_annotations().clone());
        let checked = AstShared::new(CheckedTypes::new(expr_types, environment));
        Self { spec, checked }
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

impl Specification for CheckedDsrvSpecification {
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

fn complete_order(order: Vec<VarName>, members: &BTreeSet<VarName>) -> Vec<VarName> {
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
        let input_order = input_vars.iter().cloned().collect();
        let output_order: Vec<VarName> = output_vars.iter().cloned().collect();
        let aux_vars = aux_vars.into_iter().collect::<Vec<_>>();
        let stream_order = output_order
            .iter()
            .chain(aux_vars.iter())
            .cloned()
            .collect();
        let assignment_order = exprs.keys().cloned().collect();
        Self::from_expression_forest_with_orders(
            input_vars,
            output_vars,
            exprs,
            type_annotations,
            aux_vars,
            input_order,
            output_order,
            stream_order,
            assignment_order,
        )
    }

    pub(crate) fn from_expression_forest_with_orders(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        exprs: ExprForestMap<VarName>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
        input_order: Vec<VarName>,
        output_order: Vec<VarName>,
        stream_order: Vec<VarName>,
        assignment_order: Vec<VarName>,
    ) -> Self {
        let aux_order = aux_vars.into_iter().collect::<Vec<_>>();
        let aux_vars = aux_order.iter().cloned().collect::<BTreeSet<_>>();
        let input_order = complete_order(input_order, &input_vars);
        let output_order = complete_order(output_order, &output_vars);
        let aux_order = complete_order(aux_order, &aux_vars);
        let stream_members = output_vars
            .iter()
            .chain(aux_vars.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let stream_order = complete_order(stream_order, &stream_members);
        let expr_names = exprs.keys().cloned().collect::<BTreeSet<_>>();
        let assignment_order = complete_order(assignment_order, &expr_names);
        Self {
            input_vars,
            output_vars,
            aux_vars,
            stream_vars: stream_order.iter().cloned().collect(),
            exprs,
            type_annotations,
            input_order,
            output_order,
            aux_order,
            stream_order,
            assignment_order,
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
        self.input_order.clone()
    }
    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        &self.output_vars
    }
    pub fn output_vars_in_order(&self) -> Vec<VarName> {
        self.output_order.clone()
    }
    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        &self.aux_vars
    }
    pub fn aux_vars_in_order(&self) -> Vec<VarName> {
        self.aux_order.clone()
    }
    pub fn stream_vars(&self) -> &BTreeSet<VarName> {
        &self.stream_vars
    }
    pub fn stream_vars_in_order(&self) -> Vec<VarName> {
        self.stream_order.clone()
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

    use crate::TypeCheckOptions;
    use crate::VarName;
    use crate::core::BinaryOperator;
    use crate::core::{StreamType, StreamTypeAscription};
    use crate::dsrv_fixtures::{
        spec_simple_add_aux_monitor, spec_simple_add_aux_typed_monitor, spec_simple_add_monitor,
        spec_simple_add_monitor_typed,
    };
    use crate::lang::dsrv::ast::{
        CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, DynamicExprScope, ExprBuilder,
        ExprKind,
    };
    use crate::lang::dsrv::ast::{Expr, ExprView};
    use crate::lang::dsrv::parser::parse_expr;
    use crate::lang::dsrv::test_support::{
        arb_boolean_sexpr, arb_float_sexpr, arb_int_sexpr, arb_mixed_sexpr, arb_string_sexpr,
    };
    use crate::lang::dsrv::type_checker::TCType;

    fn checked_expression(source: &str) -> CheckedExpr {
        CheckedDsrvSpecification::parse_with(source, TypeCheckOptions::STRICT)
            .unwrap()
            .var_expr(&VarName::new("y"))
            .unwrap()
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

        assert!(
            crate::lang::dsrv::type_checker::type_check_expression(
                &expr,
                &StreamType::Map(Box::new(StreamType::Int)),
                &mut context,
            )
            .is_err()
        );
    }
    use crate::lang::dsrv::span::{Span, strip_span};
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
        assert_eq!(scope, &DynamicExprScope::Automatic);

        let ExprView::ObjectLiteral(fields) = second.view() else {
            panic!("expected object-literal container");
        };
        let defer = fields.get("defer").unwrap();
        let ExprView::Defer(_, _, scope) = defer.view() else {
            panic!("expected defer expression");
        };
        assert_eq!(
            scope,
            &DynamicExprScope::Explicit(eco_vec!["source".into()])
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
