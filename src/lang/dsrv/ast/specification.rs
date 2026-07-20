//! Checked and unchecked DSRV specifications.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;

use crate::core::{Specification, StreamType, VarName};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::type_checker::TCType;

use super::{
    CheckedExpr, CheckedExprRef, Expr, ExprArena, ExprHandle, ExprId, ExprRef, TypeAnnotations,
};

/// A top-level DSRV declaration produced by the parsers.
#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum STopDecl {
    Input(VarName, Option<StreamType>, Span),
    Output(VarName, Option<StreamType>, Span),
    Aux(VarName, Option<StreamType>, Span),
    Assignment(VarName, ExprId, Span),
}

/// An unchecked DSRV specification.
#[derive(Clone, PartialEq, serde::Serialize)]
pub struct DsrvSpecification {
    pub(crate) input_vars: BTreeSet<VarName>,
    pub(crate) output_vars: BTreeSet<VarName>,
    pub(crate) aux_vars: BTreeSet<VarName>,
    pub(crate) stream_vars: BTreeSet<VarName>,
    pub(crate) exprs: BTreeMap<VarName, Expr>,
    pub(crate) type_annotations: BTreeMap<VarName, StreamType>,
}

impl Debug for DsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DsrvSpecification")
            .field("input_vars", &self.input_vars)
            .field("output_vars", &self.output_vars)
            .field("aux_vars", &self.aux_vars)
            .field("stream_vars", &self.stream_vars)
            .field("exprs", &self.exprs)
            .field("type_annotations", &self.type_annotations)
            .finish()
    }
}

/// A specification paired with one immutable type for every reachable AST node.
#[derive(Clone, Debug)]
pub struct CheckedDsrvSpecification {
    pub(super) spec: DsrvSpecification,
    checked: Rc<TypeAnnotations>,
}

impl CheckedDsrvSpecification {
    pub(crate) fn new(spec: DsrvSpecification, types: Vec<TCType>) -> Self {
        let arena_len = spec
            .exprs
            .values()
            .next()
            .map_or(0, |expr| expr.arena().len());
        assert_eq!(
            types.len(),
            arena_len,
            "every AST node must have a checked type"
        );
        let type_info = Rc::new(spec.type_annotations().clone());
        Self {
            spec,
            checked: Rc::new(TypeAnnotations::new(types, type_info)),
        }
    }

    pub fn unchecked(&self) -> &DsrvSpecification {
        &self.spec
    }

    pub fn roots(&self) -> impl DoubleEndedIterator<Item = (&VarName, CheckedExprRef<'_>)> {
        self.spec
            .exprs
            .iter()
            .map(|(name, expr)| (name, expr.checked_ref(&self.checked)))
    }

    /// Every checked syntax node in allocation order.
    pub fn nodes(&self) -> impl DoubleEndedIterator<Item = CheckedExprRef<'_>> {
        self.spec.arena().into_iter().flat_map(|arena| {
            arena
                .iter()
                .map(|(id, _)| ExprRef::from_arena(arena, id).with_annotations(&self.checked))
        })
    }

    pub fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.spec
            .exprs
            .get(var)
            .cloned()
            .map(|expr| CheckedExpr::from_annotations(expr, self.checked.clone()))
    }
    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        self.spec.input_vars()
    }
    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        self.spec.output_vars()
    }
    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        self.spec.aux_vars()
    }
    pub fn stream_vars(&self) -> &BTreeSet<VarName> {
        self.spec.stream_vars()
    }
    pub fn type_annotations(&self) -> &BTreeMap<VarName, StreamType> {
        self.spec.type_annotations()
    }
    pub fn expressions(&self) -> impl Iterator<Item = (&VarName, CheckedExpr)> {
        self.spec.exprs.iter().map(|(name, expr)| {
            (
                name,
                CheckedExpr::from_annotations(expr.clone(), self.checked.clone()),
            )
        })
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
    fn output_vars(&self) -> BTreeSet<VarName> {
        self.spec.output_vars().clone()
    }
    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.spec.aux_vars().clone()
    }
    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.spec.stream_vars().clone()
    }
    fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        CheckedDsrvSpecification::var_expr(self, var)
    }
    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.spec.type_annotations().clone()
    }
}

impl DsrvSpecification {
    pub fn roots(&self) -> impl DoubleEndedIterator<Item = (&VarName, ExprRef<'_>)> {
        self.exprs.iter().map(|(name, expr)| (name, expr.as_ref()))
    }

    /// Every syntax node in allocation order. Assignment trees occupy disjoint ranges.
    pub fn nodes(&self) -> impl DoubleEndedIterator<Item = ExprRef<'_>> {
        self.arena()
            .into_iter()
            .flat_map(|arena| arena.iter().map(|(id, _)| ExprRef::from_arena(arena, id)))
    }
    pub(crate) fn into_compact_arena(self) -> Self {
        let mut arenas = self
            .exprs
            .values()
            .map(|expr| expr.arena() as *const ExprArena as usize);
        let Some(first) = arenas.next() else {
            return self;
        };
        if arenas.all(|arena| arena == first) {
            return self;
        }
        Self::new(
            self.input_vars,
            self.output_vars,
            self.exprs,
            self.type_annotations,
            self.aux_vars,
        )
    }

    pub(crate) fn arena_len(&self) -> usize {
        self.arena().map_or(0, |arena| arena.len())
    }

    /// Shared storage containing every assignment root.
    fn arena(&self) -> Option<&ExprArena> {
        let mut expressions = self.exprs.values();
        let first = expressions.next()?;
        debug_assert!(expressions.all(|expr| first.shares_storage_with(expr)));
        Some(first.arena())
    }

    pub(crate) fn from_arena(
        input_vars: BTreeSet<VarName>,
        output_vars: BTreeSet<VarName>,
        arena: ExprArena,
        exprs: BTreeMap<VarName, ExprId>,
        type_annotations: BTreeMap<VarName, StreamType>,
        aux_vars: impl IntoIterator<Item = VarName>,
    ) -> Self {
        // `ExprArena::alloc` is crate-private, so debug validation is sufficient to catch bugs in
        // internal builders without adding a second full traversal to production parsing.
        let aux_vars = aux_vars.into_iter().collect::<BTreeSet<_>>();
        let stream_vars = output_vars
            .iter()
            .cloned()
            .chain(aux_vars.iter().cloned())
            .collect();
        #[cfg(debug_assertions)]
        arena.assert_forest(exprs.values().copied());
        let entries = exprs.into_iter().collect::<Vec<_>>();
        let handles = ExprHandle::forest(arena, entries.iter().map(|(_, root)| *root));
        let exprs = entries
            .into_iter()
            .zip(handles)
            .map(|((name, _), tree)| (name, Expr::new(tree)))
            .collect();
        Self {
            input_vars,
            output_vars,
            aux_vars,
            stream_vars,
            exprs,
            type_annotations,
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
        let capacity = exprs.values().map(|expr| expr.arena().len()).sum();
        let mut arena = ExprArena::with_capacity(capacity);
        let roots = exprs
            .iter()
            .map(|(name, expr)| (name.clone(), arena.clone_tree(expr)))
            .collect();
        Self::from_arena(
            input_vars,
            output_vars,
            arena,
            roots,
            type_annotations,
            aux_vars,
        )
    }

    pub fn exprs(&self) -> &BTreeMap<VarName, Expr> {
        &self.exprs
    }
    pub fn var_expr_ref(&self, var: &VarName) -> Option<ExprRef<'_>> {
        self.exprs.get(var).map(Expr::as_ref)
    }
    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        &self.input_vars
    }
    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        &self.output_vars
    }
    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        &self.aux_vars
    }
    pub fn stream_vars(&self) -> &BTreeSet<VarName> {
        &self.stream_vars
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

    fn output_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::output_vars(self).clone()
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::aux_vars(self).clone()
    }

    fn stream_vars(&self) -> BTreeSet<VarName> {
        DsrvSpecification::stream_vars(self).clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<Expr> {
        self.exprs.get(var).cloned()
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

    use crate::Specification;
    use crate::VarName;
    use crate::core::{StreamType, StreamTypeAscription};
    use crate::dsrv_fixtures::{
        spec_simple_add_aux_monitor, spec_simple_add_aux_typed_monitor, spec_simple_add_monitor,
        spec_simple_add_monitor_typed,
    };
    use crate::dsrv_specification;
    use crate::lang::dsrv::ast::NumericalBinOp;
    use crate::lang::dsrv::ast::generation::{
        arb_boolean_sexpr, arb_float_sexpr, arb_int_sexpr, arb_mixed_sexpr, arb_string_sexpr,
    };
    use crate::lang::dsrv::ast::{
        CheckedExpr, DsrvSpecification, DynamicExprScope, ExprArena, ExprHandle, ExprId, ExprKind,
        SBinOp,
    };
    use crate::lang::dsrv::ast::{Expr, ExprView};
    use crate::lang::dsrv::lalr_parser::parse_sexpr;
    use crate::lang::dsrv::parser::sexpr as parse_sexpr_comb;
    use crate::lang::dsrv::type_checker::{TCType, type_check};

    fn checked_expression(source: &str) -> CheckedExpr {
        let spec = crate::lang::dsrv::lalr_parser::parse_str(source).unwrap();
        type_check(spec)
            .unwrap()
            .var_expr(&VarName::new("y"))
            .unwrap()
    }

    #[test]
    fn specification_roots_share_one_arena_and_disjoint_ranges() {
        let specification = crate::lang::dsrv::lalr_parser::parse_str(
            "in x: Int\nout first: Int\nout second: Int\nfirst = x + 1\nsecond = x + 2",
        )
        .unwrap();
        let roots = specification.exprs().values().collect::<Vec<_>>();

        assert!(
            roots
                .windows(2)
                .all(|pair| pair[0].shares_storage_with(pair[1]))
        );

        let ranges = roots
            .iter()
            .map(|root| {
                root.as_ref()
                    .postorder()
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
        assert!(
            root.postorder()
                .all(|node| node.type_info() == expression.as_ref().type_info())
        );
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
        let expr = Expr::MapOrdered([("x".into(), Expr::Val(1)), ("x".into(), Expr::Val(2))]);
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
    fn child_ids_preserve_source_order_for_each_node_shape() {
        let ids = [0, 1, 2, 3].map(ExprId::new);
        let cases = [
            (ExprKind::Val(1.into()), vec![]),
            (ExprKind::Not(ids[0]), vec![ids[0]]),
            (
                ExprKind::BinOp(ids[0], ids[1], SBinOp::NOp(NumericalBinOp::Add)),
                vec![ids[0], ids[1]],
            ),
            (
                ExprKind::If(ids[0], ids[1], ids[2]),
                vec![ids[0], ids[1], ids[2]],
            ),
            (
                ExprKind::Apply(ids[0], eco_vec![ids[1], ids[2]]),
                vec![ids[0], ids[1], ids[2]],
            ),
            (
                ExprKind::List(eco_vec![ids[2], ids[0], ids[1]]),
                vec![ids[2], ids[0], ids[1]],
            ),
            (
                ExprKind::Map(BTreeMap::from([("a".into(), ids[1]), ("b".into(), ids[3])]).into()),
                vec![ids[1], ids[3]],
            ),
        ];

        for (node, expected) in cases {
            let mut visited = Vec::new();
            visited.extend(node.child_ids());
            assert_eq!(visited, expected, "unexpected child order for {node:?}");
        }
    }

    #[test]
    fn borrowed_child_iteration_does_not_clone_the_arena_handle() {
        let expr = Expr::If(
            Box::new(Expr::Var("condition".into())),
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val(0)),
        );
        let owners = expr.storage_strong_count();
        let mut visited = 0;
        for child in expr.as_ref().children() {
            visited += 1;
            let _ = child.node();
        }

        assert_eq!(visited, 3);
        assert_eq!(expr.storage_strong_count(), owners);
    }

    #[test]
    fn subtree_traversal_and_folding_use_contiguous_postorder() {
        let expr = Expr::If(
            Box::new(Expr::Var("condition".into())),
            Box::new(Expr::BinOp(
                Box::new(Expr::Val(1)),
                Box::new(Expr::Val(2)),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            Box::new(Expr::Val(0)),
        );
        let root = expr.as_ref();
        let nodes = root.postorder().collect::<Vec<_>>();

        assert_eq!(nodes.len(), 6);
        assert_eq!(nodes.last().unwrap().id(), root.id());
        assert_eq!(
            root.fold(|node| 1 + node.children().copied().sum::<usize>()),
            nodes.len()
        );
    }

    #[test]
    fn specification_construction_gives_each_assignment_its_own_type_context() {
        let expr = parse_sexpr("1 + 2").unwrap();
        let source_nodes = expr.arena().len();
        let spec = DsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from(["a".into(), "b".into()]),
            BTreeMap::from([(VarName::new("a"), expr.clone()), (VarName::new("b"), expr)]),
            BTreeMap::new(),
            Vec::new(),
        );
        let a = spec.var_expr(&VarName::new("a")).unwrap();
        let b = spec.var_expr(&VarName::new("b")).unwrap();
        assert_eq!(a.arena().len(), source_nodes * 2);
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
            SBinOp::NOp(NumericalBinOp::Add),
        );

        let mut arena = ExprArena::default();
        arena.alloc(ExprKind::Val(99.into()), Span::default());
        let left = arena.alloc(ExprKind::Val(1.into()), Span::default());
        let right = arena.alloc(ExprKind::Val(2.into()), Span::default());
        let root = arena.alloc(
            ExprKind::BinOp(left, right, SBinOp::NOp(NumericalBinOp::Add)),
            Span::default(),
        );
        let laid_out_differently = Expr::new(ExprHandle::new(arena, root));

        assert_eq!(built, laid_out_differently);
    }

    #[test]
    fn expression_serialization_uses_language_source_not_arena_internals() {
        let expr = Expr::BinOp(
            Box::new(Expr::Val(1)),
            Box::new(Expr::Val(2)),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let json = serde_json::to_value(&expr).unwrap();

        assert_eq!(json, serde_json::Value::String("(1 + 2)".into()));
        let encoded = json.to_string();
        assert!(!encoded.contains("ExprId"));
        assert!(!encoded.contains("nodes"));
    }

    #[test]
    fn winnow_builds_nested_expressions_in_one_compact_arena() {
        let mut source = "1 + 2 + 3 + 4";
        let expr = parse_sexpr_comb(&mut source).unwrap();

        assert_eq!(expr.arena().len(), 7);
        assert_eq!(
            strip_span(&expr),
            "BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), NOp(Add)), Val(Int(3)), NOp(Add)), Val(Int(4)), NOp(Add))"
        );
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
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_int(e in arb_int_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_float(e in arb_float_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_display_parse_roundtrip_string(e in arb_string_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            info!("Testing roundtrip on {formatted} ({e:?})");
            let parsed = parse_sexpr(&formatted).expect(format!("Display output {formatted} should be parsable").as_str());
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_free_variables_works(e in arb_boolean_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            for input in e.free_variables() {
                assert!(valid_inputs.contains(&input));
            }
        }

        #[test]
        fn test_prop_display_parse_roundtrip_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let formatted = format!("{}", e);
            let parsed = parse_sexpr(&formatted).expect("Mixed display output should be parsable");
            prop_assert_eq!(strip_span(&parsed), strip_span(&e));
        }

        #[test]
        fn test_prop_free_variables_works_mixed(e in arb_mixed_sexpr(vec!["a".into(), "b".into()])) {
            let valid_inputs: Vec<VarName> = vec!["a".into(), "b".into()];
            for input in e.free_variables() {
                assert!(valid_inputs.contains(&input));
            }
        }
    }

    #[test]
    fn test_display_simple_add() {
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_typed() {
        let spec = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x: Int\nin y: Int\nout z: Int\nz = (x + y)\n";
        assert_eq!(res, expected);
    }

    #[test]
    fn test_display_simple_add_aux() {
        let spec = dsrv_specification(&mut spec_simple_add_aux_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected = "in x\nin y\nout z\naux u\naux w\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_display_simple_add_aux_typed() {
        let spec = dsrv_specification(&mut spec_simple_add_aux_typed_monitor()).unwrap();
        let res = format!("{}", spec);
        let expected =
            "in x: Int\nin y: Int\nout z: Int\naux u: Int\naux w: Int\nu = x\nw = y\nz = (u + w)";
        assert_eq!(
            res.lines().collect::<BTreeSet<_>>(),
            expected.lines().collect::<BTreeSet<_>>()
        );
    }

    fn assert_display_roundtrips_both_parsers(expr: &Expr) {
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(expr));
    }

    #[test]
    fn test_display_parse_roundtrip_dynamic_type_ascriptions_both_parsers() {
        assert_display_roundtrips_both_parsers(&Expr::Dynamic(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
        ));
        assert_display_roundtrips_both_parsers(&Expr::RestrictedDynamic(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["x".into(), "y".into()],
        ));
        assert_display_roundtrips_both_parsers(&Expr::Defer(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            EcoVec::new(),
        ));
        assert_display_roundtrips_both_parsers(&Expr::Defer(
            Box::new(Expr::Var("x".into())),
            StreamTypeAscription::Ascribed(StreamType::Int),
            eco_vec!["x".into(), "y".into()],
        ));
    }

    #[test]
    fn test_display_parse_roundtrip_list_literal_both_parsers() {
        let expr = Expr::List(eco_vec![Expr::Val(1), Expr::Val(2)]);
        assert_eq!(format!("{}", expr), "[1, 2]");
        assert_display_roundtrips_both_parsers(&expr);
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mget_both_parsers() {
        let expr = Expr::MGet(Box::new(Expr::Var("records".into())), "target".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_minsert_both_parsers() {
        let expr = Expr::MInsert(
            Box::new(Expr::Var("m".into())),
            "key".into(),
            Box::new(Expr::Val(42)),
        );
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mremove_both_parsers() {
        let expr = Expr::MRemove(Box::new(Expr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_key_quoting_mhas_key_both_parsers() {
        let expr = Expr::MHasKey(Box::new(Expr::Var("m".into())), "key".into());
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }

    #[test]
    fn test_display_parse_roundtrip_map_literal_key_quoting_both_parsers() {
        let expr = Expr::Map(BTreeMap::from([("quoted".into(), Expr::Val(true))]));
        let formatted = format!("{}", expr);

        let parsed_lalr = parse_sexpr(&formatted).expect("LALR parser should parse display output");
        assert_eq!(strip_span(&parsed_lalr), strip_span(&expr));

        let mut input = formatted.as_str();
        let parsed_comb =
            parse_sexpr_comb(&mut input).expect("Combinator parser should parse display output");
        assert_eq!(strip_span(&parsed_comb), strip_span(&expr));
    }
}
