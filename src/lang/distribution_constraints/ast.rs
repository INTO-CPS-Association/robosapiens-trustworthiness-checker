// TODO: TW - Figure out how to deduplicate this grammar compared to the basic one

use crate::core::Value;
use crate::core::VarName;
use contiguous_tree::TreeCursorExt;
use ecow::EcoVec;
use std::fmt::{Debug, Display};

// Numerical Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum NumericalBinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

// Bool Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum BoolBinOp {
    Or,
    And,
}

// Str Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum StrBinOp {
    Concat,
}

// Comparison Binary Operations
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum CompBinOp {
    Eq,
    Le,
    Ge,
    Lt,
    Gt,
}

// Stream BinOp
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum SBinOp {
    NOp(NumericalBinOp),
    BOp(BoolBinOp),
    SOp(StrBinOp),
    COp(CompBinOp),
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub enum DistConstraintType {
    CanRun,
    LocalityScore,
    Redundancy,
}

contiguous_tree::tree_schema! {
    pub tree DistConstraintExpr {
        internals: pub(crate),
        metadata: metadata: () = (),
        id: u32,
        key: String,
        children: EcoVec,
        keyed_children: EcoVec,

        If(condition: child, then_expr: child, else_expr: child),
        SIndex(input: child, offset: copy(isize), default: data(Value)),
        Val(value: data(Value)),
        BinOp(left: child, right: child, operator: data(SBinOp)),
        Var(variable: data(VarName)),
        Default(value: child, default: child),
        IsDefined(value: child),
        Not(value: child),
        List(items: children),
        LIndex(list: child, index: child),
        LAppend(list: child, value: child),
        LConcat(left: child, right: child),
        LHead(list: child),
        LTail(list: child),
        Sin(value: child),
        Cos(value: child),
        Tan(value: child),
        Monitor(variable: data(VarName)),
        Source(variable: data(VarName)),
        Dist(target: child),
        WeightedDist(weight: child, target: child),
        Sum(variable: data(VarName)),
    }
}

impl DistConstraintExprBuilder {
    pub fn finish(self, root: DistConstraintExprId) -> DistConstraintExpr {
        DistConstraintExpr::new(DistConstraintExprHandle::new(self.arena, root))
    }
}

// These constructors keep expression construction concise for parsers and tests.
// Each operation produces one self-contained tree.
#[allow(non_snake_case, clippy::boxed_local)]
impl DistConstraintExpr {
    fn leaf(kind: DistConstraintExprKind) -> Self {
        let mut arena = DistConstraintExprArena::with_capacity(1);
        let root = arena.alloc(kind, ());
        Self::new(DistConstraintExprHandle::new(arena, root))
    }

    fn merge(
        children: impl IntoIterator<Item = Self>,
        make: impl FnOnce(&[DistConstraintExprId]) -> DistConstraintExprKind,
    ) -> Self {
        let children = children.into_iter().collect::<Vec<_>>();
        let capacity = children
            .iter()
            .map(|child| child.as_ref().postorder().len())
            .sum::<usize>()
            + 1;
        let mut arena = DistConstraintExprArena::with_capacity(capacity);
        let ids = children
            .iter()
            .map(|child| arena.clone_tree(child))
            .collect::<Vec<_>>();
        let root = arena.alloc(make(&ids), ());
        Self::new(DistConstraintExprHandle::new(arena, root))
    }

    fn unary(
        child: Self,
        make: impl FnOnce(DistConstraintExprId) -> DistConstraintExprKind,
    ) -> Self {
        Self::merge([child], |ids| make(ids[0]))
    }

    fn binary(
        left: Self,
        right: Self,
        make: impl FnOnce(DistConstraintExprId, DistConstraintExprId) -> DistConstraintExprKind,
    ) -> Self {
        Self::merge([left, right], |ids| make(ids[0], ids[1]))
    }

    pub(super) fn If(condition: Box<Self>, then_expr: Box<Self>, else_expr: Box<Self>) -> Self {
        Self::merge([*condition, *then_expr, *else_expr], |ids| {
            DistConstraintExprKind::If(ids[0], ids[1], ids[2])
        })
    }
    pub(super) fn SIndex(input: Box<Self>, offset: isize, default: Value) -> Self {
        Self::unary(*input, |id| {
            DistConstraintExprKind::SIndex(id, offset, default)
        })
    }
    pub(super) fn Val(value: Value) -> Self {
        Self::leaf(DistConstraintExprKind::Val(value))
    }
    pub(super) fn BinOp(left: Box<Self>, right: Box<Self>, operator: SBinOp) -> Self {
        Self::binary(*left, *right, |l, r| {
            DistConstraintExprKind::BinOp(l, r, operator)
        })
    }
    pub(super) fn Var(variable: VarName) -> Self {
        Self::leaf(DistConstraintExprKind::Var(variable))
    }
    pub(super) fn Default(value: Box<Self>, default: Box<Self>) -> Self {
        Self::binary(*value, *default, DistConstraintExprKind::Default)
    }
    pub(super) fn IsDefined(value: Box<Self>) -> Self {
        Self::unary(*value, DistConstraintExprKind::IsDefined)
    }
    pub(super) fn Not(value: Box<Self>) -> Self {
        Self::unary(*value, DistConstraintExprKind::Not)
    }
    pub(super) fn List(items: Vec<Self>) -> Self {
        Self::merge(items, |ids| {
            DistConstraintExprKind::List(ids.iter().copied().collect())
        })
    }
    pub(super) fn LIndex(list: Box<Self>, index: Box<Self>) -> Self {
        Self::binary(*list, *index, DistConstraintExprKind::LIndex)
    }
    pub(super) fn LAppend(list: Box<Self>, value: Box<Self>) -> Self {
        Self::binary(*list, *value, DistConstraintExprKind::LAppend)
    }
    pub(super) fn LConcat(left: Box<Self>, right: Box<Self>) -> Self {
        Self::binary(*left, *right, DistConstraintExprKind::LConcat)
    }
    pub(super) fn LHead(list: Box<Self>) -> Self {
        Self::unary(*list, DistConstraintExprKind::LHead)
    }
    pub(super) fn LTail(list: Box<Self>) -> Self {
        Self::unary(*list, DistConstraintExprKind::LTail)
    }
    pub(super) fn Sin(value: Box<Self>) -> Self {
        Self::unary(*value, DistConstraintExprKind::Sin)
    }
    pub(super) fn Cos(value: Box<Self>) -> Self {
        Self::unary(*value, DistConstraintExprKind::Cos)
    }
    pub(super) fn Tan(value: Box<Self>) -> Self {
        Self::unary(*value, DistConstraintExprKind::Tan)
    }
    pub(super) fn Monitor(variable: VarName) -> Self {
        Self::leaf(DistConstraintExprKind::Monitor(variable))
    }
    pub(super) fn Source(variable: VarName) -> Self {
        Self::leaf(DistConstraintExprKind::Source(variable))
    }
}

impl Display for DistConstraintExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self.as_ref(), f)
    }
}

impl Display for DistConstraintExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use DistConstraintExprView::*;
        use SBinOp::*;
        match self.view() {
            If(b, e1, e2) => write!(f, "if {} then {} else {}", b, e1, e2),
            SIndex(s, i, c) => write!(f, "{}[{},{}]", s, i, c),
            Val(n) => write!(f, "{}", n),
            BinOp(e1, e2, NOp(NumericalBinOp::Add)) => write!(f, "({} + {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Sub)) => write!(f, "({} - {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Mul)) => write!(f, "({} * {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Div)) => write!(f, "({} / {})", e1, e2),
            BinOp(e1, e2, NOp(NumericalBinOp::Mod)) => write!(f, "({} % {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::Or)) => write!(f, "({} || {})", e1, e2),
            BinOp(e1, e2, BOp(BoolBinOp::And)) => write!(f, "({} && {})", e1, e2),
            BinOp(e1, e2, SOp(StrBinOp::Concat)) => write!(f, "({} ++ {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Eq)) => write!(f, "({} == {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Le)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Lt)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Ge)) => write!(f, "({} <= {})", e1, e2),
            BinOp(e1, e2, COp(CompBinOp::Gt)) => write!(f, "({} <= {})", e1, e2),
            Not(b) => write!(f, "!{}", b),
            Var(v) => write!(f, "{}", v),
            Default(e, v) => write!(f, "default({}, {})", e, v),
            IsDefined(sexpr) => write!(f, "is_defined({})", sexpr),
            List(es) => {
                let es_str: Vec<String> = es.map(|e| format!("{}", e)).collect();
                write!(f, "[{}]", es_str.join(", "))
            }
            LIndex(e, i) => write!(f, "List.get({}, {})", e, i),
            LAppend(lst, el) => write!(f, "List.append({}, {})", lst, el),
            LConcat(lst1, lst2) => write!(f, "List.concat({}, {})", lst1, lst2),
            LHead(lst) => write!(f, "List.head({})", lst),
            LTail(lst) => write!(f, "List.tail({})", lst),
            Sin(v) => write!(f, "sin({})", v),
            Cos(v) => write!(f, "cos({})", v),
            Tan(v) => write!(f, "tan({})", v),
            Monitor(v) => write!(f, "monitor({})", v),
            Source(v) => write!(f, "source({})", v),
            Dist(b) => write!(f, "dist({})", b),
            WeightedDist(w, b) => write!(f, "weighted_dist({}, {})", w, b),
            Sum(v) => write!(f, "sum({})", v),
        }
    }
}

impl PartialEq for DistConstraintExpr {
    fn eq(&self, other: &Self) -> bool {
        self.same_root(other)
            || self
                .as_ref()
                .try_zip_with::<std::convert::Infallible, _>(other.as_ref(), |a, b| {
                    Ok(a.node().node.same_payload(&b.node().node))
                })
                .unwrap_or_else(|never| match never {})
    }
}

impl Debug for DistConstraintExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.as_ref(), f)
    }
}

impl Debug for DistConstraintExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DistConstraintExprView::*;
        match self.view() {
            If(a, b, c) => f.debug_tuple("If").field(&a).field(&b).field(&c).finish(),
            SIndex(a, i, d) => f
                .debug_tuple("SIndex")
                .field(&a)
                .field(&i)
                .field(d)
                .finish(),
            Val(v) => f.debug_tuple("Val").field(v).finish(),
            BinOp(a, b, op) => f
                .debug_tuple("BinOp")
                .field(&a)
                .field(&b)
                .field(op)
                .finish(),
            Var(v) => f.debug_tuple("Var").field(v).finish(),
            Default(a, b) => f.debug_tuple("Default").field(&a).field(&b).finish(),
            IsDefined(a) => f.debug_tuple("IsDefined").field(&a).finish(),
            Not(a) => f.debug_tuple("Not").field(&a).finish(),
            List(items) => f
                .debug_tuple("List")
                .field(&items.collect::<Vec<_>>())
                .finish(),
            LIndex(a, b) => f.debug_tuple("LIndex").field(&a).field(&b).finish(),
            LAppend(a, b) => f.debug_tuple("LAppend").field(&a).field(&b).finish(),
            LConcat(a, b) => f.debug_tuple("LConcat").field(&a).field(&b).finish(),
            LHead(a) => f.debug_tuple("LHead").field(&a).finish(),
            LTail(a) => f.debug_tuple("LTail").field(&a).finish(),
            Sin(a) => f.debug_tuple("Sin").field(&a).finish(),
            Cos(a) => f.debug_tuple("Cos").field(&a).finish(),
            Tan(a) => f.debug_tuple("Tan").field(&a).finish(),
            Monitor(v) => f.debug_tuple("Monitor").field(v).finish(),
            Source(v) => f.debug_tuple("Source").field(v).finish(),
            Dist(a) => f.debug_tuple("Dist").field(&a).finish(),
            WeightedDist(a, b) => f.debug_tuple("WeightedDist").field(&a).field(&b).finish(),
            Sum(v) => f.debug_tuple("Sum").field(v).finish(),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct DistConstraint(pub DistConstraintType, pub DistConstraintExpr);

#[cfg(test)]
mod tests {
    use contiguous_tree::TreeCursorExt;

    use super::{DistConstraintExprBuilder, DistConstraintExprKind, NumericalBinOp, SBinOp};

    #[test]
    fn expression_is_stored_in_postorder() {
        let mut builder = DistConstraintExprBuilder::with_capacity(3);
        let left = builder.alloc(DistConstraintExprKind::Val(1.into()), ());
        let right = builder.alloc(DistConstraintExprKind::Val(2.into()), ());
        let root = builder.alloc(
            DistConstraintExprKind::BinOp(left, right, SBinOp::NOp(NumericalBinOp::Add)),
            (),
        );
        let expression = builder.finish(root);

        assert_eq!(expression.as_ref().postorder().len(), 3);
        assert_eq!(expression.to_string(), "(1 + 2)");
    }
}
