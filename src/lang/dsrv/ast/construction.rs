//! Efficient expression construction.

use std::collections::BTreeMap;

use contiguous_tree::TreeCursorExt;
use ecow::{EcoString, EcoVec};

use super::{
    DynamicExprScope, Expr, ExprArena, ExprBuilder, ExprFields, ExprHandle, ExprId, ExprKind,
    SBinOp,
};
use crate::core::{StreamType, StreamTypeAscription, Value, VarName};
use crate::lang::dsrv::span::Span;

impl From<BTreeMap<EcoString, ExprId>> for ExprFields {
    fn from(value: BTreeMap<EcoString, ExprId>) -> Self {
        value.into_iter().collect()
    }
}

impl ExprBuilder {
    pub(crate) fn for_source(source: &str) -> Self {
        Self::with_capacity(source.len() / 4)
    }

    #[inline]
    pub(crate) fn alloc_at(&mut self, node: ExprKind, start: usize, end: usize) -> ExprId {
        self.alloc(node, Span::new(start as u32, end as u32))
    }

    pub fn finish(self, root: ExprId) -> Expr {
        Expr::new(ExprHandle::new(self.arena, root))
    }
}

pub(crate) fn finish_root_span(expr: Expr, span: Span) -> Expr {
    let (mut arena, root) = expr.into_owned_arena();
    arena.get_mut(root).span = span;
    Expr::new(ExprHandle::new(arena, root))
}

// Box-shaped constructors used by the Winnow parser. The LALR parser and
// `sexpr!` construct nodes directly through `ExprBuilder`.
impl From<ExprKind> for Expr {
    fn from(node: ExprKind) -> Self {
        Self::with_span(node, Span::default())
    }
}

#[doc(hidden)]
#[allow(non_snake_case, clippy::boxed_local)]
impl Expr {
    fn merge_children(
        children: impl IntoIterator<Item = Expr>,
        make: impl FnOnce(&[crate::lang::dsrv::ast::ExprId]) -> ExprKind,
    ) -> Self {
        let children = children.into_iter().collect::<Vec<_>>();
        let capacity = children
            .iter()
            .map(|child| child.as_ref().postorder().len())
            .sum::<usize>()
            + 1;
        let mut arena = ExprArena::with_capacity(capacity);
        let ids = children
            .iter()
            .map(|child| arena.clone_tree(child))
            .collect::<Vec<_>>();
        let root = arena.alloc(make(&ids), Span::default());
        Self::new(ExprHandle::new(arena, root))
    }

    fn unary(
        child: impl Into<Expr>,
        make: impl FnOnce(crate::lang::dsrv::ast::ExprId) -> ExprKind,
    ) -> Self {
        Self::merge_children([child.into()], |ids| make(ids[0]))
    }

    fn binary(
        left: impl Into<Expr>,
        right: impl Into<Expr>,
        make: impl FnOnce(crate::lang::dsrv::ast::ExprId, crate::lang::dsrv::ast::ExprId) -> ExprKind,
    ) -> Self {
        Self::merge_children([left.into(), right.into()], |ids| make(ids[0], ids[1]))
    }
    pub fn with_span(node: ExprKind, span: Span) -> Self {
        let mut arena = crate::lang::dsrv::ast::ExprArena::with_capacity(1);
        let root = arena.alloc(node, span);
        Self::new(ExprHandle::new(arena, root))
    }

    pub fn Val(value: impl Into<Value>) -> Self {
        Self::with_span(ExprKind::Val(value.into()), Span::default())
    }

    pub fn Var(var: VarName) -> Self {
        Self::with_span(ExprKind::Var(var), Span::default())
    }

    pub fn VarAt(var: VarName, span: Span) -> Self {
        Self::with_span(ExprKind::Var(var), span)
    }

    pub fn MonitoredAt(
        var: VarName,
        node: crate::distributed::distribution_graphs::NodeName,
    ) -> Self {
        Self::with_span(ExprKind::MonitoredAt(var, node), Span::default())
    }

    pub fn Dist(
        var: crate::lang::dsrv::ast::VarOrNodeName,
        node: crate::lang::dsrv::ast::VarOrNodeName,
    ) -> Self {
        Self::with_span(ExprKind::Dist(var, node), Span::default())
    }

    pub fn BinOp<L: Into<Self>, R: Into<Self>>(left: Box<L>, right: Box<R>, op: SBinOp) -> Self {
        Self::binary((*left).into(), (*right).into(), |l, r| {
            ExprKind::BinOp(l, r, op)
        })
    }
    pub fn If<C: Into<Self>, T: Into<Self>, E: Into<Self>>(
        cond: Box<C>,
        then_expr: Box<T>,
        else_expr: Box<E>,
    ) -> Self {
        Self::merge_children(
            [(*cond).into(), (*then_expr).into(), (*else_expr).into()],
            |ids| ExprKind::If(ids[0], ids[1], ids[2]),
        )
    }
    pub fn SIndex<E: Into<Self>>(expr: Box<E>, index: u64) -> Self {
        Self::unary(*expr, |id| ExprKind::SIndex(id, index))
    }
    pub fn Init<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::Init)
    }
    pub fn Update<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::Update)
    }
    pub fn Default<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::Default)
    }
    pub fn Latch<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::Latch)
    }
    pub fn Not<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Not)
    }
    pub fn When<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::When)
    }
    pub fn IsDefined<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::IsDefined)
    }
    pub fn Fix<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Fix)
    }
    pub fn Sin<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Sin)
    }
    pub fn Cos<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Cos)
    }
    pub fn Tan<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Tan)
    }
    pub fn Abs<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::Abs)
    }
    pub fn LHead<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::LHead)
    }
    pub fn LTail<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::LTail)
    }
    pub fn LLen<E: Into<Self>>(e: Box<E>) -> Self {
        Self::unary(*e, ExprKind::LLen)
    }
    pub fn LIndex<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::LIndex)
    }
    pub fn LAppend<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::LAppend)
    }
    pub fn LConcat<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::LConcat)
    }
    pub fn LMap<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::LMap)
    }
    pub fn LFilter<L: Into<Self>, R: Into<Self>>(l: Box<L>, r: Box<R>) -> Self {
        Self::binary(*l, *r, ExprKind::LFilter)
    }
    pub fn MGet<E: Into<Self>>(e: Box<E>, key: EcoString) -> Self {
        Self::unary(*e, |id| ExprKind::MGet(id, key))
    }
    pub fn SGet<E: Into<Self>>(e: Box<E>, key: EcoString) -> Self {
        Self::unary(*e, |id| ExprKind::SGet(id, key))
    }
    pub fn MRemove<E: Into<Self>>(e: Box<E>, key: EcoString) -> Self {
        Self::unary(*e, |id| ExprKind::MRemove(id, key))
    }
    pub fn MHasKey<E: Into<Self>>(e: Box<E>, key: EcoString) -> Self {
        Self::unary(*e, |id| ExprKind::MHasKey(id, key))
    }
    pub fn MInsert<M: Into<Self>, V: Into<Self>>(
        map: Box<M>,
        key: EcoString,
        value: Box<V>,
    ) -> Self {
        Self::binary(*map, *value, |m, v| ExprKind::MInsert(m, key, v))
    }

    pub fn Lambda(params: EcoVec<(VarName, StreamType)>, body: Box<impl Into<Self>>) -> Self {
        Self::unary(*body, |id| ExprKind::Lambda(params, id))
    }
    pub fn Apply(func: Box<impl Into<Self>>, args: EcoVec<Self>) -> Self {
        let count = args.len();
        Self::merge_children(std::iter::once((*func).into()).chain(args), |ids| {
            ExprKind::Apply(ids[0], ids[1..=count].into())
        })
    }
    pub fn Partial(func: Box<impl Into<Self>>, args: EcoVec<Self>) -> Self {
        let count = args.len();
        Self::merge_children(std::iter::once((*func).into()).chain(args), |ids| {
            ExprKind::Partial(ids[0], ids[1..=count].into())
        })
    }
    pub fn LFold(
        func: Box<impl Into<Self>>,
        init: Box<impl Into<Self>>,
        list: Box<impl Into<Self>>,
    ) -> Self {
        Self::merge_children([(*func).into(), (*init).into(), (*list).into()], |ids| {
            ExprKind::LFold(ids[0], ids[1], ids[2])
        })
    }

    fn collection(
        items: impl IntoIterator<Item = Self>,
        make: impl FnOnce(EcoVec<crate::lang::dsrv::ast::ExprId>) -> ExprKind,
    ) -> Self {
        let items = items.into_iter().collect::<Vec<_>>();
        Self::merge_children(items, |ids| make(ids.into()))
    }
    pub fn List(items: EcoVec<Self>) -> Self {
        Self::collection(items, ExprKind::List)
    }
    pub fn Tuple(items: EcoVec<Self>) -> Self {
        Self::collection(items, ExprKind::Tuple)
    }
    pub fn Map(items: BTreeMap<EcoString, Self>) -> Self {
        Self::keyed(items, ExprKind::Map)
    }
    pub(crate) fn MapOrdered(items: impl IntoIterator<Item = (EcoString, Self)>) -> Self {
        Self::keyed(items, ExprKind::Map)
    }
    pub fn Struct(items: BTreeMap<EcoString, Self>) -> Self {
        Self::keyed(items, ExprKind::Struct)
    }
    pub(crate) fn StructOrdered(items: impl IntoIterator<Item = (EcoString, Self)>) -> Self {
        Self::keyed(items, ExprKind::Struct)
    }
    pub fn ObjectLiteral(items: BTreeMap<EcoString, Self>) -> Self {
        Self::keyed(items, ExprKind::ObjectLiteral)
    }
    pub(crate) fn ObjectLiteralOrdered(items: impl IntoIterator<Item = (EcoString, Self)>) -> Self {
        Self::keyed(items, ExprKind::ObjectLiteral)
    }
    fn keyed(
        items: impl IntoIterator<Item = (EcoString, Self)>,
        make: impl FnOnce(ExprFields) -> ExprKind,
    ) -> Self {
        let (keys, values): (Vec<_>, Vec<_>) = items.into_iter().unzip();
        Self::merge_children(values, |ids| {
            make(keys.into_iter().zip(ids.iter().copied()).collect())
        })
    }

    pub fn Dynamic<E: Into<Self>>(source: Box<E>, result_type: StreamTypeAscription) -> Self {
        Self::unary(*source, |id| {
            ExprKind::Dynamic(id, result_type, DynamicExprScope::Automatic)
        })
    }
    pub fn RestrictedDynamic<E: Into<Self>>(
        source: Box<E>,
        result_type: StreamTypeAscription,
        vars: EcoVec<VarName>,
    ) -> Self {
        Self::unary(*source, |id| {
            ExprKind::Dynamic(id, result_type, DynamicExprScope::Explicit(vars))
        })
    }
    pub fn Defer<E: Into<Self>>(
        source: Box<E>,
        result_type: StreamTypeAscription,
        vars: EcoVec<VarName>,
    ) -> Self {
        Self::unary(*source, |id| {
            let scope = if vars.is_empty() {
                DynamicExprScope::Automatic
            } else {
                DynamicExprScope::Explicit(vars)
            };
            ExprKind::Defer(id, result_type, scope)
        })
    }
}
