use ecow::{EcoString, EcoVec};
use serde;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, Range};

use crate::core::{StreamTypeAscription, Value, VarName};

use crate::{
    SExpr,
    lang::dsrv::ast::{SBinOp, SpannedExpr},
};

// Span struct designed by IWANABETHATGUY in the l-lang repository at https://github.com/IWANABETHATGUY/l-lang/blob/master/crates/parser/src/span.rs
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Debug, serde::Serialize)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    pub fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }

    // Returns the length of the span.
    pub fn len(&self) -> u32 {
        self.end - self.start
    }

    // Returns true if the span is empty.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    // Convert to a Range<usize> for compatibility with codespan-reporting.
    pub fn to_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }

    // Returns true if this span contains the given offset.
    pub fn contains_offset(&self, offset: u32) -> bool {
        self.start <= offset && offset <= self.end
    }
}

impl From<Range<usize>> for Span {
    fn from(range: Range<usize>) -> Self {
        Self {
            start: range.start as u32,
            end: range.end as u32,
        }
    }
}

impl From<Span> for Range<usize> {
    fn from(span: Span) -> Self {
        span.start as usize..span.end as usize
    }
}

impl From<&Span> for Range<usize> {
    fn from(span: &Span) -> Self {
        span.start as usize..span.end as usize
    }
}

// Generic Spanned struct that can be used to wrap any node with a span, used for the SExpr nodes in the AST to keep track of their location in the source code.
#[derive(Clone, Copy, Default, Eq, Hash, PartialEq, PartialOrd, Ord, serde::Serialize)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl<T: fmt::Debug> fmt::Debug for Spanned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.fmt(f)
    }
}

//Deref that allows us to use Spanned<T> as if it were a T, while still retaining the span information. So .inputs() instead of .node.input()
impl<T> Deref for Spanned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

// Helper function to convert SExpr into SpannedExpr with a default span for the files that need use spans
impl From<SExpr> for SpannedExpr {
    fn from(node: SExpr) -> Self {
        Spanned {
            node,
            span: Span::default(),
        }
    }
}

// Helper functions to create SpannedExprs without having to specify the span every time for quicker migration to the spans
#[allow(non_snake_case)]
impl SpannedExpr {
    pub fn with_span(node: SExpr, span: Span) -> Self {
        Spanned {
            node: node,
            span: span,
        }
    }
    pub fn VarAt(v: VarName, span: Span) -> Self {
        Self::with_span(SExpr::Var(v), span)
    }

    pub fn Val(v: impl Into<Value>) -> Self {
        SExpr::Val(v.into()).into()
    }

    pub fn Var(v: VarName) -> Self {
        SExpr::Var(v).into()
    }

    pub fn BinOp<L, R>(lhs: Box<L>, rhs: Box<R>, op: SBinOp) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::BinOp(Box::new((*lhs).into()), Box::new((*rhs).into()), op).into()
    }

    pub fn If<C, T, E>(c: Box<C>, t: Box<T>, e: Box<E>) -> Self
    where
        C: Into<SpannedExpr>,
        T: Into<SpannedExpr>,
        E: Into<SpannedExpr>,
    {
        SExpr::If(
            Box::new((*c).into()),
            Box::new((*t).into()),
            Box::new((*e).into()),
        )
        .into()
    }

    pub fn SIndex<E>(e: Box<E>, idx: u64) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::SIndex(Box::new((*e).into()), idx).into()
    }

    pub fn Init<L, R>(lhs: Box<L>, rhs: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::Init(Box::new((*lhs).into()), Box::new((*rhs).into())).into()
    }

    pub fn Dynamic<E>(e: Box<E>, t: StreamTypeAscription) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Dynamic(Box::new((*e).into()), t).into()
    }

    pub fn RestrictedDynamic<E>(e: Box<E>, t: StreamTypeAscription, vs: EcoVec<VarName>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::RestrictedDynamic(Box::new((*e).into()), t, vs).into()
    }

    pub fn Defer<E>(e: Box<E>, t: StreamTypeAscription, vs: EcoVec<VarName>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Defer(Box::new((*e).into()), t, vs).into()
    }

    pub fn Default<L, R>(l: Box<L>, r: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::Default(Box::new((*l).into()), Box::new((*r).into())).into()
    }

    pub fn When<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::When(Box::new((*e).into())).into()
    }

    pub fn Update<L, R>(l: Box<L>, r: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::Update(Box::new((*l).into()), Box::new((*r).into())).into()
    }

    pub fn Latch<V, T>(v: Box<V>, t: Box<T>) -> Self
    where
        V: Into<SpannedExpr>,
        T: Into<SpannedExpr>,
    {
        SExpr::Latch(Box::new((*v).into()), Box::new((*t).into())).into()
    }

    pub fn Not<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Not(Box::new((*e).into())).into()
    }

    pub fn IsDefined<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::IsDefined(Box::new((*e).into())).into()
    }

    pub fn Sin<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Sin(Box::new((*e).into())).into()
    }

    pub fn Cos<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Cos(Box::new((*e).into())).into()
    }

    pub fn Tan<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Tan(Box::new((*e).into())).into()
    }

    pub fn Abs<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::Abs(Box::new((*e).into())).into()
    }

    pub fn Map(map: BTreeMap<EcoString, SpannedExpr>) -> Self {
        SExpr::Map(map).into()
    }

    pub fn Struct(map: BTreeMap<EcoString, SpannedExpr>) -> Self {
        SExpr::Struct(map).into()
    }

    pub fn ObjectLiteral(map: BTreeMap<EcoString, SpannedExpr>) -> Self {
        SExpr::ObjectLiteral(map).into()
    }

    pub fn List(items: EcoVec<SpannedExpr>) -> Self {
        SExpr::List(items).into()
    }

    pub fn LIndex<L, R>(lhs: Box<L>, rhs: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::LIndex(Box::new((*lhs).into()), Box::new((*rhs).into())).into()
    }

    pub fn LAppend<L, R>(lhs: Box<L>, rhs: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::LAppend(Box::new((*lhs).into()), Box::new((*rhs).into())).into()
    }

    pub fn LConcat<L, R>(lhs: Box<L>, rhs: Box<R>) -> Self
    where
        L: Into<SpannedExpr>,
        R: Into<SpannedExpr>,
    {
        SExpr::LConcat(Box::new((*lhs).into()), Box::new((*rhs).into())).into()
    }

    pub fn LHead<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::LHead(Box::new((*e).into())).into()
    }

    pub fn LTail<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::LTail(Box::new((*e).into())).into()
    }

    pub fn LLen<E>(e: Box<E>) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::LLen(Box::new((*e).into())).into()
    }

    pub fn MGet<E>(e: Box<E>, key: EcoString) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::MGet(Box::new((*e).into()), key).into()
    }

    pub fn SGet<E>(e: Box<E>, key: EcoString) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::SGet(Box::new((*e).into()), key).into()
    }

    pub fn MInsert<M, V>(map: Box<M>, key: EcoString, value: Box<V>) -> Self
    where
        M: Into<SpannedExpr>,
        V: Into<SpannedExpr>,
    {
        SExpr::MInsert(Box::new((*map).into()), key, Box::new((*value).into())).into()
    }

    pub fn MRemove<E>(e: Box<E>, key: EcoString) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::MRemove(Box::new((*e).into()), key).into()
    }

    pub fn MHasKey<E>(e: Box<E>, key: EcoString) -> Self
    where
        E: Into<SpannedExpr>,
    {
        SExpr::MHasKey(Box::new((*e).into()), key).into()
    }
}

//Helper function to wrap an SExpr in a SpannedExpr with a default span (0..0) for winnow parsing,
pub fn span_wrapper_winnow(
    source: &str,
    start_rest: &str,
    end_rest: &str,
    node: SExpr,
) -> SpannedExpr {
    Spanned {
        node,
        span: offset(source, start_rest, end_rest),
    }
}

// Helper function to calculate the offset for the span
#[inline]
pub fn offset(source: &str, rest_start: &str, rest_end: &str) -> Span {
    let start = source.len() - rest_start.len();
    let end = source.len() - rest_end.len();

    Span::new(start as u32, end as u32)
}

pub fn strip_span(e: &Spanned<SExpr>) -> String {
    match &e.node {
        SExpr::Var(v) => format!("Var({:?})", v),
        SExpr::Val(v) => format!("Val({:?})", v),

        SExpr::If(a, b, c) => format!(
            "If({}, {}, {})",
            strip_span(a),
            strip_span(b),
            strip_span(c),
        ),

        SExpr::BinOp(l, r, op) => format!("BinOp({}, {}, {:?})", strip_span(l), strip_span(r), op,),

        SExpr::SIndex(e, idx) => format!("SIndex({}, {})", strip_span(e), idx),
        SExpr::Dynamic(e, t) => format!("Dynamic({}, {:?})", strip_span(e), t),
        SExpr::RestrictedDynamic(e, t, vs) => {
            format!("RestrictedDynamic({}, {:?}, {:?})", strip_span(e), t, vs)
        }
        SExpr::Defer(e, t, vs) => format!("Defer({}, {:?}, {:?})", strip_span(e), t, vs),
        SExpr::Update(l, r) => format!("Update({}, {})", strip_span(l), strip_span(r)),
        SExpr::Default(l, r) => format!("Default({}, {})", strip_span(l), strip_span(r)),
        SExpr::IsDefined(e) => format!("IsDefined({})", strip_span(e)),
        SExpr::When(e) => format!("When({})", strip_span(e)),
        SExpr::Latch(v, t) => format!("Latch({}, {})", strip_span(v), strip_span(t)),
        SExpr::Init(e1, e2) => format!("Init({}, {})", strip_span(e1), strip_span(e2)),
        SExpr::Not(e) => format!("Not({})", strip_span(e)),
        SExpr::List(es) => {
            let items = es
                .iter()
                .map(|e| strip_span(e))
                .collect::<Vec<_>>()
                .join(", ");
            format!("List([{}])", items)
        }
        SExpr::LAppend(lst, e1) => format!("LAppend({}, {})", strip_span(lst), strip_span(e1)),
        SExpr::LIndex(e, i) => format!("LIndex({}, {})", strip_span(e), strip_span(i)),
        SExpr::LConcat(lst, e1) => format!("LConcat({}, {})", strip_span(lst), strip_span(e1)),
        SExpr::LHead(e) => format!("LHead({})", strip_span(e)),
        SExpr::LTail(e) => format!("LTail({})", strip_span(e)),
        SExpr::LLen(e) => format!("LLen({})", strip_span(e)),

        SExpr::Map(map) => {
            let items = map
                .iter()
                .map(|(k, v)| format!("{:?}: {}", k, strip_span(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Map({{{}}})", items)
        }
        SExpr::Struct(map) => {
            let items = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, strip_span(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Struct({{{}}})", items)
        }
        SExpr::ObjectLiteral(map) => {
            let items = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, strip_span(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ObjectLiteral({{{}}})", items)
        }
        SExpr::MGet(e, k) => format!("MGet({}, {:?})", strip_span(e), k),
        SExpr::SGet(e, k) => format!("SGet({}, {})", strip_span(e), k),
        SExpr::MInsert(e, k, v) => {
            format!("MInsert({}, {:?}, {})", strip_span(e), k, strip_span(v))
        }
        SExpr::MRemove(e, k) => format!("MRemove({}, {:?})", strip_span(e), k),

        SExpr::MHasKey(e, k) => format!("MHasKey({}, {:?})", strip_span(e), k),

        SExpr::Sin(e) => format!("Sin({})", strip_span(e)),
        SExpr::Cos(e) => format!("Cos({})", strip_span(e)),
        SExpr::Tan(e) => format!("Tan({})", strip_span(e)),
        SExpr::Abs(e) => format!("Abs({})", strip_span(e)),
        SExpr::MonitoredAt(v, n) => format!("MonitoredAt({}, {})", v, n),
        SExpr::Dist(v, u) => format!("Dist({}, {})", v, u),
    }
}

pub trait SpanStrippedDisplay {
    fn span_stripped_str(&self) -> String;
}

impl SpanStrippedDisplay for Spanned<SExpr> {
    fn span_stripped_str(&self) -> String {
        format!("Ok({})", strip_span(self))
    }
}

impl<E: fmt::Debug> SpanStrippedDisplay for Result<Spanned<SExpr>, E> {
    fn span_stripped_str(&self) -> String {
        match self {
            Ok(expr) => format!("Ok({})", strip_span(expr)),
            Err(err) => format!("Err({err:?})"),
        }
    }
}

pub fn presult_strip_span<T: SpanStrippedDisplay + ?Sized>(e: &T) -> String {
    e.span_stripped_str()
}

#[cfg(test)]
mod tests {
    use super::{Span, Spanned};
    use std::collections::{BTreeSet, HashSet};

    #[test]
    fn spanned_equality_hash_and_order_include_span() {
        let a = Spanned {
            node: 7,
            span: Span::new(0, 1),
        };
        let b = Spanned {
            node: 7,
            span: Span::new(10, 20),
        };

        assert_ne!(a, b);

        let mut hash_set = HashSet::new();
        hash_set.insert(a);
        hash_set.insert(b);
        assert_eq!(hash_set.len(), 2);

        let mut btree_set = BTreeSet::new();
        btree_set.insert(a);
        btree_set.insert(b);
        assert_eq!(btree_set.len(), 2);
    }
}
