use std::fmt;
use std::ops::{Deref, Range};

use crate::{
    ExprKind,
    lang::dsrv::ast::{DynamicExprScope, Expr, ExprFields},
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

// Generic Spanned struct that can be used to wrap any node with a span, used for the ExprKind nodes in the AST to keep track of their location in the source code.
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

pub fn offset(source: &str, rest_start: &str, rest_end: &str) -> Span {
    let start = source.len() - rest_start.len();
    let end = source.len() - rest_end.len();
    Span::new(start as u32, end as u32)
}

pub fn span_wrapper_winnow(source: &str, start_rest: &str, end_rest: &str, expr: Expr) -> Expr {
    crate::lang::dsrv::ast::finish_root_span(expr, offset(source, start_rest, end_rest))
}

pub(crate) fn strip_span(expr: &Expr) -> String {
    let child = |id| strip_span(&expr.child(id));
    let children = |ids: &[crate::lang::dsrv::ast::ExprId]| {
        ids.iter()
            .map(|id| child(*id))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let keyed = |items: &ExprFields| {
        items
            .iter()
            .map(|(key, id)| format!("{key:?}: {}", child(*id)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match expr.as_ref().kind() {
        ExprKind::Var(v) => format!("Var({v:?})"),
        ExprKind::Val(v) => format!("Val({v:?})"),
        ExprKind::If(a, b, c) => format!("If({}, {}, {})", child(*a), child(*b), child(*c)),
        ExprKind::BinOp(a, b, op) => format!("BinOp({}, {}, {op:?})", child(*a), child(*b)),
        ExprKind::SIndex(e, index) => format!("SIndex({}, {index})", child(*e)),
        ExprKind::Dynamic(source, result_type, scope) => match scope {
            DynamicExprScope::Automatic => {
                format!("Dynamic({}, {:?})", child(*source), result_type)
            }
            DynamicExprScope::Explicit(vars) => format!(
                "RestrictedDynamic({}, {:?}, {vars:?})",
                child(*source),
                result_type
            ),
        },
        ExprKind::Defer(source, result_type, scope) => format!(
            "Defer({}, {:?}, {:?})",
            child(*source),
            result_type,
            scope.explicit_vars().cloned().unwrap_or_default()
        ),
        ExprKind::Update(a, b) => format!("Update({}, {})", child(*a), child(*b)),
        ExprKind::Default(a, b) => format!("Default({}, {})", child(*a), child(*b)),
        ExprKind::Latch(a, b) => format!("Latch({}, {})", child(*a), child(*b)),
        ExprKind::Init(a, b) => format!("Init({}, {})", child(*a), child(*b)),
        ExprKind::Not(e) => format!("Not({})", child(*e)),
        ExprKind::IsDefined(e) => format!("IsDefined({})", child(*e)),
        ExprKind::When(e) => format!("When({})", child(*e)),
        ExprKind::Lambda(params, body) => format!("Lambda({params:?}, {})", child(*body)),
        ExprKind::Apply(func, args) => format!("Apply({}, [{}])", child(*func), children(args)),
        ExprKind::Fix(func) => format!("Fix({})", child(*func)),
        ExprKind::Partial(func, args) => format!("Partial({}, [{}])", child(*func), children(args)),
        ExprKind::List(items) => format!("List([{}])", children(items)),
        ExprKind::Tuple(items) => format!("Tuple([{}])", children(items)),
        ExprKind::LIndex(a, b) => format!("LIndex({}, {})", child(*a), child(*b)),
        ExprKind::LAppend(a, b) => format!("LAppend({}, {})", child(*a), child(*b)),
        ExprKind::LConcat(a, b) => format!("LConcat({}, {})", child(*a), child(*b)),
        ExprKind::LHead(e) => format!("LHead({})", child(*e)),
        ExprKind::LTail(e) => format!("LTail({})", child(*e)),
        ExprKind::LLen(e) => format!("LLen({})", child(*e)),
        ExprKind::LMap(a, b) => format!("LMap({}, {})", child(*a), child(*b)),
        ExprKind::LFilter(a, b) => format!("LFilter({}, {})", child(*a), child(*b)),
        ExprKind::LFold(a, b, c) => format!("LFold({}, {}, {})", child(*a), child(*b), child(*c)),
        ExprKind::Map(items) => format!("Map({{{}}})", keyed(items)),
        ExprKind::Struct(items) => format!("Struct({{{}}})", keyed(items)),
        ExprKind::ObjectLiteral(items) => format!("ObjectLiteral({{{}}})", keyed(items)),
        ExprKind::MGet(e, key) => format!("MGet({}, {key:?})", child(*e)),
        ExprKind::SGet(e, key) => format!("SGet({}, {key})", child(*e)),
        ExprKind::MInsert(e, key, value) => {
            format!("MInsert({}, {key:?}, {})", child(*e), child(*value))
        }
        ExprKind::MRemove(e, key) => format!("MRemove({}, {key:?})", child(*e)),
        ExprKind::MHasKey(e, key) => format!("MHasKey({}, {key:?})", child(*e)),
        ExprKind::Sin(e) => format!("Sin({})", child(*e)),
        ExprKind::Cos(e) => format!("Cos({})", child(*e)),
        ExprKind::Tan(e) => format!("Tan({})", child(*e)),
        ExprKind::Abs(e) => format!("Abs({})", child(*e)),
        ExprKind::MonitoredAt(var, node) => format!("MonitoredAt({var}, {node})"),
        ExprKind::Dist(a, b) => format!("Dist({a}, {b})"),
    }
}

#[cfg(test)]
pub(crate) trait SpanStrippedDisplay {
    fn span_stripped_str(&self) -> String;
}

#[cfg(test)]
impl SpanStrippedDisplay for Expr {
    fn span_stripped_str(&self) -> String {
        format!("Ok({})", strip_span(self))
    }
}

#[cfg(test)]
impl<E: fmt::Debug> SpanStrippedDisplay for Result<Expr, E> {
    fn span_stripped_str(&self) -> String {
        match self {
            Ok(expr) => format!("Ok({})", strip_span(expr)),
            Err(err) => format!("Err({err:?})"),
        }
    }
}

#[cfg(test)]
pub(crate) fn presult_strip_span<T: SpanStrippedDisplay + ?Sized>(value: &T) -> String {
    value.span_stripped_str()
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
