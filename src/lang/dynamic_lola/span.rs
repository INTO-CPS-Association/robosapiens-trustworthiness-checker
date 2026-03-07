use serde;
use std::ops::{Deref, Range};

use crate::{SExpr, lang::dynamic_lola::ast::SpannedExpr};

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

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Debug, serde::Serialize)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

//Deref that allows us to use Spanned<T> as if it were a T, while still retaining the span information. So .inputs() instead of .node.input()
impl<T> Deref for Spanned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.node
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
