#[cfg(test)]
use std::fmt;
use std::ops::Range;

#[cfg(test)]
use crate::lang::dsrv::ast::Expr;

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

#[cfg(test)]
pub(crate) fn strip_span(expr: &Expr) -> String {
    strip_span_ref(expr.as_ref())
}

#[cfg(test)]
pub(crate) fn strip_span_ref(expr: crate::lang::dsrv::ast::ExprRef<'_>) -> String {
    format!("{expr:?}")
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
