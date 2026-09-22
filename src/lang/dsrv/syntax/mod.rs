//! Parsing DSRV source text into the private parsed tree.
//!
//! This stage resolves no names and builds no semantic nodes: type names and
//! declarations are kept exactly as written, for [`super::expand`] to resolve.

use std::fmt;
use std::sync::LazyLock;

use lalrpop_util::lalrpop_mod;

pub(crate) mod parsed;

lalrpop_mod!(lalr, "/lang/dsrv/syntax/lalr.rs");

use self::lalr::{DeclarationsParser, ExprParser};
pub(crate) use parsed::{
    ParsedDeclaration, ParsedExpr, ParsedSpecification, SourceAscription, SpanningBuilder,
};

static EXPR_PARSER: LazyLock<ExprParser> = LazyLock::new(ExprParser::new);
static DECLARATIONS_PARSER: LazyLock<DeclarationsParser> = LazyLock::new(DeclarationsParser::new);

/// A failure while reading source text.
#[derive(Debug, thiserror::Error)]
pub enum DsrvSyntaxError {
    #[error("invalid DSRV syntax: {0}")]
    Syntax(#[source] anyhow::Error),

    #[error("invalid DSRV specification: {0}")]
    Ast(#[from] super::ast::DsrvAstError),
}

/// Parse a whole specification into its parsed declarations and expressions.
pub(crate) fn parse_specification(input: &str) -> Result<ParsedSpecification, DsrvSyntaxError> {
    let mut builder = SpanningBuilder::with_capacity(input.len() / 4);
    let mut user_error_location = None;
    let declarations = DECLARATIONS_PARSER
        .parse(&mut builder, &mut user_error_location, input)
        .map_err(|error| syntax_error(input, user_error_location, error))?;
    ParsedSpecification::new(builder.into_inner(), declarations)
}

/// Parse a whole specification that is a program of its own, archiving its
/// text as that program's only file.
pub(crate) fn parse_archived_specification(
    input: &str,
    label: super::source_map::SourceLabel,
) -> Result<(ParsedSpecification, super::source_map::SourceArchive), DsrvSyntaxError> {
    use super::source_map::{SourceArchive, SourceFile};
    let parsed = parse_specification(input)?;
    let (archive, source) = SourceArchive::single(SourceFile::new(label, Vec::new(), input));
    Ok((parsed.with_source(source), archive))
}

/// Parse one expression, as supplied at runtime by `dynamic` or `defer`.
pub(crate) fn parse_expression(input: &str) -> Result<ParsedExpr, DsrvSyntaxError> {
    let mut builder = SpanningBuilder::with_capacity(input.len() / 4);
    let mut user_error_location = None;
    let root = EXPR_PARSER
        .parse(&mut builder, &mut user_error_location, input)
        .map_err(|error| syntax_error(input, user_error_location, error))?;
    Ok(builder
        .into_inner()
        .finish(root)
        .map_err(super::ast::DsrvAstError::from)?)
}

struct LineCol {
    line: usize,
    col: usize,
}

impl fmt::Display for LineCol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "line {}, column {}", self.line, self.col)
    }
}

// Converts a byte offset into a line and a column
fn line_col(input: &str, byte: usize) -> LineCol {
    let byte = byte.min(input.len());
    let mut line = 1usize;
    let mut col = 1usize;

    for ch in input[..byte].chars() {
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    LineCol { line, col }
}

fn syntax_error(
    input: &str,
    user_error_location: Option<usize>,
    error: lalrpop_util::ParseError<usize, lalrpop_util::lexer::Token<'_>, &str>,
) -> DsrvSyntaxError {
    let err_fixed = error.map_location(|byte| line_col(input, byte));
    let location = user_error_location
        .map(|byte| format!(" near {}", line_col(input, byte)))
        .unwrap_or_default();
    DsrvSyntaxError::Syntax(anyhow::anyhow!(err_fixed.to_string()).context(format!(
        "Failed to parse input {input}{location}: {err_fixed}"
    )))
}
