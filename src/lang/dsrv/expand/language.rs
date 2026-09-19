//! Language settings: the dialect and edition a specification declares,
//! resolved once during expansion.
//!
//! Later stages see the settings only as part of the source fingerprint,
//! through the dialect (which decides admission) and through the header text.

use std::fmt;
use std::str::FromStr;

use ecow::EcoString;

use crate::core::StreamType;
use crate::lang::dsrv::ast::{DsrvSpecification, ExprKind, ExprRef, SemanticEntry};
use crate::lang::dsrv::span::Span;
use crate::lang::dsrv::syntax::ParsedDeclaration;

/// Which language a specification is written in. The dialects are nested:
/// Core ⊂ Full ⊂ Distributed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Dialect {
    /// The basic temporal language, closest to the published calculus.
    Core,
    /// The whole language, and the default.
    #[default]
    Full,
    /// Full DSRV plus the distribution primitives.
    Distributed,
}

impl Dialect {
    /// The name used after `language` in a header.
    fn header_name(self) -> Option<&'static str> {
        match self {
            Self::Core => Some("core"),
            Self::Full => None,
            Self::Distributed => Some("distributed"),
        }
    }

    fn from_header_name(name: &str) -> Option<Self> {
        [Self::Core, Self::Distributed]
            .into_iter()
            .find(|dialect| dialect.header_name() == Some(name))
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Core => "Core DSRV",
            Self::Full => "Full DSRV",
            Self::Distributed => "Distributed DSRV",
        })
    }
}

/// A dated set of default behaviours. Each edition is a variant, so an
/// edition is always one the checker knows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub enum Edition {
    /// The language as of 16 September 2026: eager `if` and failing lookups.
    #[default]
    E2026_09,
}

impl Edition {
    /// The edition of a specification that names none.
    pub const BASE: Self = Self::E2026_09;
    /// Every edition, oldest first.
    pub const ALL: &'static [Self] = &[Self::E2026_09];

    fn year_month(self) -> (u16, u8) {
        match self {
            Self::E2026_09 => (2026, 9),
        }
    }

    fn known() -> String {
        Self::ALL
            .iter()
            .map(Self::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for Edition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month) = self.year_month();
        write!(f, "{year}-{month:02}")
    }
}

impl FromStr for Edition {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|edition| edition.to_string() == text)
            .ok_or_else(|| {
                format!(
                    "unknown edition `{text}`; known editions: {}",
                    Self::known()
                )
            })
    }
}

/// The resolved settings of one specification.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct LanguageConfig {
    dialect: Dialect,
    edition: Edition,
}

impl LanguageConfig {
    /// The dialect decides which runtimes may admit a specification.
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// The header lines that declare these settings; empty for the defaults.
    pub(crate) fn header(&self) -> String {
        let Self { dialect, edition } = self;
        let mut header = String::new();
        if let Some(name) = dialect.header_name() {
            header.push_str(&format!("language {name}\n"));
        }
        if *edition != Edition::BASE {
            header.push_str(&format!("edition {edition}\n"));
        }
        header
    }
}

impl fmt::Display for LanguageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, edition {}", self.dialect, self.edition)
    }
}

/// Settings requested from outside a file, such as on the command line.
/// They apply to a file that does not declare them; a file that declares
/// something different is an error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LanguageRequest {
    pub dialect: Option<Dialect>,
    pub edition: Option<Edition>,
}

/// A problem with a specification's language settings.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LanguageError {
    #[error("unknown language `{name}` at {span:?}; expected `core` or `distributed`")]
    UnknownDialect { name: EcoString, span: Span },

    #[error("unknown edition `{text}` at {span:?}; known editions: {known}")]
    UnknownEdition {
        text: EcoString,
        known: String,
        span: Span,
    },

    #[error("a second `{keyword}` line at {span:?}; the first is at {first:?}")]
    DuplicateHeader {
        keyword: &'static str,
        first: Span,
        span: Span,
    },

    #[error("`{keyword}` at {span:?} must come before every other declaration")]
    HeaderAfterDeclaration { keyword: &'static str, span: Span },

    #[error("the file declares {declared} but {requested} was requested")]
    RequestConflict { declared: String, requested: String },

    #[error("{construct} at {span:?} is not part of Core DSRV")]
    NotCore { construct: &'static str, span: Span },

    #[error("{construct} at {span:?} needs `language distributed`")]
    NeedsDistributed { construct: &'static str, span: Span },
}

/// Read the header declarations and combine them with any outside request.
pub(crate) fn resolve_language(
    declarations: &[ParsedDeclaration],
    request: LanguageRequest,
) -> Result<LanguageConfig, LanguageError> {
    let mut dialect: Option<(Dialect, Span)> = None;
    let mut edition: Option<(Edition, Span)> = None;
    let mut body_started = false;

    for declaration in declarations {
        let (keyword, span) = match declaration {
            ParsedDeclaration::Language(_, span) => ("language", *span),
            ParsedDeclaration::Edition(_, span) => ("edition", *span),
            _ => {
                body_started = true;
                continue;
            }
        };
        if body_started {
            return Err(LanguageError::HeaderAfterDeclaration { keyword, span });
        }
        match declaration {
            ParsedDeclaration::Language(name, span) => {
                if let Some((_, first)) = dialect {
                    return Err(LanguageError::DuplicateHeader {
                        keyword,
                        first,
                        span: *span,
                    });
                }
                let resolved = Dialect::from_header_name(name).ok_or_else(|| {
                    LanguageError::UnknownDialect {
                        name: name.clone(),
                        span: *span,
                    }
                })?;
                dialect = Some((resolved, *span));
            }
            ParsedDeclaration::Edition(text, span) => {
                if let Some((_, first)) = edition {
                    return Err(LanguageError::DuplicateHeader {
                        keyword,
                        first,
                        span: *span,
                    });
                }
                let resolved =
                    text.parse::<Edition>()
                        .map_err(|_| LanguageError::UnknownEdition {
                            text: text.clone(),
                            known: Edition::known(),
                            span: *span,
                        })?;
                edition = Some((resolved, *span));
            }
            _ => unreachable!("only header declarations reach here"),
        }
    }

    let dialect = reconcile(
        dialect.map(|(value, _)| value),
        request.dialect,
        |dialect| match dialect.header_name() {
            Some(name) => format!("`language {name}`"),
            None => "Full DSRV".to_owned(),
        },
    )?;
    let edition = reconcile(
        edition.map(|(value, _)| value),
        request.edition,
        |edition| format!("edition {edition}"),
    )?;
    Ok(LanguageConfig { dialect, edition })
}

/// A setting declared in the file wins only when the request agrees with it.
fn reconcile<T: Copy + PartialEq + Default>(
    declared: Option<T>,
    requested: Option<T>,
    describe: impl Fn(T) -> String,
) -> Result<T, LanguageError> {
    match (declared, requested) {
        (Some(declared), Some(requested)) if declared != requested => {
            Err(LanguageError::RequestConflict {
                declared: describe(declared),
                requested: describe(requested),
            })
        }
        (Some(value), _) | (None, Some(value)) => Ok(value),
        (None, None) => Ok(T::default()),
    }
}

/// A specification checked to be Core DSRV. Only [`CoreDsrvSpecification::check`]
/// constructs one, so a function that takes this type only ever sees Core.
#[derive(Clone, Debug)]
pub struct CoreDsrvSpecification(DsrvSpecification);

impl CoreDsrvSpecification {
    /// Accept a specification whose dialect is Core and which uses only Core
    /// constructs and types.
    pub fn check(specification: DsrvSpecification) -> Result<Self, LanguageError> {
        let dialect = specification.source_context().language().dialect();
        if dialect != Dialect::Core {
            return Err(LanguageError::RequestConflict {
                declared: dialect.to_string(),
                requested: Dialect::Core.to_string(),
            });
        }
        for node in specification.nodes() {
            check_core_node(node)?;
        }
        for entry in specification.semantic_entries() {
            match entry {
                SemanticEntry::Input {
                    annotation: Some(ty),
                    span,
                    ..
                }
                | SemanticEntry::Output {
                    annotation: Some(ty),
                    span,
                    ..
                }
                | SemanticEntry::Aux {
                    annotation: Some(ty),
                    span,
                    ..
                } => check_core_type(ty, *span)?,
                SemanticEntry::Input { .. }
                | SemanticEntry::Output { .. }
                | SemanticEntry::Aux { .. }
                | SemanticEntry::Assignment { .. } => {}
            }
        }
        Ok(Self(specification))
    }

    pub fn specification(&self) -> &DsrvSpecification {
        &self.0
    }

    pub fn into_specification(self) -> DsrvSpecification {
        self.0
    }
}

/// Check one expression tree against Core, as runtime sources are.
pub(crate) fn is_core_fragment(root: ExprRef<'_>) -> Result<(), LanguageError> {
    use contiguous_tree::TreeCursorExt;
    root.postorder().try_for_each(check_core_node)
}

/// The distribution primitives belong to Distributed DSRV only. Core
/// specifications report them through the Core check instead.
pub(crate) fn check_dialect_node(node: ExprRef<'_>, dialect: Dialect) -> Result<(), LanguageError> {
    let construct = match node.kind() {
        ExprKind::MonitoredAt(..) => "`monitored_at`",
        ExprKind::Dist(..) => "`dist`",
        _ => return Ok(()),
    };
    match dialect {
        Dialect::Distributed => Ok(()),
        Dialect::Core => check_core_node(node),
        Dialect::Full => Err(LanguageError::NeedsDistributed {
            construct,
            span: node.span(),
        }),
    }
}

/// Every expression kind is listed, with no wildcard, so a new kind must be
/// placed inside or outside Core before this compiles.
pub(crate) fn check_core_node(node: ExprRef<'_>) -> Result<(), LanguageError> {
    use crate::lang::dsrv::ast::SyntaxLiteral;
    let construct = match node.kind() {
        ExprKind::Val(value) => match value {
            SyntaxLiteral::Int(_)
            | SyntaxLiteral::Float(_)
            | SyntaxLiteral::Str(_)
            | SyntaxLiteral::Bool(_)
            | SyntaxLiteral::Unit
            | SyntaxLiteral::NoVal => None,
            SyntaxLiteral::List(_) => Some("a list literal"),
            SyntaxLiteral::Tuple(_) => Some("a tuple literal"),
            SyntaxLiteral::Map(_) => Some("a map literal"),
            SyntaxLiteral::Struct(_) => Some("a struct literal"),
        },
        ExprKind::If(..)
        | ExprKind::SIndex(..)
        | ExprKind::BinOp(..)
        | ExprKind::Var(..)
        | ExprKind::Dynamic(..)
        | ExprKind::Defer(..)
        | ExprKind::Update(..)
        | ExprKind::Default(..)
        | ExprKind::IsDefined(..)
        | ExprKind::When(..)
        | ExprKind::Latch(..)
        | ExprKind::Init(..)
        | ExprKind::Not(..)
        | ExprKind::Neg(..) => None,
        ExprKind::Lambda(..) => Some("a lambda"),
        ExprKind::Apply(..) => Some("a function call"),
        ExprKind::Fix(..) => Some("`fix`"),
        ExprKind::Partial(..) => Some("`partial`"),
        ExprKind::List(..) => Some("a list"),
        ExprKind::Tuple(..) => Some("a tuple"),
        ExprKind::LIndex(..)
        | ExprKind::LAppend(..)
        | ExprKind::LConcat(..)
        | ExprKind::LHead(..)
        | ExprKind::LTail(..)
        | ExprKind::LLen(..)
        | ExprKind::LMap(..)
        | ExprKind::LFilter(..)
        | ExprKind::LFold(..) => Some("a list operation"),
        ExprKind::Map(..) => Some("a map"),
        ExprKind::Struct(..) => Some("a struct"),
        ExprKind::ObjectLiteral(..) => Some("an object literal"),
        ExprKind::MGet(..)
        | ExprKind::MInsert(..)
        | ExprKind::MRemove(..)
        | ExprKind::MHasKey(..) => Some("a map operation"),
        ExprKind::SGet(..) => Some("a field read"),
        ExprKind::Sin(..) => Some("`sin`"),
        ExprKind::Cos(..) => Some("`cos`"),
        ExprKind::Tan(..) => Some("`tan`"),
        ExprKind::Abs(..) => Some("`abs`"),
        ExprKind::MonitoredAt(..) => Some("`monitored_at`"),
        ExprKind::Dist(..) => Some("`dist`"),
    };
    match construct {
        Some(construct) => Err(LanguageError::NotCore {
            construct,
            span: node.span(),
        }),
        None => Ok(()),
    }
}

/// Core has the scalar types, the gradual type, and `Expr` of those.
pub(crate) fn check_core_type(ty: &StreamType, span: Span) -> Result<(), LanguageError> {
    let construct = match ty {
        StreamType::Int
        | StreamType::Float
        | StreamType::Str
        | StreamType::Bool
        | StreamType::Unit
        | StreamType::Any => return Ok(()),
        StreamType::Expr(inner) => return check_core_type(inner, span),
        StreamType::List(_) => "a list type",
        StreamType::Tuple(_) => "a tuple type",
        StreamType::Map(_) => "a map type",
        StreamType::Struct(..) => "a struct type",
        StreamType::Function(..) => "a function type",
    };
    Err(LanguageError::NotCore { construct, span })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::{
        DsrvParseError, check_core_source, parse_expr_with_context, parse_str, parse_str_with,
    };

    fn language_of(source: &str) -> LanguageConfig {
        parse_str(source)
            .unwrap()
            .source_context()
            .language()
            .clone()
    }

    fn language_error(source: &str) -> LanguageError {
        language_error_with(source, LanguageRequest::default())
    }

    fn language_error_with(source: &str, request: LanguageRequest) -> LanguageError {
        match parse_str_with(source, request) {
            Err(DsrvParseError::Language(error)) => error,
            other => panic!("expected a language error for {source:?}, got {other:?}"),
        }
    }

    fn config(dialect: Dialect) -> LanguageConfig {
        LanguageConfig {
            dialect,
            edition: Edition::BASE,
        }
    }

    const BODY: &str = "in x: Int\nout y: Int\ny = x + 1\n";

    // R3.1-a
    #[test]
    fn a_file_without_a_header_is_full_base_edition() {
        assert_eq!(language_of(BODY), LanguageConfig::default());
        assert_eq!(LanguageConfig::default(), config(Dialect::Full));
    }

    // R3.1-b
    #[test]
    fn each_header_form_resolves() {
        let cases: [(&str, LanguageConfig); 3] = [
            ("language core\n", config(Dialect::Core)),
            ("language distributed\n", config(Dialect::Distributed)),
            ("edition 2026-09\n", config(Dialect::Full)),
        ];
        for (header, expected) in cases {
            assert_eq!(
                language_of(&format!("{header}{BODY}")),
                expected,
                "{header}"
            );
        }
        assert_eq!(
            language_of(&format!("language distributed\nedition 2026-09\n{BODY}")),
            config(Dialect::Distributed)
        );
    }

    // R3.2
    #[test]
    fn header_errors_name_the_problem() {
        use LanguageError::*;
        let cases: [(&str, fn(&LanguageError) -> bool); 6] = [
            (
                "language paper\n",
                |e| matches!(e, UnknownDialect { name, .. } if name == "paper"),
            ),
            (
                "edition 2027-03\n",
                |e| matches!(e, UnknownEdition { text, known, .. } if text == "2027-03" && known == "2026-09"),
            ),
            (
                "edition 2026-9\n",
                |e| matches!(e, UnknownEdition { text, .. } if text == "2026-9"),
            ),
            ("edition 2026-13\n", |e| matches!(e, UnknownEdition { .. })),
            ("language core\nlanguage core\n", |e| {
                matches!(
                    e,
                    DuplicateHeader {
                        keyword: "language",
                        ..
                    }
                )
            }),
            ("edition 2026-09\nedition 2026-09\n", |e| {
                matches!(
                    e,
                    DuplicateHeader {
                        keyword: "edition",
                        ..
                    }
                )
            }),
        ];
        for (header, check) in cases {
            let error = language_error(&format!("{header}{BODY}"));
            assert!(check(&error), "{header}: {error}");
        }
        let late = language_error(&format!("{BODY}edition 2026-09\n"));
        assert!(
            matches!(
                late,
                HeaderAfterDeclaration {
                    keyword: "edition",
                    ..
                }
            ),
            "{late}"
        );
    }

    // R3.3
    #[test]
    fn header_keywords_are_reserved_but_the_names_they_take_are_not() {
        for source in ["out edition\nedition = 1", "in language\n"] {
            assert!(
                matches!(parse_str(source), Err(DsrvParseError::Syntax(_))),
                "{source}"
            );
        }
        for source in [
            "in core\nout y\ny = core",
            "in distributed\nout y\ny = distributed",
        ] {
            parse_str(source).unwrap_or_else(|error| panic!("{source}: {error}"));
        }
    }

    // R3.4-a
    #[test]
    fn a_core_file_yields_a_core_specification() {
        let source = "in x: Int\nout y: Int\ny = if x > 0 then default(x[1], 0) else -x\n";
        let core = check_core_source(source).unwrap();
        assert_eq!(
            core.specification().source_context().language().dialect(),
            Dialect::Core
        );
        // A header that agrees with the request is fine; another dialect is not.
        check_core_source(&format!("language core\n{source}")).unwrap();
        assert!(matches!(
            check_core_source(&format!("language distributed\n{source}")),
            Err(DsrvParseError::Language(
                LanguageError::RequestConflict { .. }
            ))
        ));
        // A Full specification is never Core, even if its constructs are.
        assert!(CoreDsrvSpecification::check(parse_str(source).unwrap()).is_err());
    }

    // R3.4-b
    #[test]
    fn every_construct_outside_core_is_named_at_its_span() {
        let cases = [
            ("sin(x)", "`sin`"),
            ("cos(x)", "`cos`"),
            ("tan(x)", "`tan`"),
            ("abs(x)", "`abs`"),
            ("List(x)", "a list"),
            ("Tuple(x, x)", "a tuple"),
            ("List.len(x)", "a list operation"),
            ("Map(\"a\": x)", "a map"),
            ("Map.get(x, \"a\")", "a map operation"),
            ("Struct(\"a\": x)", "a struct"),
            ("{ a: x }", "an object literal"),
            ("\\v: Int -> v", "a lambda"),
            ("x(x)", "a function call"),
            ("fix(x)", "`fix`"),
            ("partial(x, x)", "`partial`"),
            ("x.a", "a field read"),
            ("monitored_at(x, n)", "`monitored_at`"),
            ("dist(x, n)", "`dist`"),
        ];
        for (expression, construct) in cases {
            let source = format!("language core\nin x\nout y\ny = {expression}\n");
            match language_error(&source) {
                LanguageError::NotCore {
                    construct: found,
                    span,
                } => {
                    assert_eq!(found, construct, "{expression}");
                    let start = source.find(expression).unwrap() as u32;
                    assert!(span.start >= start, "{expression}: {span:?}");
                }
                other => panic!("{expression}: {other}"),
            }
        }
    }

    // R3.4-b: aliases and non-scalar types are outside Core too.
    #[test]
    fn core_rejects_aliases_and_collection_types() {
        assert!(matches!(
            language_error("language core\ntype T = Int\nin x: T\n"),
            LanguageError::NotCore {
                construct: "a type alias",
                ..
            }
        ));
        assert!(matches!(
            language_error("language core\nin x: List<Int>\n"),
            LanguageError::NotCore {
                construct: "a list type",
                ..
            }
        ));
    }

    // R3.4-c
    #[test]
    fn runtime_sources_under_a_core_specification_stay_in_core() {
        let spec = parse_str("language core\nin x\nout y\ny = x\n").unwrap();
        let context = spec.source_context().clone();
        parse_expr_with_context("x + 1", context.clone()).unwrap();
        assert!(matches!(
            parse_expr_with_context("abs(x)", context),
            Err(DsrvParseError::Language(LanguageError::NotCore { .. }))
        ));
    }

    // R3b.1
    #[test]
    fn distribution_primitives_need_the_distributed_dialect() {
        for (expression, construct) in [
            ("monitored_at(x, n)", "`monitored_at`"),
            ("dist(x, n)", "`dist`"),
        ] {
            let body = format!("in x\nout y\ny = {expression}\n");
            let error = language_error(&body);
            let LanguageError::NeedsDistributed {
                construct: found,
                span,
            } = error
            else {
                panic!("{expression}: {error}");
            };
            assert_eq!(found, construct);
            assert_eq!(span.start as usize, body.find(expression).unwrap());
            assert_eq!(
                error.to_string(),
                format!("{construct} at {span:?} needs `language distributed`")
            );
            let distributed = parse_str(&format!("language distributed\n{body}")).unwrap();
            assert_eq!(
                distributed.source_context().language().dialect(),
                Dialect::Distributed
            );
            // Runtime text follows the dialect of the specification it runs in.
            let full = parse_str(BODY).unwrap();
            assert!(matches!(
                parse_expr_with_context(expression, full.source_context().clone()),
                Err(DsrvParseError::Language(
                    LanguageError::NeedsDistributed { .. }
                ))
            ));
            parse_expr_with_context(expression, distributed.source_context().clone()).unwrap();
        }
    }

    // R3.4-d
    #[test]
    fn full_and_distributed_files_are_not_core_checked() {
        parse_str("in x\nout y\ny = abs(x)\n").unwrap();
        parse_str("language distributed\nin x\nout y\ny = abs(x)\n").unwrap();
    }

    // R3.5-a
    #[test]
    fn different_settings_give_different_fingerprints() {
        let full = parse_str(BODY).unwrap();
        let core = parse_str(&format!("language core\n{BODY}")).unwrap();
        let distributed = parse_str(&format!("language distributed\n{BODY}")).unwrap();
        let fingerprints = [&full, &core, &distributed]
            .map(|spec| serde_json::to_string(spec.source_context().fingerprint()).unwrap());
        assert_ne!(fingerprints[0], fingerprints[1]);
        assert_ne!(fingerprints[0], fingerprints[2]);
        assert_ne!(fingerprints[1], fingerprints[2]);
    }

    // R3.7
    #[test]
    fn printing_keeps_the_settings() {
        for header in ["language core\n", "language distributed\n"] {
            let spec = parse_str(&format!("{header}{BODY}")).unwrap();
            let printed = spec.to_string();
            assert!(printed.starts_with(header), "{printed}");
            let reparsed = parse_str(&printed).unwrap();
            assert_eq!(
                reparsed.source_context().language(),
                spec.source_context().language()
            );
            assert_eq!(reparsed, spec);
        }
        assert!(!parse_str(BODY).unwrap().to_string().contains("language"));
    }

    // R3.8-a, b
    #[test]
    fn a_request_applies_only_where_the_file_is_silent() {
        let core = LanguageRequest {
            dialect: Some(Dialect::Core),
            edition: None,
        };
        assert_eq!(
            parse_str_with(BODY, core)
                .unwrap()
                .source_context()
                .language(),
            &config(Dialect::Core)
        );
        let error = language_error_with(&format!("language distributed\n{BODY}"), core);
        assert_eq!(
            error.to_string(),
            "the file declares `language distributed` but `language core` was requested"
        );
        let edition = LanguageRequest {
            dialect: None,
            edition: Some(Edition::BASE),
        };
        parse_str_with(&format!("edition 2026-09\n{BODY}"), edition).unwrap();
    }
}
