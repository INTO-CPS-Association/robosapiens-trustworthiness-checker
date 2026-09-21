//! Public parsing entry points.
//!
//! Parsing itself lives in [`super::syntax`] and name resolution in
//! [`super::expand`]; these functions run both stages for existing callers.

use super::ast::AstShared as Rc;
use crate::lang::dsrv::modules::{
    ImportError, ModuleCollector, ModuleSources, module_file, show_path,
};
use crate::lang::dsrv::path::ModuleName;

use anyhow::Error;
#[cfg(test)]
use anyhow::anyhow;

#[cfg(test)]
use super::ast::Declaration;
use super::ast::{DsrvAstError, Expr, ExprId};
use super::expand::language::{CoreDsrvSpecification, Dialect, LanguageError, LanguageRequest};
use super::expand::{self, DsrvExpandError};
use super::source::{SourceContext, SourceResolveError};
use super::syntax::{self, DsrvSyntaxError};
use crate::DsrvSpecification;

// The test modules below exercise the whole frontend, so they use the
// semantic types the stages produce.
#[cfg(test)]
use super::span::Span;
#[cfg(test)]
use crate::core::StreamType;
#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};

/// A failure while parsing or expanding a DSRV specification.
#[derive(Debug, thiserror::Error)]
pub enum DsrvParseError {
    #[error("invalid DSRV syntax: {0}")]
    Syntax(#[source] anyhow::Error),

    #[error("invalid DSRV specification: {0}")]
    Ast(#[from] DsrvAstError),

    #[error("invalid DSRV source: {0}")]
    Resolve(#[from] SourceResolveError),

    #[error("invalid source-to-semantic tree conversion: {0}")]
    Transcode(#[source] contiguous_tree::TranscodeError<SourceResolveError, ExprId>),

    #[error("invalid language settings: {0}")]
    Language(#[from] LanguageError),

    #[error("invalid import: {0}")]
    Import(#[from] ImportError),

    #[error("invalid module structure: {0}")]
    Modules(String),
}

impl From<DsrvSyntaxError> for DsrvParseError {
    fn from(error: DsrvSyntaxError) -> Self {
        match error {
            DsrvSyntaxError::Syntax(error) => Self::Syntax(error),
            DsrvSyntaxError::Ast(error) => Self::Ast(error),
        }
    }
}

impl From<DsrvExpandError> for DsrvParseError {
    fn from(error: DsrvExpandError) -> Self {
        match error {
            DsrvExpandError::Resolve(error) => Self::Resolve(error),
            DsrvExpandError::Ast(error) => Self::Ast(error),
            DsrvExpandError::Transcode(error) => Self::Transcode(error),
            DsrvExpandError::Language(error) => Self::Language(error),
            DsrvExpandError::Import(error) => Self::Import(error),
            // The message is the whole of these; nothing downstream
            // discriminates them further.
            other @ (DsrvExpandError::ModuleCycle { .. }
            | DsrvExpandError::UnknownModule { .. }
            | DsrvExpandError::UnknownConstructor { .. }
            | DsrvExpandError::UnknownExport { .. }
            | DsrvExpandError::InternalImport { .. }
            | DsrvExpandError::Inlined(_)
            | DsrvExpandError::UnknownFunction { .. }
            | DsrvExpandError::RecursiveFunction { .. }
            | DsrvExpandError::FunctionArity { .. }
            | DsrvExpandError::NotConstant { .. }
            | DsrvExpandError::UnknownConstant { .. }
            | DsrvExpandError::RecursiveConstant { .. }
            | DsrvExpandError::ConstantValue { .. }
            | DsrvExpandError::ConstantOffset { .. }) => Self::Modules(other.to_string()),
        }
    }
}

pub fn parse_expr(input: &str) -> Result<Expr, Error> {
    parse_expr_with_context(input, Rc::new(SourceContext::default())).map_err(|error| {
        let message = format!("Parse error: {error}");
        Error::new(error).context(message)
    })
}

/// Parse an expression using an immutable alias namespace.
/// Types in the returned expression are expanded structural types.
pub fn parse_expr_with_context(
    input: &str,
    context: Rc<SourceContext>,
) -> Result<Expr, DsrvParseError> {
    parse_expr_with_functions(input, context, Rc::default())
}

/// Parse runtime text that may call a `def`.
pub(crate) fn parse_expr_with_functions(
    input: &str,
    context: Rc<SourceContext>,
    callable: Rc<expand::functions::Callable>,
) -> Result<Expr, DsrvParseError> {
    let parsed = syntax::parse_expression(input)?;
    Ok(expand::expand_expression(&parsed, &context, &callable)?)
}

pub fn parse_str(input: &str) -> Result<DsrvSpecification, DsrvParseError> {
    parse_str_with(input, LanguageRequest::default())
}

/// Parse a specification, applying settings requested from outside the file
/// (such as on the command line) where the file declares none.
pub fn parse_str_with(
    input: &str,
    request: LanguageRequest,
) -> Result<DsrvSpecification, DsrvParseError> {
    Ok(expand::expand_specification(
        syntax::parse_specification(input)?,
        request,
    )?)
}

/// Read a program and every module it declares.
///
/// This is the one place modules touch the filesystem: the collector asks
/// for each module in turn and this loop answers, so everything above it
/// stays sans-IO.
pub async fn collect_modules_from_file(file: &str) -> anyhow::Result<ModuleSources> {
    use anyhow::Context;

    let directory = std::path::Path::new(file)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_owned();
    let root = smol::fs::read_to_string(file)
        .await
        .with_context(|| format!("reading {file}"))?;
    let mut collector = ModuleCollector::new(&root)?;
    while let Some(path) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let location = directory.join(module_file(&path));
        let source = smol::fs::read_to_string(&location).await.with_context(|| {
            format!(
                "reading module {} from {}",
                show_path(&path),
                location.display()
            )
        })?;
        collector.supply(&source)?;
    }
    Ok(collector.finish()?)
}

/// A program read from disk and expanded.
pub struct LoadedProgram {
    pub specification: DsrvSpecification,
    /// The root file's text from the same read that produced the
    /// specification, so diagnostics are presented against what was checked.
    pub root_source: String,
}

/// Read a program, its modules included, and expand it.
///
/// A file that declares no module expands as a program of one; one that
/// declares modules has their namespaces built first, in dependency order.
pub async fn parse_program_file(
    file: &str,
    request: LanguageRequest,
) -> anyhow::Result<LoadedProgram> {
    let sources = collect_modules_from_file(file).await?;
    let root_source = sources.root_source().to_owned();
    Ok(LoadedProgram {
        specification: expand::expand_program(sources, request)?,
        root_source,
    })
}

/// Accept a Core DSRV file. A file without a `language` line is read as Core;
/// one that declares another dialect is rejected.
pub fn check_core_source(input: &str) -> Result<CoreDsrvSpecification, DsrvParseError> {
    let request = LanguageRequest {
        dialect: Some(Dialect::Core),
        edition: None,
    };
    let specification = parse_str_with(input, request)?;
    Ok(CoreDsrvSpecification::check(specification)?)
}

/// Run only the syntax stage, so benchmarks can separate it from expansion.
pub fn parse_syntax_for_benchmark(input: &str) -> Result<(), DsrvParseError> {
    syntax::parse_specification(input)?;
    Ok(())
}

#[cfg(test)]
fn presult_to_string<T: std::fmt::Debug, E: std::fmt::Debug>(result: &Result<T, E>) -> String {
    let rendered = format!("{result:?}");
    let Some(start) = rendered.find("declarations: [") else {
        return rendered;
    };
    let mut depth = 0usize;
    let mut end = start;
    for (offset, character) in rendered[start..].char_indices() {
        match character {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    end = start + offset + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let mut compatible = rendered;
    compatible.replace_range(start..end + 2, "");
    compatible
}

#[cfg(test)]
fn parse_declaration(input: &str) -> Result<(Option<Expr>, Declaration), Error> {
    let parsed = syntax::parse_specification(input).map_err(|e| anyhow!("Parse error: {e:?}"))?;
    if parsed.declaration_count() != 1 {
        return Err(anyhow!(
            "expected exactly one declaration, got {}",
            parsed.declaration_count()
        ));
    }
    let expand::ExpandedDeclarations {
        builder,
        declarations,
        roots,
        ..
    } = expand::expand_declarations(parsed, Default::default())?;
    let declaration = declarations
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("expected a stream declaration"))?;
    let expression = match &declaration {
        Declaration::Equation { .. } => {
            let expression = builder
                .finish(roots[0])
                .map_err(|error| anyhow!("Invalid expression tree: {error}"))?;
            if let Some(key) = expression.as_ref().duplicate_field() {
                return Err(anyhow!("duplicate expression field {key:?}"));
            }
            Some(expression)
        }
        _ => {
            builder
                .finish_forest([])
                .map_err(|error| anyhow!("Invalid expression forest: {error}"))?;
            None
        }
    };
    Ok((expression, declaration))
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::lang::dsrv::ast::ExprView;
    use crate::lang::dsrv::path::TypePath;
    use crate::lang::dsrv::source::TypeName;

    #[test]
    fn delimiter_free_aliases_expand_forward_references_in_all_annotations() {
        let spec = parse_str(
            "type State = Struct<speed: Count, stopped: Bool>
             in _ : State
             out result : Count
             result = 2
             aux f
             f = \\x: Count -> dynamic(\"x\": Count)
             aux deferred
             deferred = defer(\"result\": Count)
             type Count = Int",
        )
        .unwrap();
        assert_eq!(
            spec.type_annotation(&"_".into()),
            Some(&StreamType::Struct(
                vec![
                    ("speed".into(), StreamType::Int),
                    ("stopped".into(), StreamType::Bool)
                ]
                .into(),
                false
            ))
        );
        assert_eq!(
            spec.type_annotation(&"result".into()),
            Some(&StreamType::Int)
        );
        let ExprView::Lambda(parameters, body) = spec.var_expr_ref(&"f".into()).unwrap().view()
        else {
            panic!("expected lambda")
        };
        assert_eq!(
            parameters[0].1,
            crate::core::StreamTypeAscription::Ascribed(StreamType::Int)
        );
        assert!(matches!(
            body.view(),
            ExprView::Dynamic(
                _,
                crate::core::StreamTypeAscription::Ascribed(StreamType::Int),
                _
            )
        ));
        assert!(matches!(
            spec.var_expr_ref(&"deferred".into()).unwrap().view(),
            ExprView::Defer(
                _,
                crate::core::StreamTypeAscription::Ascribed(StreamType::Int),
                _
            )
        ));
        assert_eq!(spec.source_context().aliases().len(), 2);
        let displayed = spec.to_string();
        let reparsed = parse_str(&displayed).unwrap();
        assert_eq!(spec.type_annotations(), reparsed.type_annotations());
        assert_eq!(displayed, reparsed.to_string());
    }

    #[test]
    fn aliased_annotations_type_check_structurally() {
        use crate::lang::dsrv::test_support::type_check;
        let spec = parse_str(
            "type P = Struct<x: Int>
             in p: P
             out y: Int
             y = p.x + 1",
        )
        .unwrap();
        assert_eq!(
            spec.type_annotation(&"p".into()),
            Some(&StreamType::Struct(
                vec![("x".into(), StreamType::Int)].into(),
                false
            ))
        );
        assert!(type_check(spec).is_ok());
        let mismatched = parse_str(
            "type P = Struct<x: Int>
             in p: P
             out y: Bool
             y = p.x",
        )
        .unwrap();
        assert!(type_check(mismatched).is_err());
    }

    #[test]
    fn standalone_and_specification_resolution_use_the_same_context() {
        let spec = parse_str("type Count = Int out y y = \\x: Count -> x").unwrap();
        let source = "\\x: Count -> x";
        let standalone = parse_expr_with_context(source, spec.source_context().clone()).unwrap();
        assert_eq!(
            standalone.to_string(),
            spec.var_expr_ref(&"y".into()).unwrap().to_string()
        );
        assert!(matches!(
            parse_expr_with_context(source, Rc::new(SourceContext::default())),
            Err(DsrvParseError::Resolve(SourceResolveError::UnknownAlias { span, .. }))
                if span == Span::new(4, 9)
        ));
    }

    #[test]
    fn namespace_failures_include_unused_aliases_and_original_spans() {
        assert!(matches!(
            parse_str("type A = Int type A = Bool"),
            Err(DsrvParseError::Resolve(
                SourceResolveError::DuplicateAlias { .. }
            ))
        ));
        assert!(matches!(
            parse_str("type A = B type B = A"),
            Err(DsrvParseError::Resolve(
                SourceResolveError::AliasCycle { .. }
            ))
        ));
        assert!(matches!(parse_str("type A = Missing"),
            Err(DsrvParseError::Resolve(SourceResolveError::UnknownAlias { span, .. }))
                if span == Span::new(9, 16)));
        assert!(parse_str("type Int = Bool").is_err());
        assert!(parse_str("type A = Int; out x").is_err());
    }

    #[test]
    fn aliases_nested_types_and_quoted_fields_display_as_self_contained_source() {
        let spec = parse_str(
            "type State = Map<List<Count>>
             type Count = Int
             type Record = Struct<\"type\": State, \"quoted field\": (Count,), ...>
             in row: Record out x: Count x = 1",
        )
        .unwrap();
        let displayed = spec.to_string();
        // Aliases print where they were declared, not sorted by name.
        let aliases: Vec<_> = displayed
            .lines()
            .filter_map(|line| line.strip_prefix("type "))
            .map(|line| line.split(' ').next().unwrap())
            .collect();
        assert_eq!(aliases, ["State", "Count", "Record"]);
        let reparsed = parse_str(&displayed).unwrap();
        assert_eq!(spec.source_context(), reparsed.source_context());
        assert_eq!(spec.type_annotations(), reparsed.type_annotations());
        assert_eq!(displayed, reparsed.to_string());
    }

    #[test]
    fn declarations_keep_source_order_including_type_aliases_but_not_the_header() {
        use crate::lang::dsrv::ast::Declaration;
        let source = "language distributed\nin x\ntype T = Int\nout y: T\ny = x\n";
        let spec = parse_str(source).unwrap();
        let kinds: Vec<String> = spec
            .declarations()
            .iter()
            .map(|declaration| match declaration {
                Declaration::Input { name, .. } => format!("in {name}"),
                Declaration::Output { name, .. } => format!("out {name}"),
                Declaration::Aux { name, .. } => format!("aux {name}"),
                Declaration::Equation { name, .. } => format!("{name} ="),
                Declaration::TypeAlias { name, .. } => format!("type {name}"),
            })
            .collect();
        assert_eq!(kinds, ["in x", "type T", "out y", "y ="]);
        let Declaration::TypeAlias { span, .. } = &spec.declarations()[1] else {
            panic!("the alias is the second declaration");
        };
        let start = source.find("type T").unwrap() as u32;
        assert_eq!(*span, Span::new(start, start + "type T = Int".len() as u32));
        assert_eq!(spec.declarations()[1].stream(), None);
        assert_eq!(
            spec.declarations()[3].stream(),
            Some(&crate::VarName::new("y"))
        );
    }

    #[test]
    fn a_one_line_definition_is_a_declaration_and_an_equation() {
        use crate::lang::dsrv::ast::Declaration;
        for (one_line, two_lines) in [
            (
                "in x: Int\nout y: Int = x + 1",
                "in x: Int\nout y: Int\ny = x + 1",
            ),
            ("in x\nout y = x", "in x\nout y\ny = x"),
            (
                "in x: Int\naux a: Int = x\nout y: Int = a",
                "in x: Int\naux a: Int\na = x\nout y: Int\ny = a",
            ),
            (
                "in x: Int\nvar a: Int = x\nout y: Int = a",
                "in x: Int\naux a: Int\na = x\nout y: Int\ny = a",
            ),
        ] {
            let one = parse_str(one_line).unwrap_or_else(|error| panic!("{one_line}: {error}"));
            assert_eq!(one, parse_str(two_lines).unwrap(), "{one_line}");
            // Printing uses the two-line form and reads back the same.
            assert_eq!(parse_str(&one.to_string()).unwrap(), one, "{one_line}");
        }

        let source = "in x: Int\nout y: Int = x + 1\nout z: Int = y";
        let spec = parse_str(source).unwrap();
        let line = |text: &str| {
            let start = source.find(text).unwrap() as u32;
            Span::new(start, start + text.len() as u32)
        };
        let declarations = spec.declarations();
        assert!(
            matches!(&declarations[1], Declaration::Output { span, .. } if *span == line("out y: Int = x + 1"))
        );
        assert!(
            matches!(&declarations[2], Declaration::Equation { span, .. } if *span == line("out y: Int = x + 1"))
        );
        assert!(matches!(&declarations[3], Declaration::Output { .. }));
        assert!(matches!(&declarations[4], Declaration::Equation { .. }));
        assert_eq!(declarations.len(), 5);
    }

    #[test]
    fn one_line_definitions_are_core_and_follow_the_usual_rules() {
        crate::lang::dsrv::check_core_source("in x: Int\nout y: Int = x[1]").unwrap();
        assert!(matches!(
            parse_str("in x = 1"),
            Err(DsrvParseError::Syntax(_))
        ));
        let error = parse_str("out y = 1\ny = 2").unwrap_err().to_string();
        assert!(
            error.contains("stream y has more than one equation"),
            "{error}"
        );
    }

    #[test]
    fn object_field_shorthand_names_a_stream_by_its_field() {
        let declarations = "in x: Int\nin label: Str\nout o: Struct<x: Int, label: Str>\n";
        for (short, long) in [
            ("o = {x, label}", "o = {x: x, label: label}"),
            ("o = {x, label: \"p\"}", "o = {x: x, label: \"p\"}"),
            ("o = {label: \"p\", x,}", "o = {label: \"p\", x: x}"),
        ] {
            let short_spec = parse_str(&format!("{declarations}{short}"))
                .unwrap_or_else(|error| panic!("{short}: {error}"));
            assert_eq!(
                short_spec,
                parse_str(&format!("{declarations}{long}")).unwrap(),
                "{short}"
            );
            assert_eq!(
                parse_str(&short_spec.to_string()).unwrap(),
                short_spec,
                "{short}"
            );
        }
        crate::dsrv_fixtures::checked_with(
            &format!("{declarations}o = {{x, label}}"),
            crate::lang::dsrv::TypeCheckOptions::STRICT,
        );

        assert!(matches!(
            parse_str(&format!("{declarations}o = {{x, x: 1}}")),
            Err(DsrvParseError::Ast(_))
        ));
        for source in ["out o\no = {and}", "out o\no = {\"x\"}"] {
            assert!(
                matches!(parse_str(source), Err(DsrvParseError::Syntax(_))),
                "{source}"
            );
        }
        // A `dynamic` scope is still a list of streams, not an object.
        parse_str("in source: Str\nin x: Int\nout y: Int\ny = dynamic(source: Int, {x})").unwrap();
    }

    #[test]
    fn a_stream_with_two_equations_names_both() {
        let error = parse_str("out y\ny = 1\ny = 2").unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("stream y has more than one equation"),
            "{message}"
        );
    }

    #[test]
    fn type_namespace_is_separate_from_stream_names_and_underscore_is_an_identifier() {
        let spec =
            parse_str("type _ = Int type Count = _ in Count: Count out _: _ _ = Count").unwrap();
        assert_eq!(spec.type_annotation(&"_".into()), Some(&StreamType::Int));
        assert_eq!(
            spec.source_context()
                .get(&TypePath::local(TypeName::new("_").unwrap())),
            Some(&StreamType::Int)
        );
    }

    #[test]
    fn expression_syntax_errors_report_line_and_column() {
        let error = parse_expr_with_context("1 +\n )", Rc::new(SourceContext::default()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("line 2, column 2"), "{error}");
    }

    #[test]
    fn runtime_context_survives_extraction_and_display() {
        use contiguous_tree::TreeCursorExt;
        let spec = parse_str(
            "type Count = Int type Other = Count
             out x x = Tuple(dynamic(\"1\": Count), defer(\"2\": Other))",
        )
        .unwrap();
        let displayed = spec.to_string();
        assert_eq!(parse_str(&displayed).unwrap().to_string(), displayed);
        let extracted = spec.var_expr(&"x".into()).unwrap();
        let context = spec.source_context().clone();
        drop(spec);
        for node in extracted.as_ref().postorder() {
            if matches!(node.view(), ExprView::Dynamic(..) | ExprView::Defer(..)) {
                assert!(Rc::ptr_eq(
                    node.metadata().context.as_ref().unwrap(),
                    &context
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: Fix the test
    use contiguous_tree::TreeCursorExt;

    use crate::core::StreamType;

    use crate::VarName;
    use crate::core::BinaryOperator;
    use crate::lang::dsrv::ast::{Expr, ExprView, SyntaxLiteral};
    use crate::lang::dsrv::span::Span;

    use crate::core::StreamTypeAscription;
    use crate::lang::dsrv::span::{presult_strip_span, strip_span, strip_span_ref};
    use crate::lang::dsrv::test_support::{
        arb_finite_float_literal_source, arb_invalid_integer_literal_source,
        arb_revised_operator_display_expr, arb_trailing_comma_pair,
        arb_valid_integer_literal_source,
    };
    use proptest::prelude::*;

    use super::*;
    use test_log::test;

    #[test]
    fn unary_minus_is_syntax_faithful_and_preserves_spans() {
        let source = "-42";
        let expression = parse_expr(source).expect("unary minus should parse");
        let ExprView::Neg(operand) = expression.as_ref().view() else {
            panic!("expected Neg root, got {:?}", expression.as_ref().kind());
        };

        assert_eq!(
            expression.as_ref().span(),
            Span::new(0, source.len() as u32)
        );
        assert_eq!(operand.span(), Span::new(1, source.len() as u32));
        assert!(matches!(
            operand.view(),
            ExprView::Val(SyntaxLiteral::Int(42))
        ));
    }

    #[test]
    fn integer_literal_magnitudes_stop_at_i64_max_without_panicking() {
        let expression =
            parse_expr("-9223372036854775807").expect("largest negative magnitude should parse");
        let ExprView::Neg(operand) = expression.as_ref().view() else {
            panic!("largest accepted negative literal should retain a Neg root");
        };
        assert!(matches!(
            operand.view(),
            ExprView::Val(SyntaxLiteral::Int(i64::MAX))
        ));

        for source in [
            "9223372036854775808",
            "-9223372036854775808",
            "999999999999999999999999999999999999999999",
            "-999999999999999999999999999999999999999999",
        ] {
            assert!(parse_expr(source).is_err(), "{source} should be rejected");
        }
    }

    #[test]
    fn interleaved_call_and_field_prefixes_have_exact_spans() {
        let source = "factory()(x).result(y)";
        let expression = parse_expr(source).expect("postfix chain should parse");
        assert_eq!(expression.as_ref().span(), Span::new(0, 22));

        let ExprView::Apply(field_access, mut final_args) = expression.as_ref().view() else {
            panic!("expected final call");
        };
        assert_eq!(field_access.span(), Span::new(0, 19));
        assert!(
            matches!(final_args.next().unwrap().view(), ExprView::Var(name) if name == &VarName::new("y"))
        );

        let ExprView::SGet(second_call, field) = field_access.view() else {
            panic!("expected field access");
        };
        assert_eq!(field, "result");
        assert_eq!(second_call.span(), Span::new(0, 12));

        let ExprView::Apply(first_call, mut second_args) = second_call.view() else {
            panic!("expected second call");
        };
        assert!(
            matches!(second_args.next().unwrap().view(), ExprView::Var(name) if name == &VarName::new("x"))
        );
        assert_eq!(first_call.span(), Span::new(0, 9));

        let ExprView::Apply(factory, first_args) = first_call.view() else {
            panic!("expected first call");
        };
        assert_eq!(first_args.len(), 0);
        assert_eq!(factory.span(), Span::new(0, 7));
        assert!(matches!(factory.view(), ExprView::Var(name) if name == &VarName::new("factory")));
    }

    #[test]
    fn unary_minus_display_roundtrips_for_nested_operands() {
        let expression = parse_expr("-(x + 1)").expect("nested negation should parse");
        let displayed = format!("{expression}");
        let reparsed = parse_expr(&displayed).expect("displayed negation should parse");

        assert_eq!(strip_span(&expression), strip_span(&reparsed));
        assert!(matches!(expression.as_ref().view(), ExprView::Neg(_)));
    }

    /// A compact rendering, with the span, which `Declaration`'s equality
    /// ignores.
    fn declaration_to_string(result: &Result<(Option<Expr>, Declaration), Error>) -> String {
        match result {
            Ok((Some(expression), Declaration::Equation { name, span })) => {
                format!(
                    "Ok(Equation({name:?}, {}, {span:?}))",
                    strip_span(expression)
                )
            }
            Ok((
                _,
                Declaration::Input {
                    name,
                    annotation,
                    span,
                },
            )) => {
                format!("Ok(Input({name:?}, {annotation:?}, {span:?}))")
            }
            Ok((
                _,
                Declaration::Output {
                    name,
                    annotation,
                    span,
                },
            )) => {
                format!("Ok(Output({name:?}, {annotation:?}, {span:?}))")
            }
            Ok((_, declaration)) => format!("Ok({declaration:?})"),
            Err(error) => format!("Err({error:?})"),
        }
    }

    fn assert_specs_eq_ignoring_spans(actual: &DsrvSpecification, expected: &DsrvSpecification) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        assert_eq!(actual.type_annotations, expected.type_annotations);

        let actual_exprs = actual
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        let expected_exprs = expected
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span_ref(expr)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_exprs, expected_exprs);
    }

    #[test]
    fn test_streamdata() {
        let parsed = parse_expr("42");
        let exp = "Ok(Val(Int(42)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("42.0");
        let exp = "Ok(Val(Float(42.0)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        // Unsupported:
        // let parsed = parse_str("1e-1");
        // let exp = "Ok(Val(Float(0.1)))";
        // assert_eq!(presult_to_string(&parsed), exp);

        let parsed = parse_expr("\"abc2d\"");
        let exp = "Ok(Val(Str(\"abc2d\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("true");
        let exp = "Ok(Val(Bool(true)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("false");
        let exp = "Ok(Val(Bool(false)))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("()");
        let exp = "Ok(Val(Unit))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("\"x+y\"");
        let exp = "Ok(Val(Str(\"x+y\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);
    }

    #[test]
    fn test_sexpr() {
        let parsed = parse_expr("1 + 2");
        let exp = "Ok(BinOp(Val(Int(1)), Val(Int(2)), Add))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("1 + 2 * 3");
        let exp = "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), Multiply), Add))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("x + (y + 2)");
        let exp = "Ok(BinOp(Var(VarName::new(\"x\")), BinOp(Var(VarName::new(\"y\")), Val(Int(2)), Add), Add))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("if true then 1 else 2");
        let exp = "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(x)[1]");
        let exp = "Ok(SIndex(Var(VarName::new(\"x\")), 1))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(x + y)[3]");
        let exp = "Ok(SIndex(BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), Add), 3))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("1 + (x)[1]");
        let exp = "Ok(BinOp(Val(Int(1)), SIndex(Var(VarName::new(\"x\")), 1), Add))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("\"test\"");
        let exp = "Ok(Val(Str(\"test\")))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);

        let parsed = parse_expr("(stage == \"m\")");
        let exp = "Ok(BinOp(Var(VarName::new(\"stage\")), Val(Str(\"m\")), Equal))";
        assert_eq!(presult_strip_span(&parsed.unwrap()), exp);
    }

    #[test]
    fn test_input_decl() {
        let parsed = parse_declaration(&mut "in x");
        let exp = r#"Ok(Input(VarName::new("x"), None, Span { start: 0, end: 4 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_declaration(&mut "inx");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_typed_input_decl() {
        let parsed = parse_declaration("in x: Int");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Int), Span { start: 0, end: 9 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        let parsed = parse_declaration("in x: Float");
        let exp = r#"Ok(Input(VarName::new("x"), Some(Float), Span { start: 0, end: 11 }))"#;
        assert_eq!(declaration_to_string(&parsed), exp);

        let input = "in xs: List<Int>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input {
                name: "xs".into(),
                annotation: Some(StreamType::List(Box::new(StreamType::Int))),
                span: Span::new(0, input.len() as u32)
            }
        );

        let input = "in m: Map<List<Bool>>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input {
                name: "m".into(),
                annotation: Some(StreamType::Map(Box::new(StreamType::List(Box::new(
                    StreamType::Bool
                ))))),
                span: Span::new(0, input.len() as u32)
            }
        );

        let input = "in property: Expr<List<Bool>>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input {
                name: "property".into(),
                annotation: Some(StreamType::Expr(Box::new(StreamType::List(Box::new(
                    StreamType::Bool
                ))))),
                span: Span::new(0, input.len() as u32)
            }
        );

        let input = "in robot: Struct<id: Int, label: Str>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input {
                name: "robot".into(),
                annotation: Some(StreamType::Struct(
                    vec![
                        ("id".into(), StreamType::Int),
                        ("label".into(), StreamType::Str),
                    ]
                    .into(),
                    false,
                )),
                span: Span::new(0, input.len() as u32)
            }
        );

        let input = "in robot: Struct<id: Int, ...>";
        assert_eq!(
            parse_declaration(input).unwrap().1,
            Declaration::Input {
                name: "robot".into(),
                annotation: Some(StreamType::Struct(
                    vec![("id".into(), StreamType::Int)].into(),
                    true,
                )),
                span: Span::new(0, input.len() as u32)
            }
        );

        // Not sure if we should allow this, but this is how it currently works. As long as we
        // start with "in"
        let parsed = parse_declaration("inx:Int");
        assert_eq!(parsed.is_err(), true);
        let err = parsed.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn test_parse_dsrv_simple_add() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    BinaryOperator::Add,
                ),
            )]),
            BTreeMap::new(),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_simple_add_typed() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor_typed();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    BinaryOperator::Add,
                ),
            )]),
            BTreeMap::from([
                (VarName::new("x"), StreamType::Int),
                (VarName::new("y"), StreamType::Int),
                (VarName::new("z"), StreamType::Int),
            ]),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_simple_add_float_typed() {
        let input = crate::dsrv_fixtures::spec_simple_add_monitor_typed_float();
        let simple_add_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from(["z".into()]),
            BTreeMap::from([(
                "z".into(),
                Expr::BinOp(
                    Box::new(Expr::Var("x".into())),
                    Box::new(Expr::Var("y".into())),
                    BinaryOperator::Add,
                ),
            )]),
            BTreeMap::from([
                ("x".into(), StreamType::Float),
                ("y".into(), StreamType::Float),
                ("z".into(), StreamType::Float),
            ]),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &simple_add_spec);
    }

    #[test]
    fn test_parse_dsrv_count() {
        let input = "\
            out x\n\
            x = 1 + (x)[1]";
        let count_spec = DsrvSpecification::new(
            BTreeSet::from([]),
            BTreeSet::from(["x".into()]),
            BTreeMap::from([(
                "x".into(),
                Expr::BinOp(
                    Box::new(Expr::Val(1)),
                    Box::new(Expr::SIndex(Box::new(Expr::Var("x".into())), 1)),
                    BinaryOperator::Add,
                ),
            )]),
            BTreeMap::new(),
            Vec::<VarName>::new(),
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &count_spec);
    }

    #[test]
    fn test_parse_dsrv_dynamic() {
        let input = "\
            in x\n\
            in y\n\
            in s\n\
            out z\n\
            out w\n\
            z = x + y\n\
            w = dynamic(s)";
        let dynamic_spec = DsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into(), "s".into()]),
            BTreeSet::from(["z".into(), "w".into()]),
            BTreeMap::from([
                (
                    "z".into(),
                    Expr::BinOp(
                        Box::new(Expr::Var("x".into())),
                        Box::new(Expr::Var("y".into())),
                        BinaryOperator::Add,
                    ),
                ),
                (
                    "w".into(),
                    Expr::Dynamic(
                        Box::new(Expr::Var("s".into())),
                        StreamTypeAscription::Unascribed,
                    ),
                ),
            ]),
            BTreeMap::new(),
            vec![],
        );
        let spec = parse_str(input);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_specs_eq_ignoring_spans(&spec, &dynamic_spec);
    }

    #[test]
    fn test_unary() {
        assert_eq!(
            presult_strip_span(&parse_expr("-1").unwrap()),
            "Ok(Neg(Val(Int(1))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-1.0").unwrap()),
            "Ok(Neg(Val(Float(1.0))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-x").unwrap()),
            r#"Ok(Neg(Var(VarName::new("x"))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-(1+2)").unwrap()),
            "Ok(Neg(BinOp(Val(Int(1)), Val(Int(2)), Add)))"
        );
    }

    #[test]
    fn test_float_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0.0").unwrap()),
            "Ok(Val(Float(0.0)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("-1.0").unwrap()),
            "Ok(Neg(Val(Float(1.0))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 +2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  + 2.0 +3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), Add), Val(Float(3.0)), Add))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 -2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), Subtract))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  - 2.0 -3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), Subtract), Val(Float(3.0)), Subtract))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 *2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), Multiply))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1.0  * 2.0 *3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Float(2.0)), Multiply), Val(Float(3.0)), Multiply))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("  1.0 /2.0  ").unwrap()),
            "Ok(BinOp(Val(Float(1.0)), Val(Float(2.0)), Divide))"
        );
    }

    #[test]
    fn test_mixed_float_int_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0.0 + 2").unwrap()),
            "Ok(BinOp(Val(Float(0.0)), Val(Int(2)), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 + 2 + 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), Add), Val(Float(3.0)), Add))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("1 - 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), Subtract))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 - 2 - 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), Subtract), Val(Float(3.0)), Subtract))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), Multiply))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1.0 * 2 * 3.0").unwrap()),
            "Ok(BinOp(BinOp(Val(Float(1.0)), Val(Int(2)), Multiply), Val(Float(3.0)), Multiply))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("1 / 2.0").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Float(2.0)), Divide))"
        );
    }

    #[test]
    fn test_integer_exprs() {
        // Add
        assert_eq!(
            presult_strip_span(&parse_expr("0").unwrap()),
            "Ok(Val(Int(0)))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  1 +2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  + 2 +3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), Val(Int(3)), Add))"
        );
        // Sub
        assert_eq!(
            presult_strip_span(&parse_expr("  1 -2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Subtract))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  - 2 -3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Subtract), Val(Int(3)), Subtract))"
        );
        // Mul
        assert_eq!(
            presult_strip_span(&parse_expr("  1 *2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Multiply))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  * 2 *3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Multiply), Val(Int(3)), Multiply))"
        );
        // Div
        assert_eq!(
            presult_strip_span(&parse_expr("  1 /2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Divide))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  / 2 /3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Divide), Val(Int(3)), Divide))"
        );
        // Mod
        assert_eq!(
            presult_strip_span(&parse_expr("  1 %2  ").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Modulo))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr(" 1  % 2 %3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Modulo), Val(Int(3)), Modulo))"
        );
        // Var
        assert_eq!(
            presult_strip_span(&parse_expr("  x  ").unwrap()),
            r#"Ok(Var(VarName::new("x")))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("  xsss ").unwrap()),
            r#"Ok(Var(VarName::new("xsss")))"#
        );
        // Time index
        assert_eq!(
            presult_strip_span(&parse_expr("x [1]").unwrap()),
            r#"Ok(SIndex(Var(VarName::new("x")), 1))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x[1 ]").unwrap()),
            r#"Ok(SIndex(Var(VarName::new("x")), 1))"#
        );
        // Paren
        assert_eq!(
            presult_strip_span(&parse_expr("  (1)  ").unwrap()),
            "Ok(Val(Int(1)))"
        );
        // Don't care about order of eval; care about what the AST looks like
        assert_eq!(
            presult_strip_span(&parse_expr(" 2 + (2 + 3)").unwrap()),
            "Ok(BinOp(Val(Int(2)), BinOp(Val(Int(2)), Val(Int(3)), Add), Add))"
        );
        // If then else
        assert_eq!(
            presult_strip_span(&parse_expr("if true then 1 else 2").unwrap()),
            "Ok(If(Val(Bool(true)), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("if true then x+x else y+y").unwrap()),
            r#"Ok(If(Val(Bool(true)), BinOp(Var(VarName::new("x")), Var(VarName::new("x")), Add), BinOp(Var(VarName::new("y")), Var(VarName::new("y")), Add)))"#
        );

        // ChatGPT generated tests with mixed arithmetic and parentheses iexprs. It only had knowledge of the tests above.
        // Basic mixed addition and multiplication
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 * 3").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), Multiply), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2 + 3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Multiply), Val(Int(3)), Add))"
        );
        // Mixed addition, subtraction, and multiplication
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 * 3 - 4").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), BinOp(Val(Int(2)), Val(Int(3)), Multiply), Add), Val(Int(4)), Subtract))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 * 2 + 3 - 4").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Multiply), Val(Int(3)), Add), Val(Int(4)), Subtract))"
        );
        // Mixed addition and division
        assert_eq!(
            presult_strip_span(&parse_expr("10 + 20 / 5").unwrap()),
            "Ok(BinOp(Val(Int(10)), BinOp(Val(Int(20)), Val(Int(5)), Divide), Add))"
        );
        // Nested parentheses with mixed operations
        assert_eq!(
            presult_strip_span(&parse_expr("(1 + 2) * (3 - 4)").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), BinOp(Val(Int(3)), Val(Int(4)), Subtract), Multiply))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + (2 * (3 + 4))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(4)), Add), Multiply), Add))"
        );
        // Complex nested expressions
        assert_eq!(
            presult_strip_span(&parse_expr("((1 + 2) * 3) + (4 / (5 - 6))").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), Val(Int(3)), Multiply), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), Subtract), Divide), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("(1 + (2 * (3 - (4 / 5))))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), Val(Int(5)), Divide), Subtract), Multiply), Add))"
        );
        // More complex expressions with deep nesting
        assert_eq!(
            presult_strip_span(&parse_expr("((1 + 2) * (3 + 4))").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), BinOp(Val(Int(3)), Val(Int(4)), Add), Multiply))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("((1 * 2) + (3 * 4)) / 5").unwrap()),
            "Ok(BinOp(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Multiply), BinOp(Val(Int(3)), Val(Int(4)), Multiply), Add), Val(Int(5)), Divide))"
        );
        // Multiple levels of nested expressions
        assert_eq!(
            presult_strip_span(&parse_expr("1 + (2 * (3 + (4 / (5 - 6))))").unwrap()),
            "Ok(BinOp(Val(Int(1)), BinOp(Val(Int(2)), BinOp(Val(Int(3)), BinOp(Val(Int(4)), BinOp(Val(Int(5)), Val(Int(6)), Subtract), Divide), Add), Multiply), Add))"
        );

        // ChatGPT generated tests with mixed iexprs. It only had knowledge of the tests above.
        // Mixing addition, subtraction, and variables
        assert_eq!(
            presult_strip_span(&parse_expr("x + 2 - y").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Val(Int(2)), Add), Var(VarName::new("y")), Subtract))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("(x + y) * 3").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), Add), Val(Int(3)), Multiply))"#
        );
        // Nested arithmetic with variables and parentheses
        assert_eq!(
            presult_strip_span(&parse_expr("(a + b) / (c - d)").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("a")), Var(VarName::new("b")), Add), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), Subtract), Divide))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x * (y + 3) - z / 2").unwrap()),
            r#"Ok(BinOp(BinOp(Var(VarName::new("x")), BinOp(Var(VarName::new("y")), Val(Int(3)), Add), Multiply), BinOp(Var(VarName::new("z")), Val(Int(2)), Divide), Subtract))"#
        );
        // If-then-else with mixed arithmetic
        assert_eq!(
            presult_strip_span(&parse_expr("if true then 1 + 2 else 3 * 4").unwrap()),
            "Ok(If(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), Add), BinOp(Val(Int(3)), Val(Int(4)), Multiply)))"
        );
        // Time index in arithmetic expression
        assert_eq!(
            presult_strip_span(&parse_expr("x[0] + y[1]").unwrap()),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 0), SIndex(Var(VarName::new("y")), 1), Add))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x[1] * (y + 3)").unwrap()),
            r#"Ok(BinOp(SIndex(Var(VarName::new("x")), 1), BinOp(Var(VarName::new("y")), Val(Int(3)), Add), Multiply))"#
        );
        // Case to test precedence of if-then-else with arithmetic
        // Most languages implement this as "if a then b else (c + d)" and so should we.
        // Programmers can write "(if a then b else c) + d" if they want the other behavior.
        assert_eq!(
            presult_strip_span(&parse_expr("if a then b else c + d").unwrap()),
            r#"Ok(If(Var(VarName::new("a")), Var(VarName::new("b")), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), Add)))"#
        );
    }

    // NOTE: I have not been able to find a way to parse this expression. Starting to believe it is not possible with LALR(1).
    // The issue is: We don't have any identifiers determining when the else branch ends.
    // So if we want "if a then b else c + d" to be (c + d), it must be implemented with if
    // expressions having lower precedence than arithmetic.
    // But then we can't parse "1 + if a then b else c" because we cannot the if-statement comes
    // earlier in the precedence chain...
    //
    // Note that I haven't found any other parsers using LALRPop that was able to resolve this.
    // One of the most advanced are:
    // https://github.com/Storyyeller/cubiml-demo/tree/master?tab=readme-ov-file
    // and it has the same issue...
    // See also: https://github.com/lalrpop/lalrpop/issues/1022 and
    // https://github.com/lalrpop/lalrpop/issues/705 for analogous issues
    //
    // (OCaml has syntax similar to ours, and the only way they can support this type of grammar is by
    // using %prec macros. They use the #prec macro to make a special case.)
    //
    // The user can wrap the if-statement in parentheses to get around this.
    // We should probably change our syntax to be easier to support
    #[ignore]
    #[test]
    fn test_ambiguous_case() {
        assert_eq!(
            presult_to_string(&parse_expr("1 + if a then b else c")),
            r#""#
        );
    }

    #[test]
    fn test_assignment_decl() {
        assert_eq!(
            declaration_to_string(&parse_declaration("x = 0")),
            r#"Ok(Equation(VarName::new("x"), Val(Int(0)), Span { start: 0, end: 5 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration(r#"x = "hello""#)),
            r#"Ok(Equation(VarName::new("x"), Val(Str("hello")), Span { start: 0, end: 11 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("x = true")),
            r#"Ok(Equation(VarName::new("x"), Val(Bool(true)), Span { start: 0, end: 8 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("x = false")),
            r#"Ok(Equation(VarName::new("x"), Val(Bool(false)), Span { start: 0, end: 9 }))"#
        );
    }

    #[test]
    fn test_parse_empty_string() {
        let res = parse_str("");
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(
            res,
            DsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, Expr>::new(),
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    fn test_parse_invalid_expression() {
        let res = parse_expr("1 +");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));

        let res = parse_expr("&& true");
        assert_eq!(res.is_err(), true);
        let err = res.err().unwrap();
        assert!(err.to_string().contains("Parse error"));
    }

    #[test]
    fn identifiers_and_whitespace_are_ascii() {
        assert!(parse_expr("ascii_name_1").is_ok());
        assert!(parse_expr("xé").is_err());
        assert!(parse_expr("x\u{a0}+ 1").is_err());
        assert_eq!(
            presult_strip_span(&parse_expr("x\x0b+\x0c1").unwrap()),
            "Ok(BinOp(Var(VarName::new(\"x\")), Val(Int(1)), Add))"
        );
    }

    #[test]
    fn strings_and_comments_retain_unicode_support() {
        assert_eq!(
            presult_strip_span(&parse_expr("\"Zażółć gęślą jaźń 🤖\"").unwrap()),
            "Ok(Val(Str(\"Zażółć gęślą jaźń 🤖\")))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 // komentarz 日本語 🤖\n + 2").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Add))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 (* комментарий 🤖 *) + 2").unwrap()),
            "Ok(BinOp(Val(Int(1)), Val(Int(2)), Add))"
        );
    }

    #[test]
    fn duplicate_assignments_are_ast_validation_errors() {
        let source = "out x\nx = 1\nx = 2";
        let error = parse_str(source).unwrap_err();
        let DsrvParseError::Ast(DsrvAstError::DuplicateEquation {
            variable,
            first,
            duplicate,
        }) = error
        else {
            panic!("expected a duplicate-assignment AST error, got {error:?}");
        };

        assert_eq!(variable, VarName::new("x"));
        assert_eq!(&source[first.to_range()], "x = 1");
        assert_eq!(&source[duplicate.to_range()], "x = 2");
    }

    #[test]
    fn duplicate_assignment_errors_follow_source_order() {
        let source = "out z\nout a\nz = 0\na = 0\nz = 1\na = 1";
        let error = parse_str(source).unwrap_err();
        let DsrvParseError::Ast(DsrvAstError::DuplicateEquation {
            variable,
            first,
            duplicate,
        }) = error
        else {
            panic!("expected a duplicate-assignment AST error, got {error:?}");
        };

        assert_eq!(variable, VarName::new("z"));
        assert_eq!(&source[first.to_range()], "z = 0");
        assert_eq!(&source[duplicate.to_range()], "z = 1");
    }

    #[test]
    fn test_parse_boolean_expressions() {
        assert_eq!(
            presult_strip_span(&parse_expr("true && false").unwrap()),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), And))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("true || false").unwrap()),
            "Ok(BinOp(Val(Bool(true)), Val(Bool(false)), Or))"
        );
    }

    #[test]
    fn test_parse_mixed_boolean_and_arithmetic() {
        // Expressions do not make sense but parser should allow it
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 && 3").unwrap()),
            "Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), Val(Int(3)), And))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("true || 1 * 2").unwrap()),
            "Ok(BinOp(Val(Bool(true)), BinOp(Val(Int(1)), Val(Int(2)), Multiply), Or))"
        );
    }

    #[test]
    fn test_parse_string_concatenation() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#""foo" ++ "bar""#).unwrap()),
            r#"Ok(BinOp(Val(Str("foo")), Val(Str("bar")), Concatenate))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#""hello" ++ " " ++ "world""#).unwrap()),
            r#"Ok(BinOp(BinOp(Val(Str("hello")), Val(Str(" ")), Concatenate), Val(Str("world")), Concatenate))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#""a" ++ "b" ++ "c""#).unwrap()),
            r#"Ok(BinOp(BinOp(Val(Str("a")), Val(Str("b")), Concatenate), Val(Str("c")), Concatenate))"#
        );
    }

    #[test]
    fn test_parse_defer() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"defer(x)"#).unwrap()),
            r#"Ok(Defer(Var(VarName::new("x")), Unascribed, Automatic))"#
        )
    }

    #[test]
    fn test_parse_update() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"update(x, y)"#).unwrap()),
            r#"Ok(Update(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_parse_default() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"default(x, 0)"#).unwrap()),
            r#"Ok(Default(Var(VarName::new("x")), Val(Int(0))))"#
        )
    }

    #[test]
    fn test_parse_default_parse_expr() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"default(x, y)"#).unwrap()),
            r#"Ok(Default(Var(VarName::new("x")), Var(VarName::new("y"))))"#
        )
    }

    #[test]
    fn test_when() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"when(x)"#).unwrap()),
            r#"Ok(When(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_is_defined() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"is_defined(x)"#).unwrap()),
            r#"Ok(IsDefined(Var(VarName::new("x"))))"#
        )
    }

    #[test]
    fn test_parse_list() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"[]"#).unwrap()),
            r#"Ok(List([]))"#,
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"[1, 2]"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Int(2))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List()"#).unwrap()),
            r#"Ok(List([]))"#,
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List () "#).unwrap()),
            r#"Ok(List([]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1,2)"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Int(2))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1+2,2*5)"#).unwrap()),
            r#"Ok(List([BinOp(Val(Int(1)), Val(Int(2)), Add), BinOp(Val(Int(2)), Val(Int(5)), Multiply)]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List("hello","world")"#).unwrap()),
            r#"Ok(List([Val(Str("hello")), Val(Str("world"))]))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(true || false, true && false)"#).unwrap()),
            r#"Ok(List([BinOp(Val(Bool(true)), Val(Bool(false)), Or), BinOp(Val(Bool(true)), Val(Bool(false)), And)]))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List(1,"hello")"#).unwrap()),
            r#"Ok(List([Val(Int(1)), Val(Str("hello"))]))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("y = List()")),
            r#"Ok(Equation(VarName::new("y"), List([]), Span { start: 0, end: 10 }))"#
        )
    }

    #[test]
    fn test_parse_lindex() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(List(1, 2), 42)"#).unwrap()),
            r#"Ok(LIndex(List([Val(Int(1)), Val(Int(2))]), Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(x, 42)"#).unwrap()),
            r#"Ok(LIndex(Var(VarName::new("x")), Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.get(x, 1+2)"#).unwrap()),
            r#"Ok(LIndex(Var(VarName::new("x")), BinOp(Val(Int(1)), Val(Int(2)), Add)))"#
        );
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"List.get(List.get(List(List(1, 2), List(3, 4)), 0), 1)"#).unwrap()
            ),
            r#"Ok(LIndex(LIndex(List([List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])]), Val(Int(0))), Val(Int(1))))"#
        );
    }

    #[test]
    fn test_parse_lconcat() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.concat(List(1, 2), List(3, 4))"#).unwrap()),
            r#"Ok(LConcat(List([Val(Int(1)), Val(Int(2))]), List([Val(Int(3)), Val(Int(4))])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.concat(List(), List())"#).unwrap()),
            r#"Ok(LConcat(List([]), List([])))"#
        );
    }

    #[test]
    fn test_parse_lappend() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(1, 2), 3)"#).unwrap()),
            r#"Ok(LAppend(List([Val(Int(1)), Val(Int(2))]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(), 3)"#).unwrap()),
            r#"Ok(LAppend(List([]), Val(Int(3))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.append(List(), x)"#).unwrap()),
            r#"Ok(LAppend(List([]), Var(VarName::new("x"))))"#
        );
    }

    #[test]
    fn test_parse_lhead() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.head(List(1, 2))"#).unwrap()),
            r#"Ok(LHead(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.head(List())"#).unwrap()),
            r#"Ok(LHead(List([])))"#
        );
    }

    #[test]
    fn test_parse_ltail() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.tail(List(1, 2))"#).unwrap()),
            r#"Ok(LTail(List([Val(Int(1)), Val(Int(2))])))"#
        );
        // Ok for parser but will result in runtime error:
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.tail(List())"#).unwrap()),
            r#"Ok(LTail(List([])))"#
        );
    }

    #[test]
    fn test_parse_llen() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.len(List(1, 2))"#).unwrap()),
            r#"Ok(LLen(List([Val(Int(1)), Val(Int(2))])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"List.len(List())"#).unwrap()),
            r#"Ok(LLen(List([])))"#
        );
    }

    #[test]
    fn test_parse_dynamic_type_ascription() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"dynamic(x: Int)"#).unwrap()),
            r#"Ok(Dynamic(Var(VarName::new("x")), Ascribed(Int), Automatic))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"dynamic(x: Int, {x, y})"#).unwrap()),
            r#"Ok(Dynamic(Var(VarName::new("x")), Ascribed(Int), Explicit([VarName::new("x"), VarName::new("y")])))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"defer(x: Int)"#).unwrap()),
            r#"Ok(Defer(Var(VarName::new("x")), Ascribed(Int), Automatic))"#
        );
    }

    #[test]
    fn test_parse_map() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map()"#).unwrap()),
            r#"Ok(Map({}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"{"x": 1, "y": 2}"#).unwrap()),
            r#"Ok(ObjectLiteral({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"{x: 1, y: 2}"#).unwrap()),
            r#"Ok(ObjectLiteral({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert!(parse_expr(r#"Map(x: 1)"#).is_err());
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": 1, "y": 2)"#).unwrap()),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Int(2))}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": 1+2,"y": 2*5)"#).unwrap()),
            r#"Ok(Map({"x": BinOp(Val(Int(1)), Val(Int(2)), Add), "y": BinOp(Val(Int(2)), Val(Int(5)), Multiply)}))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map("x": "hello", "y": "world")"#).unwrap()),
            r#"Ok(Map({"x": Val(Str("hello")), "y": Val(Str("world"))}))"#
        );
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"Map("xxxx": true || false, "yyyy": true && false)"#).unwrap()
            ),
            r#"Ok(Map({"xxxx": BinOp(Val(Bool(true)), Val(Bool(false)), Or), "yyyy": BinOp(Val(Bool(true)), Val(Bool(false)), And)}))"#
        );
        // Can mix expressions - not that it is necessarily a good idea
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map( "x": 1, "y": "hello" )"#).unwrap()),
            r#"Ok(Map({"x": Val(Int(1)), "y": Val(Str("hello"))}))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("y = Map()")),
            r#"Ok(Equation(VarName::new("y"), Map({}), Span { start: 0, end: 9 }))"#
        )
    }

    #[test]
    fn keyed_expression_fields_preserve_source_order() {
        let expr = parse_expr(r#"Map("z": 1, "a": 2, "m": 3)"#).unwrap();
        let crate::lang::dsrv::ast::ExprKind::Map(fields) = expr.as_ref().kind() else {
            panic!("expected a map expression");
        };
        assert_eq!(
            fields
                .keys()
                .map(|key| AsRef::<str>::as_ref(key))
                .collect::<Vec<_>>(),
            ["z", "a", "m"]
        );
    }

    #[test]
    fn keyed_expression_fields_reject_duplicates() {
        assert!(parse_expr(r#"Map("x": 1, "x": 2)"#).is_err());
        assert!(parse_expr(r#"Struct("x": 1, "x": 2)"#).is_err());
        assert!(parse_expr(r#"{x: 1, x: 2}"#).is_err());
    }

    #[test]
    fn test_parse_mget() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MGet(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(x, "key")"#).unwrap()),
            r#"Ok(MGet(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.get(x, "")"#).unwrap()),
            r#"Ok(MGet(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.get(Map.get(Map.get(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MGet(MGet(MGet(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.id"#).unwrap()),
            r#"Ok(SGet(Var(VarName::new("robot")), "id"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.pose.x + 1"#).unwrap()),
            r#"Ok(BinOp(SGet(SGet(Var(VarName::new("robot")), "pose"), "x"), Val(Int(1)), Add))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Struct("id": 1).id"#).unwrap()),
            r#"Ok(SGet(Struct({"id": Val(Int(1))}), "id"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot.sensor_value"#).unwrap()),
            r#"Ok(SGet(Var(VarName::new("robot")), "sensor_value"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"robot_A.pose.x + 1"#).unwrap()),
            r#"Ok(BinOp(SGet(SGet(Var(VarName::new("robot_A")), "pose"), "x"), Val(Int(1)), Add))"#
        );
    }

    #[test]
    fn test_parse_mremove() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MRemove(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(x, "key")"#).unwrap()),
            r#"Ok(MRemove(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.remove(x, "")"#).unwrap()),
            r#"Ok(MRemove(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.remove(Map.remove(Map.remove(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MRemove(MRemove(MRemove(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_mhas_key() {
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(Map("x": 2, "y": true), "x")"#).unwrap()),
            r#"Ok(MHasKey(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "x"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(x, "key")"#).unwrap()),
            r#"Ok(MHasKey(Var(VarName::new("x")), "key"))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.has_key(x, "")"#).unwrap()),
            r#"Ok(MHasKey(Var(VarName::new("x")), ""))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(
                r#"Map.has_key(Map.has_key(Map.has_key(Map("three": Map("two": Map("one": 42))), "three"), "two"), "one")"#
            ).unwrap()),
            r#"Ok(MHasKey(MHasKey(MHasKey(Map({"three": Map({"two": Map({"one": Val(Int(42))})})}), "three"), "two"), "one"))"#
        );
    }

    #[test]
    fn test_parse_minsert() {
        assert_eq!(
            presult_strip_span(
                &parse_expr(r#"Map.insert(Map("x": 2, "y": true), "z", 42)"#).unwrap()
            ),
            r#"Ok(MInsert(Map({"x": Val(Int(2)), "y": Val(Bool(true))}), "z", Val(Int(42))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.insert(x, "key", true)"#).unwrap()),
            r#"Ok(MInsert(Var(VarName::new("x")), "key", Val(Bool(true))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr(r#"Map.insert(x, "", 1)"#).unwrap()),
            r#"Ok(MInsert(Var(VarName::new("x")), "", Val(Int(1))))"#
        );
    }

    #[test]
    fn test_dangling_else() {
        assert_eq!(
            presult_strip_span(&parse_expr("if a then b else c + d").unwrap()),
            r#"Ok(If(Var(VarName::new("a")), Var(VarName::new("b")), BinOp(Var(VarName::new("c")), Var(VarName::new("d")), Add)))"#
        )
    }

    // SYN-R01: revised operators remain distinct AST nodes and retain source spans.
    #[test]
    fn syntax_revision_operators_are_first_class_and_display_roundtrip() {
        for (source, operator) in [
            ("a != b", BinaryOperator::NotEqual),
            ("a ** b", BinaryOperator::Power),
        ] {
            let expression = parse_expr(source).expect("revised operator should parse");
            let ExprView::BinOp(left, right, actual) = expression.as_ref().view() else {
                panic!("expected a binary expression for {source}");
            };
            assert_eq!(actual, operator);
            assert_eq!(actual.kind(), operator.kind());
            assert_eq!(
                actual.name(),
                match operator {
                    BinaryOperator::NotEqual => "inequality",
                    BinaryOperator::Power => "exponentiation",
                    _ => unreachable!(),
                }
            );
            assert_eq!(
                expression.as_ref().span(),
                Span::new(0, source.len() as u32)
            );
            assert_eq!(left.span(), Span::new(0, 1));
            assert_eq!(
                right.span(),
                Span::new((source.len() - 1) as u32, source.len() as u32)
            );

            let displayed = expression.to_string();
            assert!(
                displayed.contains(if operator == BinaryOperator::Power {
                    "**"
                } else {
                    "!="
                }),
                "display lost the operator spelling: {displayed}"
            );
            let reparsed = parse_expr(&displayed).expect("displayed expression should parse");
            assert_eq!(strip_span(&expression), strip_span(&reparsed));
        }
        let expected = strip_span(&parse_expr("a != b").unwrap());
        for source in [
            "a!=b",
            "a \n != \n b",
            "a (* inequality *) != // continue\n b",
        ] {
            assert_eq!(
                strip_span(&parse_expr(source).unwrap()),
                expected,
                "operator whitespace/comment changed the AST for {source:?}"
            );
        }
    }

    // SYN-R02/SYN-R03: keyword aliases use the symbolic precedence table.
    #[test]
    fn boolean_keyword_aliases_match_symbolic_forms() {
        for (symbolic, keyword) in [
            ("a && b", "a and b"),
            ("a || b", "a or b"),
            ("!a", "not a"),
            ("!a && b || c", "not a and b or c"),
            ("!!a", "not not a"),
            ("a && b && c", "a and b and c"),
        ] {
            assert_eq!(
                strip_span(&parse_expr(symbolic).unwrap()),
                strip_span(&parse_expr(keyword).unwrap()),
                "{symbolic:?} and {keyword:?} should have equal ASTs"
            );
        }

        let cases = [
            (
                "2 ** 3 ** 2",
                "BinOp(Val(Int(2)), BinOp(Val(Int(3)), Val(Int(2)), Power), Power)",
            ),
            ("-2 ** 2", "Neg(BinOp(Val(Int(2)), Val(Int(2)), Power))"),
            ("(-2) ** 2", "BinOp(Neg(Val(Int(2))), Val(Int(2)), Power)"),
            ("2 ** -3", "BinOp(Val(Int(2)), Neg(Val(Int(3))), Power)"),
            (
                "2 ** --3",
                "BinOp(Val(Int(2)), Neg(Neg(Val(Int(3)))), Power)",
            ),
            (
                "a * b ** c",
                "BinOp(Var(VarName::new(\"a\")), BinOp(Var(VarName::new(\"b\")), Var(VarName::new(\"c\")), Power), Multiply)",
            ),
            (
                "a ** b * c",
                "BinOp(BinOp(Var(VarName::new(\"a\")), Var(VarName::new(\"b\")), Power), Var(VarName::new(\"c\")), Multiply)",
            ),
            (
                "a + b != c * d",
                "BinOp(BinOp(Var(VarName::new(\"a\")), Var(VarName::new(\"b\")), Add), BinOp(Var(VarName::new(\"c\")), Var(VarName::new(\"d\")), Multiply), NotEqual)",
            ),
            (
                "a != b and c",
                "BinOp(BinOp(Var(VarName::new(\"a\")), Var(VarName::new(\"b\")), NotEqual), Var(VarName::new(\"c\")), And)",
            ),
            (
                "a != b == c",
                "BinOp(BinOp(Var(VarName::new(\"a\")), Var(VarName::new(\"b\")), NotEqual), Var(VarName::new(\"c\")), Equal)",
            ),
            (
                "not a == b",
                "BinOp(Not(Var(VarName::new(\"a\"))), Var(VarName::new(\"b\")), Equal)",
            ),
        ];
        for (source, expected) in cases {
            assert_eq!(
                strip_span(&parse_expr(source).unwrap()),
                expected,
                "unexpected grouping for {source}"
            );
        }
        assert_eq!(
            strip_span(&parse_expr("if true then 2 ** 3 else -1").unwrap()),
            "If(Val(Bool(true)), BinOp(Val(Int(2)), Val(Int(3)), Power), Neg(Val(Int(1))))"
        );
    }

    // SYN-R02: aliases are reserved in identifier positions, but only at token boundaries.
    #[test]
    fn boolean_keywords_are_reserved_without_affecting_neighbouring_identifiers() {
        for source in [
            "in and: Bool",
            "out or: Bool",
            "aux not: Bool",
            "and = true",
            "and",
            "or",
            "not",
        ] {
            assert!(
                parse_str(source).is_err(),
                "{source:?} should fail lexically rather than as an undeclared variable"
            );
        }
        for source in ["android", "origin", "notice", "and_", "or2", "notable"] {
            assert!(matches!(
                parse_expr(source).unwrap().as_ref().view(),
                ExprView::Var(name) if name == &VarName::new(source)
            ));
            assert!(
                parse_str(&format!("in {source}: Bool")).is_ok(),
                "{source:?} should remain a valid declaration name"
            );
        }
        assert!(matches!(
            parse_expr("\"and or not\"").unwrap().as_ref().view(),
            ExprView::Val(SyntaxLiteral::Str(value)) if value == "and or not"
        ));
        for source in [
            r#"\and: Bool -> and"#,
            "dynamic(source, {and})",
            "{and: 1}",
            "x.and",
            "monitored_at(x, and)",
        ] {
            assert!(
                parse_expr(source).is_err(),
                "{source:?} should reject a reserved alias"
            );
        }
        assert!(parse_expr(r#"{"and": 1}"#).is_ok());
        assert!(
            parse_expr("not(a)").is_ok(),
            "adjacent parentheses must be accepted"
        );
    }

    // SYN-R03/SYN-R04: signed RHS operands are unary AST nodes, not signed tokens.
    #[test]
    fn power_accepts_signed_rhs_but_rejects_unary_plus_and_source_i64_min() {
        assert!(matches!(
            parse_expr("x ** -1").unwrap().as_ref().view(),
            ExprView::BinOp(_, rhs, BinaryOperator::Power)
                if matches!(rhs.view(), ExprView::Neg(inner)
                    if matches!(inner.view(), ExprView::Val(SyntaxLiteral::Int(1))))
        ));
        for source in [
            "x ** +1",
            "x ** -9223372036854775808",
            "2 *** 3",
            "**",
            "2 * * 3",
        ] {
            assert!(parse_expr(source).is_err(), "{source} should be rejected");
        }
    }

    // SYN-R05/SYN-R06: numeric source boundaries are errors, not successful prefixes or panics.
    #[test]
    fn numeric_literal_matrix_and_full_input_consumption() {
        for (source, expected) in [
            ("0", SyntaxLiteral::Int(0)),
            ("00", SyntaxLiteral::Int(0)),
            ("00042", SyntaxLiteral::Int(42)),
            ("9223372036854775807", SyntaxLiteral::Int(i64::MAX)),
        ] {
            assert!(matches!(
                parse_expr(source).unwrap().as_ref().view(),
                ExprView::Val(value) if value == &expected
            ));
        }
        for (source, expected) in [
            ("0.5", 0.5_f64),
            ("00.5", 0.5_f64),
            ("1e6", 1_000_000.0_f64),
            ("1E-6", 1e-6_f64),
            ("1.5e+3", 1_500.0_f64),
            ("1e-3", 1e-3_f64),
        ] {
            let expression = parse_expr(source).unwrap();
            assert!(matches!(
                expression.as_ref().view(),
                ExprView::Val(SyntaxLiteral::Float(value))
                    if value.to_bits() == expected.to_bits()
            ));
        }

        let negative = parse_expr("-1e-6").unwrap();
        let ExprView::Neg(operand) = negative.as_ref().view() else {
            panic!("leading minus must remain a Neg node");
        };
        assert_eq!(negative.as_ref().span(), Span::new(0, 5));
        assert_eq!(operand.span(), Span::new(1, 5));
        assert!(
            matches!(operand.view(), ExprView::Val(SyntaxLiteral::Float(value))
            if value.to_bits() == (1e-6_f64).to_bits())
        );

        for source in [
            ".5", "1e", "1e+", "1e-", "1e++3", "1.2.3", "1_000", "1e309", "-1e309",
        ] {
            assert!(
                parse_expr(source).is_err(),
                "{source} should be rejected completely"
            );
        }
        let error = parse_str("out result: Float\nresult = 1e309").unwrap_err();
        let diagnostic = error.to_string();
        assert!(
            diagnostic.contains("line 2, column 10"),
            "source errors should carry the literal's location: {diagnostic}"
        );
        for source in ["NaN", "inf", "Infinity"] {
            assert!(matches!(
                parse_expr(source).unwrap().as_ref().view(),
                ExprView::Var(name) if name == &VarName::new(source)
            ));
            assert!(
                parse_str(&format!("in {source}: Float")).is_ok(),
                "{source} should remain a declared identifier"
            );
        }
        for source in ["1e-4000", "-1e-4000"] {
            let expression = parse_expr(source).unwrap();
            let value = match expression.as_ref().view() {
                ExprView::Val(SyntaxLiteral::Float(value)) => *value,
                ExprView::Neg(operand) => match operand.view() {
                    ExprView::Val(SyntaxLiteral::Float(value)) => -*value,
                    _ => panic!("expected a float operand"),
                },
                _ => panic!("expected a float literal"),
            };
            assert_eq!(value.to_bits(), source.parse::<f64>().unwrap().to_bits());
        }
    }

    // SYN-R06: a second dot after a scientific exponent is not a valid
    // specification, even if the lexer can otherwise interpret the suffix as
    // field syntax.
    #[test]
    fn malformed_exponent_dot_combinations_are_rejected_by_the_pipeline() {
        for literal in ["1e2.3", "1.2e3.4"] {
            let source = format!("out result: Float\nresult = {literal}");
            assert!(
                crate::CheckedDsrvSpecification::parse_with(
                    &source,
                    crate::TypeCheckOptions::STRICT
                )
                .map_or(true, |report| report.result().is_err()),
                "{literal} must not be accepted as a valid typed specification"
            );
        }
    }

    // SYN-R05/SYN-R11: unsigned offsets and tuple-field indices have a checked parser boundary.
    #[test]
    fn temporal_and_field_indices_accept_u64_max_but_not_overflow() {
        let expression = parse_expr("x[18446744073709551615]").unwrap();
        assert!(matches!(
            expression.as_ref().view(),
            ExprView::SIndex(input, u64::MAX)
                if matches!(input.view(), ExprView::Var(name) if name == &VarName::new("x"))
        ));
        let field = parse_expr("values.18446744073709551615").unwrap();
        assert!(matches!(
            field.as_ref().view(),
            ExprView::SGet(input, key)
                if key == "18446744073709551615"
                    && matches!(input.view(), ExprView::Var(name) if name == &VarName::new("values"))
        ));
        assert!(parse_expr("x[18446744073709551616]").is_err());
        assert!(parse_expr("values.18446744073709551616").is_err());
        let error = parse_str("out result: Int\nresult = x[18446744073709551616]").unwrap_err();
        assert!(
            error.to_string().contains("line 2, column 12"),
            "unsigned literal errors should carry the index location: {error}"
        );
        assert!(parse_expr("values[i]").is_err());
        assert!(matches!(
            parse_expr("List.get(values, i)").unwrap().as_ref().view(),
            ExprView::LIndex(_, _)
        ));
    }

    // SYN-R07/SYN-R09: every changed value/call family accepts exactly one trailing comma.
    #[test]
    fn approved_value_and_builtin_lists_accept_one_trailing_comma_only() {
        let pairs = [
            ("[1, 2]", "[1, 2,]"),
            ("List(1)", "List(1,)"),
            ("Tuple(1)", "Tuple(1,)"),
            ("Map(\"x\": 1)", "Map(\"x\": 1,)"),
            ("Struct(\"x\": 1)", "Struct(\"x\": 1,)"),
            ("{x: 1}", "{x: 1,}"),
            ("f(x)", "f(x,)"),
            ("f(x)(y)", "f(x,)(y,)"),
            ("dynamic(source, {x})", "dynamic(source, {x,},)"),
            ("eval(source, {x})", "eval(source, {x,},)"),
            ("defer(source, {x})", "defer(source, {x,},)"),
            ("update(x, y)", "update(x, y,)"),
            ("default(x, y)", "default(x, y,)"),
            ("is_defined(x)", "is_defined(x,)"),
            ("when(x)", "when(x,)"),
            ("latch(x, y)", "latch(x, y,)"),
            ("init(x, y)", "init(x, y,)"),
            ("List.get(x, y)", "List.get(x, y,)"),
            ("List.concat(x, y)", "List.concat(x, y,)"),
            ("List.append(x, y)", "List.append(x, y,)"),
            ("List.head(x)", "List.head(x,)"),
            ("List.tail(x)", "List.tail(x,)"),
            ("List.len(x)", "List.len(x,)"),
            ("List.map(f, x)", "List.map(f, x,)"),
            ("List.filter(f, x)", "List.filter(f, x,)"),
            ("List.fold(f, x, y)", "List.fold(f, x, y,)"),
            ("fix(f)", "fix(f,)"),
            ("partial(f)", "partial(f,)"),
            ("partial(f, x, y)", "partial(f, x, y,)"),
            ("Map.get(m, \"x\")", "Map.get(m, \"x\",)"),
            ("Map.remove(m, \"x\")", "Map.remove(m, \"x\",)"),
            ("Map.has_key(m, \"x\")", "Map.has_key(m, \"x\",)"),
            ("Map.insert(m, \"x\", y)", "Map.insert(m, \"x\", y,)"),
            ("sin(x)", "sin(x,)"),
            ("cos(x)", "cos(x,)"),
            ("tan(x)", "tan(x,)"),
            ("abs(x)", "abs(x,)"),
            ("monitored_at(x, node)", "monitored_at(x, node,)"),
            ("dist(x, node)", "dist(x, node,)"),
        ];
        // A distributed context, so the distribution primitives parse too.
        let context = parse_str("language distributed\n")
            .unwrap()
            .source_context()
            .clone();
        for (without, with) in pairs {
            let without = parse_expr_with_context(without, context.clone())
                .unwrap_or_else(|error| panic!("{without}: {error}"));
            let with = parse_expr_with_context(with, context.clone())
                .unwrap_or_else(|error| panic!("{with}: {error}"));
            assert_eq!(
                strip_span(&without),
                strip_span(&with),
                "{without:?} vs {with:?}"
            );
        }
        assert!(parse_expr("[]").is_ok());
        for source in [
            "[,]",
            "[1,,]",
            "[1,2,,]",
            "f(x,,)",
            "f(,x)",
            "List(,)",
            "Map(\"x\": 1,,)",
        ] {
            assert!(parse_expr(source).is_err(), "{source} should be rejected");
        }
        assert_eq!(
            strip_span(&parse_expr("[1, 2,\n]").unwrap()),
            strip_span(&parse_expr("[1, 2]").unwrap())
        );
        assert_eq!(
            strip_span(&parse_expr("f(x, // trailing argument\n)").unwrap()),
            strip_span(&parse_expr("f(x)").unwrap())
        );
    }

    // SYN-R08: type wrappers and tuple/struct forms use the same one-comma boundary.
    #[test]
    fn approved_type_lists_accept_one_trailing_comma() {
        let pairs = [
            ("List<Int>", "List<Int,>"),
            ("Map<Bool>", "Map<Bool,>"),
            ("Expr<Float>", "Expr<Float,>"),
            ("(Int,)", "(Int,)"),
            ("(Int, Bool)", "(Int, Bool,)"),
            ("Struct<x: Int>", "Struct<x: Int,>"),
            ("Struct<...>", "Struct<...,>"),
            ("Struct<x: Int, ...>", "Struct<x: Int, ...,>"),
        ];
        let parse_type = |typ: &str| {
            let (_, declaration) =
                parse_declaration(&format!("in value: {typ}")).expect("type should parse");
            let Declaration::Input {
                name: _,
                annotation: typ,
                span: _,
            } = declaration
            else {
                panic!("expected input declaration");
            };
            typ
        };
        for (without, with) in pairs {
            assert_eq!(parse_type(without), parse_type(with), "{without} vs {with}");
        }
        let (_, declaration) = parse_declaration("in value: Struct<>").unwrap();
        assert!(matches!(
            declaration,
            Declaration::Input { name: _, annotation: Some(StreamType::Struct(fields, false)), span: _ }
                if fields.is_empty()
        ));
        assert!(parse_declaration("in value: Struct<,>").is_err());
        let nested_plain = parse_declaration("in value: List<Map<Int>>").unwrap().1;
        let nested_trailing = parse_declaration("in value: List<Map<Int,>,>").unwrap().1;
        let (
            Declaration::Input {
                name: _,
                annotation: plain_type,
                span: plain_span,
            },
            Declaration::Input {
                name: _,
                annotation: trailing_type,
                span: trailing_span,
            },
        ) = (&nested_plain, &nested_trailing)
        else {
            panic!("expected nested list declarations");
        };
        assert_eq!(plain_type, trailing_type);
        assert_eq!(*plain_span, Span::new(0, 24));
        assert_eq!(*trailing_span, Span::new(0, 26));
        assert!(parse_declaration("in f: (Int -> Bool)").is_ok());
        for source in [
            "in value: List<Int,,>",
            "in value: List<Int, Bool>",
            "in value: (Int,,)",
            "in value: Struct<x: Int,,>",
            "in value: (Int,) -> Int",
            "in value: (Int -> Bool,)",
        ] {
            assert!(
                parse_declaration(source).is_err(),
                "{source} should be rejected"
            );
        }
        assert!(parse_expr(r#"\x: Int, y: Int -> x"#).is_ok());
        assert!(parse_expr(r#"\x: Int, -> x"#).is_err());
        assert!(parse_expr(r#"\x: Int, y: Int, -> x"#).is_err());
    }

    // SYN-R10: canonical else-if nesting is right-nested and preserves dangling-else attachment.
    #[test]
    fn else_if_chains_and_temporal_brackets_keep_their_existing_ast_forms() {
        let source = "if a then 1 else if b then 2 else 3";
        let expected = "If(Var(VarName::new(\"a\")), Val(Int(1)), If(Var(VarName::new(\"b\")), Val(Int(2)), Val(Int(3))))";
        let expression = parse_expr(source).unwrap();
        assert_eq!(strip_span(&expression), expected);
        assert_eq!(
            strip_span(&parse_expr("if a then 1\nelse if b then 2\nelse 3").unwrap()),
            expected
        );
        assert_eq!(
            strip_span(&parse_expr("if a then if b then 1 else 2 else 3").unwrap()),
            "If(Var(VarName::new(\"a\")), If(Var(VarName::new(\"b\")), Val(Int(1)), Val(Int(2))), Val(Int(3)))"
        );
        assert_eq!(
            strip_span(&parse_expr(&expression.to_string()).unwrap()),
            expected
        );
        for source in ["if a then 1", "if a 1 else 2", "if a then 1 else 2 else 3"] {
            assert!(parse_expr(source).is_err(), "{source} should be rejected");
        }
        assert!(parse_expr("if a then 1 elif b then 2 else 3").is_err());
        assert!(
            parse_expr("elif").is_ok(),
            "elif is not a reserved identifier"
        );

        assert_eq!(
            strip_span(&parse_expr("x[1]").unwrap()),
            "SIndex(Var(VarName::new(\"x\")), 1)"
        );
        assert_eq!(
            strip_span(&parse_expr("(x + y)[2]").unwrap()),
            "SIndex(BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), Add), 2)"
        );
        assert_eq!(
            strip_span(&parse_expr("x[1] ** 2").unwrap()),
            "BinOp(SIndex(Var(VarName::new(\"x\")), 1), Val(Int(2)), Power)"
        );
        assert!(
            parse_expr("x[1,]").is_err(),
            "temporal offsets are not sequences"
        );
    }

    // SYN-P01/P03/P04/P06: bounded generators exercise totality and source/display composition.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn valid_integer_literal_sources_roundtrip((source, value) in arb_valid_integer_literal_source()) {
            let expression = parse_expr(&source).expect("generated integer source should parse");
            let actual = match expression.as_ref().view() {
                ExprView::Val(SyntaxLiteral::Int(actual)) => *actual,
                ExprView::Neg(operand) => match operand.view() {
                    ExprView::Val(SyntaxLiteral::Int(actual)) => -*actual,
                    _ => panic!("expected an integer operand"),
                },
                _ => panic!("expected an integer expression"),
            };
            prop_assert_eq!(actual, value);
            let displayed = expression.to_string();
            let reparsed = parse_expr(&displayed).expect("displayed integer should parse");
            prop_assert_eq!(strip_span(&expression), strip_span(&reparsed));
        }

        #[test]
        fn out_of_range_integer_literal_sources_are_errors(source in arb_invalid_integer_literal_source()) {
            let result = std::panic::catch_unwind(|| parse_expr(&source));
            prop_assert!(result.is_ok(), "integer parsing panicked for {source}");
            prop_assert!(result.unwrap().is_err(), "out-of-range integer parsed: {source}");
        }

        #[test]
        fn finite_float_literal_sources_match_rust_and_roundtrip((source, expected) in arb_finite_float_literal_source()) {
            let expression = parse_expr(&source).expect("generated float source should parse");
            let ExprView::Val(SyntaxLiteral::Float(actual)) = expression.as_ref().view() else {
                panic!("expected a float literal");
            };
            prop_assert_eq!(actual.to_bits(), expected.to_bits());
            let reparsed = parse_expr(&expression.to_string()).expect("displayed float should parse");
            prop_assert_eq!(strip_span(&expression), strip_span(&reparsed));
        }

        #[test]
        fn one_trailing_comma_is_a_spanless_ast_metamorphism((without, with) in arb_trailing_comma_pair()) {
            let is_type = without.starts_with("List<")
                || without.starts_with("Map<")
                || without.starts_with("Expr<")
                || without.starts_with('(')
                || without.starts_with("Struct<");
            if is_type {
                let (_, plain_declaration) = parse_declaration(&format!("in value: {without}"))
                    .expect("generated type should parse");
                let (_, trailed_declaration) = parse_declaration(&format!("in value: {with}"))
                    .expect("generated type with comma should parse");
                let (Declaration::Input { name: _, annotation: plain, span: _ }, Declaration::Input { name: _, annotation: trailed, span: _ }) =
                    (plain_declaration, trailed_declaration)
                else {
                    panic!("generated type pair did not parse as input declarations");
                };
                prop_assert_eq!(plain, trailed);
            } else if without.starts_with('{') {
                prop_assert_eq!(
                    strip_span(&parse_expr(&format!("dynamic(source, {without})")).expect("generated scope should parse")),
                    strip_span(&parse_expr(&format!("dynamic(source, {with})")).expect("generated scope with comma should parse"))
                );
            } else {
                prop_assert_eq!(
                    strip_span(&parse_expr(without).expect("generated expression should parse")),
                    strip_span(&parse_expr(with).expect("generated expression with comma should parse"))
                );
            }
        }

        // SYN-P06: this is a distinct AST rendering property, rather than a
        // relabelled literal/comma property. The generator guarantees both
        // revised operators and a negative-base power survive shrinking.
        #[test]
        fn revised_operator_ast_display_roundtrips_bounded_typed_trees(
            expression in arb_revised_operator_display_expr()
        ) {
            let nodes = expression.as_ref().postorder().collect::<Vec<_>>();
            prop_assert!(nodes.len() <= 40, "generated AST has {} nodes", nodes.len());
            prop_assert!(
                nodes.iter().any(|node| matches!(
                    node.view(),
                    ExprView::BinOp(_, _, BinaryOperator::Power)
                )),
                "generated AST omitted Power"
            );
            prop_assert!(
                nodes.iter().any(|node| matches!(
                    node.view(),
                    ExprView::BinOp(_, _, BinaryOperator::NotEqual)
                )),
                "generated AST omitted NotEqual"
            );
            prop_assert!(
                nodes.iter().any(|node| matches!(node.view(), ExprView::Neg(_))),
                "generated AST omitted Neg"
            );
            prop_assert!(
                nodes.iter().any(|node| matches!(
                    node.view(),
                    ExprView::BinOp(lhs, _, BinaryOperator::Power)
                        if matches!(lhs.view(), ExprView::Neg(_))
                )),
                "generated Power did not have a negative base"
            );
            prop_assert!(
                nodes.iter().any(|node| matches!(
                    node.view(),
                    ExprView::BinOp(_, _, BinaryOperator::Add | BinaryOperator::Multiply)
                )),
                "generated AST omitted an adjacent-precedence arithmetic operator"
            );

            let displayed = expression.to_string();
            let reparsed = parse_expr(&displayed).unwrap_or_else(|error| {
                panic!("displayed AST should parse: {displayed:?}: {error}")
            });
            prop_assert_eq!(strip_span(&expression), strip_span(&reparsed));
        }
    }

    #[test]
    fn test_trig() {
        assert_eq!(
            presult_strip_span(&parse_expr("sin(1.0)").unwrap()),
            r#"Ok(Sin(Val(Float(1.0))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("cos(0)").unwrap()),
            r#"Ok(Cos(Val(Int(0))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("tan(3.14)").unwrap()),
            r#"Ok(Tan(Val(Float(3.14))))"#
        );
    }

    #[test]
    fn test_abs() {
        assert_eq!(
            presult_strip_span(&parse_expr("abs(-5)").unwrap()),
            r#"Ok(Abs(Neg(Val(Int(5)))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("abs(3.14)").unwrap()),
            r#"Ok(Abs(Val(Float(3.14))))"#
        );
    }

    #[test]
    fn test_comparison() {
        assert_eq!(
            presult_strip_span(&parse_expr("1 < 2").unwrap()),
            r#"Ok(BinOp(Val(Int(1)), Val(Int(2)), Less))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 > 2").unwrap()),
            r#"Ok(BinOp(Val(Int(1)), Val(Int(2)), Greater))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("3.14 >= 2.71").unwrap()),
            r#"Ok(BinOp(Val(Float(3.14)), Val(Float(2.71)), GreaterEqual))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("3.14 <= 2.71").unwrap()),
            r#"Ok(BinOp(Val(Float(3.14)), Val(Float(2.71)), LessEqual))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("x == y").unwrap()),
            r#"Ok(BinOp(Var(VarName::new("x")), Var(VarName::new("y")), Equal))"#
        );
        // Test precedence:
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 > 3").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), Val(Int(3)), Greater))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("1 + 2 == 3 * 4").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), BinOp(Val(Int(3)), Val(Int(4)), Multiply), Equal))"#
        );
        // Equality has lower precedence than other comparisons
        assert_eq!(
            presult_strip_span(&parse_expr("1 < 2 == 3 < 4").unwrap()),
            r#"Ok(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Less), BinOp(Val(Int(3)), Val(Int(4)), Less), Equal))"#
        );
    }

    #[test]
    fn test_not() {
        assert_eq!(
            presult_strip_span(&parse_expr("!true").unwrap()),
            r#"Ok(Not(Val(Bool(true))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!false").unwrap()),
            r#"Ok(Not(Val(Bool(false))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("! (1 + 2 > 3)").unwrap()),
            r#"Ok(Not(BinOp(BinOp(Val(Int(1)), Val(Int(2)), Add), Val(Int(3)), Greater)))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!!false").unwrap()),
            r#"Ok(Not(Not(Val(Bool(false)))))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("!(if a then b else c)").unwrap()),
            r#"Ok(Not(If(Var(VarName::new("a")), Var(VarName::new("b")), Var(VarName::new("c")))))"#
        );
        // Another edge case:
        assert_eq!(
            presult_strip_span(&parse_expr("!1 + 2").unwrap()),
            r#"Ok(BinOp(Not(Val(Int(1))), Val(Int(2)), Add))"#
        );
        assert_eq!(
            presult_strip_span(&parse_expr("if !true then 1 else 2").unwrap()),
            "Ok(If(Not(Val(Bool(true))), Val(Int(1)), Val(Int(2))))"
        );
        assert_eq!(
            presult_strip_span(&parse_expr("dynamic(!s)").unwrap()),
            r#"Ok(Dynamic(Not(Var(VarName::new("s"))), Unascribed, Automatic))"#
        );
    }

    #[test]
    fn test_capital_varname() {
        assert_eq!(
            declaration_to_string(&parse_declaration("in G")),
            r#"Ok(Input(VarName::new("G"), None, Span { start: 0, end: 4 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("out F")),
            r#"Ok(Output(VarName::new("F"), None, Span { start: 0, end: 5 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("in GANDALF")),
            r#"Ok(Input(VarName::new("GANDALF"), None, Span { start: 0, end: 10 }))"#
        );
        assert_eq!(
            declaration_to_string(&parse_declaration("out FRODO")),
            r#"Ok(Output(VarName::new("FRODO"), None, Span { start: 0, end: 9 }))"#
        );
    }

    #[test]
    fn test_large_expression() {
        let expr = "(((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((0.1) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((0.1) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1) || (((((if !(!(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) && !((((((((0.1) * cos((a))) - ((-0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) / ((((((0.1) * sin((a))) + ((-0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ) + (if !(!(((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) == !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) <= ((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5))) && !(((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y)) == ((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y))) && !((((((((-0.181) * cos((a))) - ((0.153) * sin((a)))) + (x))) - (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) * ((((((-0.181) * sin(3.14)) + ((-0.153) * cos(3.14))) + -0.5)) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) / ((((((-0.181) * sin((a))) + ((0.153) * cos((a)))) + (y))) - (((((-0.181) * sin((a))) + ((-0.153) * cos((a)))) + (y)))) + (((((-0.181) * cos((a))) - ((-0.153) * sin((a)))) + (x)))) <= (((((-0.181) * cos(3.14)) - ((-0.153) * sin(3.14))) + 1.0))) then 1 else 0 ))) % 2) == 1)";
        let res = parse_expr(expr);
        assert!(res.is_ok());
    }
}

#[cfg(test)]
mod spec_tests {

    use super::*;
    use test_log::test;

    fn counter_inf() -> (&'static str, &'static str) {
        (
            "out z\nz = default(z[1], 0) + 1",
            "Ok(DsrvSpecification { input_vars: {}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Default(SIndex(Var(VarName::new(\"z\")), 1), Val(Int(0))), Val(Int(1)), Add)}, type_annotations: {} })",
        )
    }

    fn counter() -> (&'static str, &'static str) {
        (
            "in x\nout z\nz = default(z[1], 0) + x",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Default(SIndex(Var(VarName::new(\"z\")), 1), Val(Int(0))), Var(VarName::new(\"x\")), Add)}, type_annotations: {} })",
        )
    }

    fn future() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nout a\nz = x[1]\na = y",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\"), VarName::new(\"a\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\"), VarName::new(\"a\")}, exprs: {VarName::new(\"z\"): SIndex(Var(VarName::new(\"x\")), 1), VarName::new(\"a\"): Var(VarName::new(\"y\"))}, type_annotations: {} })",
        )
    }

    fn list() -> (&'static str, &'static str) {
        (
            "in iList\nout oList\nout nestedList\nout listIndex\nout listAppend\nout listConcat\nout listHead\nout listTail\noList = iList\nnestedList = List(iList, iList)\nlistIndex = List.get(iList, 0)\nlistAppend = List.append(iList, (1+1)/2)\nlistConcat = List.concat(iList, iList)\nlistHead = List.head(iList)\nlistTail = List.tail(iList)",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"iList\")}, output_vars: {VarName::new(\"oList\"), VarName::new(\"nestedList\"), VarName::new(\"listIndex\"), VarName::new(\"listAppend\"), VarName::new(\"listConcat\"), VarName::new(\"listHead\"), VarName::new(\"listTail\")}, aux_vars: {}, stream_vars: {VarName::new(\"oList\"), VarName::new(\"nestedList\"), VarName::new(\"listIndex\"), VarName::new(\"listAppend\"), VarName::new(\"listConcat\"), VarName::new(\"listHead\"), VarName::new(\"listTail\")}, exprs: {VarName::new(\"oList\"): Var(VarName::new(\"iList\")), VarName::new(\"nestedList\"): List([Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))]), VarName::new(\"listIndex\"): LIndex(Var(VarName::new(\"iList\")), Val(Int(0))), VarName::new(\"listAppend\"): LAppend(Var(VarName::new(\"iList\")), BinOp(BinOp(Val(Int(1)), Val(Int(1)), Add), Val(Int(2)), Divide)), VarName::new(\"listConcat\"): LConcat(Var(VarName::new(\"iList\")), Var(VarName::new(\"iList\"))), VarName::new(\"listHead\"): LHead(Var(VarName::new(\"iList\"))), VarName::new(\"listTail\"): LTail(Var(VarName::new(\"iList\")))}, type_annotations: {} })",
        )
    }

    fn simple_add_typed() -> (&'static str, &'static str) {
        (
            "in x: Int\nin y: Int\nout z: Int\nz = x + y",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), Add)}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"y\"): Int, VarName::new(\"z\"): Int} })",
        )
    }

    fn simple_add_aux() -> (&'static str, &'static str) {
        (
            crate::dsrv_fixtures::spec_simple_add_aux_monitor(),
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {VarName::new(\"u\"), VarName::new(\"w\")}, stream_vars: {VarName::new(\"z\"), VarName::new(\"u\"), VarName::new(\"w\")}, exprs: {VarName::new(\"u\"): Var(VarName::new(\"x\")), VarName::new(\"w\"): Var(VarName::new(\"y\")), VarName::new(\"z\"): BinOp(Var(VarName::new(\"u\")), Var(VarName::new(\"w\")), Add)}, type_annotations: {} })",
        )
    }
    fn simple_add_aux_typed() -> (&'static str, &'static str) {
        (
            crate::dsrv_fixtures::spec_simple_add_aux_typed_monitor(),
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {VarName::new(\"u\"), VarName::new(\"w\")}, stream_vars: {VarName::new(\"z\"), VarName::new(\"u\"), VarName::new(\"w\")}, exprs: {VarName::new(\"u\"): Var(VarName::new(\"x\")), VarName::new(\"w\"): Var(VarName::new(\"y\")), VarName::new(\"z\"): BinOp(Var(VarName::new(\"u\")), Var(VarName::new(\"w\")), Add)}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"y\"): Int, VarName::new(\"z\"): Int, VarName::new(\"u\"): Int, VarName::new(\"w\"): Int} })",
        )
    }

    fn simple_add_typed_start_and_end_comment() -> (&'static str, &'static str) {
        (
            "// Begin\nin x: Int\nin y: Int\nout z: Int\nz = x + y// End",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): BinOp(Var(VarName::new(\"x\")), Var(VarName::new(\"y\")), Add)}, type_annotations: {VarName::new(\"x\"): Int, VarName::new(\"y\"): Int, VarName::new(\"z\"): Int} })",
        )
    }

    fn if_statement() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if x == 0 then y else 42",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), Equal), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
        )
    }

    fn if_statement_newlines() -> (&'static str, &'static str) {
        (
            "in x\nin y\nout z\nz = if\nx == 0\nthen\ny\n else\n42",
            "Ok(DsrvSpecification { input_vars: {VarName::new(\"x\"), VarName::new(\"y\")}, output_vars: {VarName::new(\"z\")}, aux_vars: {}, stream_vars: {VarName::new(\"z\")}, exprs: {VarName::new(\"z\"): If(BinOp(Var(VarName::new(\"x\")), Val(Int(0)), Equal), Var(VarName::new(\"y\")), Val(Int(42)))}, type_annotations: {} })",
        )
    }

    fn function_name<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }

    fn specs() -> Vec<(&'static str, (&'static str, &'static str))> {
        // Unfortunately, can't iterate because that converts them to general function pointers
        // instead of strong types
        Vec::from([
            (function_name(counter), counter()),
            (function_name(counter_inf), counter_inf()),
            (function_name(future), future()),
            (function_name(list), list()),
            (function_name(simple_add_typed), simple_add_typed()),
            (function_name(simple_add_aux), simple_add_aux()),
            (function_name(simple_add_aux_typed), simple_add_aux_typed()),
            (
                function_name(simple_add_typed_start_and_end_comment),
                simple_add_typed_start_and_end_comment(),
            ),
            (function_name(if_statement), if_statement()),
            (
                function_name(if_statement_newlines),
                if_statement_newlines(),
            ),
        ])
    }

    #[test]
    fn test_dsrv_specs_normal() {
        for &(name, (spec, exp)) in specs().iter() {
            let parsed = presult_to_string(&parse_str(spec));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_dsrv_specs_added_newlines() {
        for &(name, (spec, exp)) in specs().iter() {
            let spec = spec.replace("\n", "\n\n");
            let parsed = presult_to_string(&parse_str(spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }

    #[test]
    fn test_dsrv_specs_added_comments() {
        for &(name, (spec, exp)) in specs().iter() {
            let mod_spec = spec.replace("\n", "\n//This is a comment\n");
            let parsed = presult_to_string(&parse_str(mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );

            let mod_spec = spec.replace("\n", "//This is a comment\n"); // Beginning \n
            let parsed = presult_to_string(&parse_str(mod_spec.as_str()));
            assert_eq!(
                format!("{}: {}", name, parsed),
                format!("{}: {}", name, exp)
            );
        }
    }
}
