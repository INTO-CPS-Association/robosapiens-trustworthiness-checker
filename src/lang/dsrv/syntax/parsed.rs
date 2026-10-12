//! Private source tree. Parsing resolves no names: unresolved type names are
//! kept for the expansion stage.

// The generated family includes borrowed views; expansion uses stored kinds.
#![allow(dead_code)]

use ecow::{EcoString, EcoVec};

use crate::core::{BinaryOperator, VarName};
use crate::distributed::distribution_graphs::NodeName;

use super::super::ast::{ReconfigurableExprScope, SyntaxLiteral, VarOrNodeName};
use super::super::expand::functions::LexicalId;
use super::super::path::{ModuleName, TypePath, UseTree, ValuePath};
use super::super::patterns::{MatchArm, MatchPattern};
use super::super::source::{AliasDeclaration, SourceType, TypeName};
use super::super::source_map::{SourceId, SourceSite};
use super::super::span::Span;
use super::DsrvSyntaxError;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SourceAscription {
    Unascribed,
    Ascribed(SourceType),
}

contiguous_tree::tree_schema! {
    pub(crate) tree ParsedExpr {
        schema: pub(crate),
        metadata: origin: ParsedOrigin = ParsedOrigin::default(),
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        If(condition: child, then_expr: child, else_expr: child),
        SIndex(input: child, offset: data(SourceOffset)),
        Val(value: into_data(SyntaxLiteral)),
        DurationLiteral(seconds: copy(f64)),
        BinOp(left: child, right: child, operator: copy(BinaryOperator)),
        Cast(value: child, target: data(SourceType)),
        // Frontend-generated type ascription; unlike a source cast, this does
        // not require the casts experiment.
        Ascribe(value: child, target: data(SourceType)),
        Var(variable: data(VarName)),
        // A value named through a module. Expansion inlines the def it
        // names, so this never reaches the core AST.
        ModuleItem(path: data(ValuePath)),
        Constructor(
            payload: children,
            tag: data(EcoString),
            qualifier: data(Option<TypePath>),
        ),
        Match(scrutinee: child, arms: children, shape: data(EcoVec<MatchArm>)),
        Matches(scrutinee: child, guard: children, pattern: data(MatchPattern)),
        Dynamic(source: child, result_type: data(SourceAscription), scope: data(ReconfigurableExprScope)),
        Defer(source: child, result_type: data(SourceAscription), scope: data(ReconfigurableExprScope)),
        Update(value: child, update: child),
        Default(value: child, default: child),
        IsDefined(value: child),
        When(value: child),
        Latch(value: child, trigger: child),
        Init(value: child, initial: child),
        Not(value: child),
        Neg(value: child),
        Lambda(parameters: data(EcoVec<(VarName, Option<SourceType>)>), body: child),
        Apply(function: child, arguments: children),
        Fix(function: child),
        Partial(function: child, arguments: children),
        List(items: children),
        Tuple(items: children),
        LIndex(list: child, index: child),
        LAppend(list: child, value: child),
        LConcat(left: child, right: child),
        LHead(list: child),
        LTail(list: child),
        LLen(list: child),
        LMap(function: child, list: child),
        LFilter(function: child, list: child),
        LFold(function: child, initial: child, list: child),
        Map(entries: keyed_children),
        Struct(entries: keyed_children),
        ObjectLiteral(entries: keyed_children),
        // `{ a: x, ..base }`: `base` with the named fields replaced. The
        // children are in source order, each named field's value and then
        // the base, since a tree's fixed children must precede a collection.
        StructUpdate(values: children, fields: data(EcoVec<EcoString>)),
        MGet(map: child, key: data(EcoString)),
        SGet(value: child, key: data(EcoString)),
        MInsert(map: child, key: data(EcoString), value: child),
        MRemove(map: child, key: data(EcoString)),
        MHasKey(map: child, key: data(EcoString)),
        Sin(value: child),
        Cos(value: child),
        Tan(value: child),
        Abs(value: child),
        Trunc(value: child),
        Floor(value: child),
        Ceil(value: child),
        Round(value: child),
        MonitoredAt(variable: data(VarName), node: data(NodeName)),
        Dist(left: data(VarOrNodeName), right: data(VarOrNodeName)),
        // A stream function call, `m::f::<fixed…>(streams…)`: the first `fixed`
        // arguments are the fixed ones, the rest are streams. Expansion
        // instantiates a use that is the whole right-hand side of an
        // equation and refuses any other, so none reaches the core AST.
        StreamFunctionCall(arguments: children, stream_function: data(TypePath), fixed: copy(usize)),
    }
}

/// Where a parsed node came from. The span is in the file being parsed or
/// inlined into; a node grafted from a def of another module additionally
/// records where that def wrote it, and whose lexical environment its names
/// and syntax belong to. The file itself is the parsed specification's, so
/// no node carries it, and a node without a lexical environment belongs to
/// the text it sits in.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ParsedOrigin {
    pub(crate) span: Span,
    pub(crate) definition: Option<SourceSite>,
    pub(crate) lexical: Option<LexicalId>,
    pub(crate) generic_def: Option<crate::lang::dsrv::ast::AstShared<GenericFunctionSignature>>,
    pub(crate) duration_seconds: Option<f64>,
}

/// Source-only signature carried by the lambda generated for a generic `def`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GenericFunctionSignature {
    pub(crate) type_parameters: ecow::EcoVec<crate::lang::dsrv::source::TypeName>,
    pub(crate) parameters: ecow::EcoVec<crate::lang::dsrv::source::SourceType>,
    pub(crate) result: crate::lang::dsrv::source::SourceType,
}

impl From<Span> for ParsedOrigin {
    fn from(span: Span) -> Self {
        Self {
            span,
            definition: None,
            lexical: None,
            generic_def: None,
            duration_seconds: None,
        }
    }
}

/// The builder the grammar allocates through: every node it parses is
/// written in the file being parsed, so it gives a span alone.
pub(crate) struct SpanningBuilder(ParsedExprBuilder);

impl SpanningBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self(ParsedExprBuilder::with_capacity(capacity))
    }

    pub(crate) fn alloc(&mut self, node: ParsedExprKind, span: Span) -> ParsedExprId {
        self.0.alloc(node, span.into())
    }

    /// The span of a node already allocated.
    pub(crate) fn metadata(&self, id: ParsedExprId) -> Span {
        self.0.metadata(id).span
    }

    pub(crate) fn into_inner(self) -> ParsedExprBuilder {
        self.0
    }
}

/// How far back a stream offset reaches, before names are resolved.
///
/// The core AST keeps a number; a constant naming one is folded away in
/// expansion, exactly as `ModuleItem` is.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SourceOffset {
    Literal(u64),
    Named(ValuePath),
}

#[derive(Clone, Debug)]
pub(crate) enum ParsedDeclaration {
    Input(VarName, Option<SourceType>, Span),
    /// `out x`, `out x: T`, or a one-line definition `out x: T = e`.
    Output(VarName, Option<SourceType>, Option<ParsedExprId>, Span),
    /// `aux x`, `aux x: T`, or a one-line definition `aux x: T = e`.
    Aux(VarName, Option<SourceType>, Option<ParsedExprId>, Span),
    Equation(VarName, ParsedExprId, Span),
    /// Private aux equations attached to one complete stream RHS.
    EquationScope {
        equation: Box<ParsedDeclaration>,
        locals: EcoVec<ParsedDeclaration>,
        span: Span,
    },
    Alias(AliasDeclaration),
    /// `language <name>`, with the name unchecked.
    Language(EcoString, Span),
    /// `use <path>`, `use <path>::*` or `use <path>::{…}`, as written.
    Use {
        tree: UseTree,
        span: Span,
    },
    /// A named def: pure bodies inline, while a `where` block or a
    /// stateful forwarding result makes each static call a stream instance.
    Def {
        name: VarName,
        type_parameters: EcoVec<TypeName>,
        parameters: EcoVec<(VarName, SourceType)>,
        fixed: EcoVec<(VarName, SourceType)>,
        locals: Option<EcoVec<ParsedDeclaration>>,
        result: SourceType,
        body: ParsedExprId,
        internal: bool,
        span: Span,
    },
    /// `const <name>: <type> = <body>`: a value folded once and written in
    /// wherever it is named, rather than evaluated.
    Const {
        name: VarName,
        ty: SourceType,
        body: ParsedExprId,
        internal: bool,
        span: Span,
    },
    /// `mod <path>`, naming a submodule this file pulls in. The path is the
    /// module's name, not a file: mapping it to one is the collector's work.
    Mod {
        path: EcoVec<ModuleName>,
        span: Span,
    },
}

impl ParsedDeclaration {
    pub(crate) fn with_locals(self, locals: Option<Vec<Self>>, span: Span) -> Self {
        match locals {
            Some(locals) => Self::EquationScope {
                equation: Box::new(self),
                locals: locals.into(),
                span,
            },
            None => self,
        }
    }

    /// The expression roots this declaration owns, in the order the parsed
    /// forest holds them.
    pub(crate) fn roots(&self) -> Vec<ParsedExprId> {
        match self {
            ParsedDeclaration::Equation(_, root, _)
            | ParsedDeclaration::Output(_, _, Some(root), _)
            | ParsedDeclaration::Aux(_, _, Some(root), _)
            | ParsedDeclaration::Const { body: root, .. } => vec![*root],
            ParsedDeclaration::EquationScope {
                equation, locals, ..
            } => {
                let mut roots = equation.roots();
                roots.extend(locals.iter().flat_map(Self::roots));
                roots
            }
            ParsedDeclaration::Def { body, locals, .. } => {
                let mut roots = vec![*body];
                if let Some(locals) = locals {
                    roots.extend(locals.iter().flat_map(Self::roots));
                }
                roots
            }
            _ => Vec::new(),
        }
    }

    /// How many expression roots this declaration owns.
    pub(crate) fn root_count(&self) -> usize {
        self.roots().len()
    }
}

/// A parameter written between a stream function's angle brackets: a type
/// parameter, or a fixed argument with its type.
#[derive(Clone, Debug)]
pub(crate) enum DefGeneric {
    Type(TypeName),
    Fixed(VarName, SourceType),
}

/// Internal expansion view of a def result and its local aux equations.
#[derive(Clone, Debug)]
pub(crate) struct StreamFunctionDeclaration {
    pub(crate) internal: bool,
    pub(crate) name: TypeName,
    pub(crate) type_parameters: EcoVec<TypeName>,
    /// Arguments fixed for the life of an instance, in order.
    pub(crate) fixed: EcoVec<(VarName, SourceType)>,
    /// Stream arguments, bound to the enclosing streams on every tick.
    pub(crate) streams: EcoVec<(VarName, SourceType)>,
    pub(crate) result: Option<SourceType>,
    pub(crate) body: EcoVec<LocalEquation>,
    pub(crate) span: Span,
}

/// A local aux equation or the synthetic equation for a def result.
#[derive(Clone, Debug)]
pub(crate) struct LocalEquation {
    /// Whether this equation represents the def's result expression.
    pub(crate) output: bool,
    pub(crate) name: VarName,
    pub(crate) ty: Option<SourceType>,
    pub(crate) root: ParsedExprId,
    pub(crate) span: Span,
}

/// The parsed forest is validated before any semantic node is allocated.
pub(crate) struct ParsedSpecification {
    expressions: ParsedExprForest,
    declarations: EcoVec<ParsedDeclaration>,
    /// The archived file this was parsed from; `None` for text no archive
    /// holds.
    source: Option<SourceId>,
}

impl ParsedSpecification {
    pub(crate) fn new(
        builder: ParsedExprBuilder,
        declarations: EcoVec<ParsedDeclaration>,
    ) -> Result<Self, DsrvSyntaxError> {
        let roots = declarations.iter().flat_map(ParsedDeclaration::roots);
        let expressions = builder
            .finish_forest(roots)
            .map_err(super::super::ast::DsrvAstError::from)?;
        Ok(Self {
            expressions,
            declarations,
            source: None,
        })
    }

    /// Make the literal's owning library available through the normal import
    /// path. This imports the module, never its names into the local namespace.
    pub(crate) fn with_duration_import(mut self) -> Self {
        if self.declarations.iter().any(|declaration| matches!(
            declaration,
            ParsedDeclaration::Use { tree, .. }
                if matches!(tree.kind(), super::super::path::ImportKind::Item)
                    && tree.path().iter().map(super::super::path::PathSegment::as_str).eq(["std", "time"])
        )) {
            return self;
        }
        let span = Span::default();
        self.declarations.push(ParsedDeclaration::Use {
            tree: UseTree::new(
                [
                    super::super::path::PathSegment::Module(ModuleName::new("std").unwrap()),
                    super::super::path::PathSegment::Module(ModuleName::new("time").unwrap()),
                ]
                .into(),
                super::super::path::ImportKind::Item,
                span,
            ),
            span,
        });
        self
    }

    /// Record the archived file this was parsed from.
    pub(crate) fn with_source(mut self, source: SourceId) -> Self {
        self.source = Some(source);
        self
    }

    /// The archived file this was parsed from.
    pub(crate) fn source(&self) -> Option<SourceId> {
        self.source
    }

    /// How many declarations the source contained.
    #[cfg(test)]
    pub(crate) fn declaration_count(&self) -> usize {
        self.declarations.len()
    }

    /// Hand the parsed forest and declarations to the expansion stage.
    pub(crate) fn declarations(&self) -> &[ParsedDeclaration] {
        &self.declarations
    }

    /// The root trees, in declaration order.
    ///
    /// The forest is shared storage, so this clones handles rather than
    /// trees.
    pub(crate) fn roots(&self) -> Vec<ParsedExpr> {
        self.expressions.clone().into_roots().collect()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ParsedExprForest,
        EcoVec<ParsedDeclaration>,
        Option<SourceId>,
    ) {
        (self.expressions, self.declarations, self.source)
    }
}

/// The source span of a parsed node, for the expansion stage.
pub(crate) fn span_of(cursor: ParsedExprRef<'_>) -> Span {
    cursor.node().origin.span
}

/// Where a parsed node came from.
pub(crate) fn origin_of(cursor: ParsedExprRef<'_>) -> ParsedOrigin {
    cursor.node().origin.clone()
}
