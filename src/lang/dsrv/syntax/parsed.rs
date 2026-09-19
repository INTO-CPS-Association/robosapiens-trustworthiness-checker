//! Private source tree. Parsing resolves no names: unresolved type names are
//! kept for the expansion stage.

// The generated family includes borrowed views; expansion uses stored kinds.
#![allow(dead_code)]

use ecow::{EcoString, EcoVec};

use crate::core::{BinaryOperator, VarName};
use crate::distributed::distribution_graphs::NodeName;

use super::super::ast::{ReconfigurableExprScope, SyntaxLiteral, VarOrNodeName};
use super::super::source::{AliasDeclaration, SourceType};
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
        metadata: span: Span = Span::default(),
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        If(condition: child, then_expr: child, else_expr: child),
        SIndex(input: child, offset: copy(u64)),
        Val(value: into_data(SyntaxLiteral)),
        BinOp(left: child, right: child, operator: copy(BinaryOperator)),
        Var(variable: data(VarName)),
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
        Lambda(parameters: data(EcoVec<(VarName, SourceType)>), body: child),
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
        MGet(map: child, key: data(EcoString)),
        SGet(value: child, key: data(EcoString)),
        MInsert(map: child, key: data(EcoString), value: child),
        MRemove(map: child, key: data(EcoString)),
        MHasKey(map: child, key: data(EcoString)),
        Sin(value: child),
        Cos(value: child),
        Tan(value: child),
        Abs(value: child),
        MonitoredAt(variable: data(VarName), node: data(NodeName)),
        Dist(left: data(VarOrNodeName), right: data(VarOrNodeName)),
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ParsedDeclaration {
    Input(VarName, Option<SourceType>, Span),
    Output(VarName, Option<SourceType>, Span),
    Aux(VarName, Option<SourceType>, Span),
    Assignment(VarName, ParsedExprId, Span),
    Alias(AliasDeclaration),
    /// `language <name>`, with the name unchecked.
    Language(EcoString, Span),
    /// `edition <year>-<month>`, as written.
    Edition(EcoString, Span),
}

/// The parsed forest is validated before any semantic node is allocated.
pub(crate) struct ParsedSpecification {
    expressions: ParsedExprForest,
    declarations: EcoVec<ParsedDeclaration>,
}

impl ParsedSpecification {
    pub(crate) fn new(
        builder: ParsedExprBuilder,
        declarations: EcoVec<ParsedDeclaration>,
    ) -> Result<Self, DsrvSyntaxError> {
        let roots = declarations
            .iter()
            .filter_map(|declaration| match declaration {
                ParsedDeclaration::Assignment(_, root, _) => Some(*root),
                _ => None,
            });
        let expressions = builder
            .finish_forest(roots)
            .map_err(super::super::ast::DsrvAstError::from)?;
        Ok(Self {
            expressions,
            declarations,
        })
    }

    /// How many declarations the source contained.
    #[cfg(test)]
    pub(crate) fn declaration_count(&self) -> usize {
        self.declarations.len()
    }

    /// Hand the parsed forest and declarations to the expansion stage.
    pub(crate) fn into_parts(self) -> (ParsedExprForest, EcoVec<ParsedDeclaration>) {
        (self.expressions, self.declarations)
    }
}

/// The source span of a parsed node, for the expansion stage.
pub(crate) fn span_of(cursor: ParsedExprRef<'_>) -> Span {
    cursor.node().span
}
