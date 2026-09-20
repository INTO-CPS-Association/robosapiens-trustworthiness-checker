//! Text supplied at run time to `dynamic` and `defer`.
//!
//! Every runtime checks such text when it arrives and refuses text that does
//! not check, whether it then consults the types or not. The text is parsed
//! in the source context of the node it was supplied to, may call the same
//! defs that node's file could, and is checked against the type and
//! environment elaboration gave that node.

use std::collections::BTreeMap;

use crate::core::StreamType;
use crate::lang::dsrv::ast::{AstShared, CheckedExpr, Expr};
use crate::lang::dsrv::expand::functions::Callable;
use crate::lang::dsrv::parser::{DsrvParseError, parse_expr_with_functions};
use crate::lang::dsrv::source::SourceContext;
use crate::lang::dsrv::type_checker::{
    SemanticErrors, StreamTypeEnvironment, TCType, check_expression,
};

/// The type and environment runtime text is checked against: what elaboration
/// gave the `dynamic` or `defer` node the text was supplied to.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeTextTyping {
    pub(crate) environment: AstShared<StreamTypeEnvironment>,
    pub(crate) expected: TCType,
}

/// How text supplied to one `dynamic` or `defer` node is accepted.
#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeText {
    context: AstShared<SourceContext>,
    /// The defs the node's file could call, which its text may call too.
    callable: AstShared<Callable>,
    /// `None` only where no elaborated node stands behind the text: an
    /// expression built directly by a test, or one nested inside text that is
    /// itself evaluated without types. Such text is checked with every
    /// variable it mentions, and its result, of type `Any`.
    typing: Option<RuntimeTextTyping>,
}

/// Why runtime text was refused.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeTextError {
    #[error("runtime text {text:?} does not parse: {error}")]
    Parse {
        text: String,
        #[source]
        error: DsrvParseError,
    },
    #[error("runtime text {text:?} failed type checking: {errors:?}")]
    TypeCheck {
        text: String,
        errors: SemanticErrors,
    },
}

impl RuntimeText {
    /// Runtime text for a node with `context` that may call `callable`,
    /// checked against `typing`.
    pub(crate) fn new(
        context: AstShared<SourceContext>,
        callable: AstShared<Callable>,
        typing: Option<RuntimeTextTyping>,
    ) -> Self {
        Self {
            context,
            callable,
            typing,
        }
    }

    pub(crate) fn parse(&self, text: &str) -> Result<Expr, RuntimeTextError> {
        parse_expr_with_functions(
            text,
            AstShared::clone(&self.context),
            AstShared::clone(&self.callable),
        )
        .map_err(|error| RuntimeTextError::Parse {
            text: text.to_owned(),
            error,
        })
    }

    pub(crate) fn check(&self, text: &str, expr: Expr) -> Result<CheckedExpr, RuntimeTextError> {
        let checked = match &self.typing {
            Some(typing) => check_expression(expr, &typing.expected, &typing.environment),
            None => {
                let environment = expr
                    .as_ref()
                    .free_variables()
                    .into_iter()
                    .map(|name| (name, StreamType::Any))
                    .collect::<BTreeMap<_, _>>();
                check_expression(expr, &TCType::Any, &AstShared::new(environment))
            }
        };
        checked.map_err(|errors| RuntimeTextError::TypeCheck {
            text: text.to_owned(),
            errors,
        })
    }

    /// Parse and check `text`, giving the expression a runtime evaluates.
    pub(crate) fn accept(&self, text: &str) -> Result<CheckedExpr, RuntimeTextError> {
        let expr = self.parse(text)?;
        self.check(text, expr)
    }
}
