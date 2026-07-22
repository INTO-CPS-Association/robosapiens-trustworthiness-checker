//! Standard parsing and type-checking entry points for DSRV specifications.

use std::str::FromStr;

use super::{
    ast::{CheckedDsrvSpecification, DsrvSpecification},
    parser,
    type_checker::{self, SemanticErrors, SemanticResult},
};

/// Type-inference policy used when checking a DSRV specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeCheckMode {
    Strict,
    Gradual,
}

/// Options controlling semantic validation and type checking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypeCheckOptions {
    pub mode: TypeCheckMode,
    pub distributed: bool,
}

impl TypeCheckOptions {
    pub const STRICT: Self = Self {
        mode: TypeCheckMode::Strict,
        distributed: false,
    };

    pub const GRADUAL: Self = Self {
        mode: TypeCheckMode::Gradual,
        distributed: false,
    };

    pub const STRICT_DISTRIBUTED: Self = Self {
        mode: TypeCheckMode::Strict,
        distributed: true,
    };

    pub const GRADUAL_DISTRIBUTED: Self = Self {
        mode: TypeCheckMode::Gradual,
        distributed: true,
    };
}

/// Failure while parsing and type checking a DSRV specification.
#[derive(Debug, thiserror::Error)]
pub enum DsrvPipelineError {
    #[error("failed to parse DSRV specification: {0}")]
    Parse(#[source] parser::DsrvParseError),

    #[error("DSRV specification failed semantic validation: {0:?}")]
    TypeCheck(SemanticErrors),
}

impl FromStr for DsrvSpecification {
    type Err = parser::DsrvParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        parser::parse_str(source)
    }
}

impl DsrvSpecification {
    /// Type check this specification using the requested policy.
    pub fn type_check(self, options: TypeCheckOptions) -> SemanticResult<CheckedDsrvSpecification> {
        match options.mode {
            TypeCheckMode::Strict => type_checker::type_check(self, options.distributed),
            TypeCheckMode::Gradual => type_checker::type_check_gradual(self, options.distributed),
        }
    }
}

impl FromStr for CheckedDsrvSpecification {
    type Err = DsrvPipelineError;

    /// Parse and strictly type check a non-distributed specification.
    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Self::parse_with(source, TypeCheckOptions::STRICT)
    }
}

impl CheckedDsrvSpecification {
    /// Parse and type check a specification using the requested policy.
    pub fn parse_with(source: &str, options: TypeCheckOptions) -> Result<Self, DsrvPipelineError> {
        source
            .parse::<DsrvSpecification>()
            .map_err(DsrvPipelineError::Parse)?
            .type_check(options)
            .map_err(DsrvPipelineError::TypeCheck)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VarName, lang::dsrv::type_checker::TCType};

    #[test]
    fn unchecked_specifications_parse_with_from_str() {
        let specification = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();

        assert!(specification.input_vars().contains(&VarName::new("x")));
        assert!(specification.output_vars().contains(&VarName::new("y")));
        assert!(specification.var_expr_ref(&VarName::new("y")).is_some());
    }

    #[test]
    fn checked_from_str_uses_strict_non_distributed_checking() {
        let specification = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();

        assert_eq!(
            specification.var_expr(&VarName::new("y")).unwrap().typ(),
            &TCType::Int
        );
        assert!(
            "in x\nout y\ny = x + 1"
                .parse::<CheckedDsrvSpecification>()
                .is_err()
        );
    }

    #[test]
    fn parse_with_supports_gradual_checking() {
        let specification = CheckedDsrvSpecification::parse_with(
            "in x\nout y\ny = x + 1",
            TypeCheckOptions::GRADUAL,
        )
        .unwrap();

        assert_eq!(
            specification.var_expr(&VarName::new("y")).unwrap().typ(),
            &TCType::Int
        );
    }
}
