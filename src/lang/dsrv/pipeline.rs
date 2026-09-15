//! Standard parsing and type-checking entry points for DSRV specifications.

use std::str::FromStr;

#[cfg(test)]
use std::cell::Cell;

use super::{
    ast::{CheckedDsrvSpecification, DsrvSpecification, LanguageMode, ValidatedDsrvSpecification},
    parser,
    type_checker::{self, SemanticErrors, SemanticResult},
};

#[cfg(test)]
thread_local! {
    static PIPELINE_COUNTS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) };
}

#[cfg(test)]
pub(crate) fn reset_test_pipeline_counts() {
    PIPELINE_COUNTS.with(|counts| counts.set((0, 0, 0)));
}

#[cfg(test)]
pub(crate) fn test_pipeline_counts() -> (usize, usize, usize) {
    PIPELINE_COUNTS.with(Cell::get)
}

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
}

impl TypeCheckOptions {
    pub const STRICT: Self = Self {
        mode: TypeCheckMode::Strict,
    };

    pub const GRADUAL: Self = Self {
        mode: TypeCheckMode::Gradual,
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
        #[cfg(test)]
        PIPELINE_COUNTS.with(|counts| {
            let (parse, strict, gradual) = counts.get();
            counts.set((parse + 1, strict, gradual));
        });
        parser::parse_str(source)
    }
}

impl DsrvSpecification {
    pub fn validate<M: LanguageMode>(self) -> SemanticResult<ValidatedDsrvSpecification<M>> {
        type_checker::validate(self)
    }

    /// Type check this specification using the requested policy.
    pub fn type_check(self, options: TypeCheckOptions) -> SemanticResult<CheckedDsrvSpecification> {
        #[cfg(test)]
        PIPELINE_COUNTS.with(|counts| {
            let (parse, strict, gradual) = counts.get();
            counts.set(match options.mode {
                TypeCheckMode::Strict => (parse, strict + 1, gradual),
                TypeCheckMode::Gradual => (parse, strict, gradual + 1),
            });
        });
        match options.mode {
            TypeCheckMode::Strict => type_checker::type_check(self),
            TypeCheckMode::Gradual => type_checker::type_check_gradual(self),
        }
    }
}

impl<M: LanguageMode> ValidatedDsrvSpecification<M> {
    pub fn type_check(self, mode: TypeCheckMode) -> SemanticResult<CheckedDsrvSpecification<M>> {
        match mode {
            TypeCheckMode::Strict => type_checker::check_validated_strict(self),
            TypeCheckMode::Gradual => type_checker::check_validated_gradual(self),
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
    use crate::{
        Distributed, Local, VarName,
        lang::dsrv::{
            ast::SemanticEntry,
            type_checker::{SemanticError, TCType},
        },
    };

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
            specification
                .var_expr_ref(&VarName::new("y"))
                .unwrap()
                .typ(),
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
            specification
                .var_expr_ref(&VarName::new("y"))
                .unwrap()
                .typ(),
            &TCType::Int
        );
    }

    #[test]
    fn checked_source_facade_keeps_parse_and_semantic_errors_distinct() {
        assert!(matches!(
            CheckedDsrvSpecification::parse_with("out y\ny = ", TypeCheckOptions::STRICT),
            Err(DsrvPipelineError::Parse(_))
        ));
        let semantic =
            CheckedDsrvSpecification::parse_with("out y\ny = missing", TypeCheckOptions::STRICT)
                .expect_err("undeclared references must fail common admission");
        assert!(matches!(
            semantic,
            DsrvPipelineError::TypeCheck(errors)
                if errors.iter().any(|error| matches!(
                    error,
                    SemanticError::UndeclaredVariable(message, _)
                        if message.contains("missing")
                ))
        ));
        let type_error =
            CheckedDsrvSpecification::parse_with("out y: Bool\ny = 1", TypeCheckOptions::STRICT)
                .expect_err("well-formed but ill-typed source must reach type checking");
        assert!(matches!(
            type_error,
            DsrvPipelineError::TypeCheck(errors)
                if errors.iter().any(|error| matches!(error, SemanticError::TypeError(_)))
        ));
    }

    #[test]
    fn interleaved_entries_drive_order_and_display() {
        let source = "in z: Int\n\
                      out b: Int\n\
                      b = a + z\n\
                      in q: Int\n\
                      aux a: Int\n\
                      a = q + 1\n\
                      out c: Int\n\
                      c = z - q";
        let specification = source.parse::<DsrvSpecification>().unwrap();
        assert_eq!(
            specification.input_vars_in_order(),
            vec![VarName::new("z"), VarName::new("q")]
        );
        assert!(matches!(
            specification.semantic_entries()[1],
            SemanticEntry::Output {
                ref name,
                annotation: Some(crate::core::StreamType::Int),
                ..
            }
                if name == &VarName::new("b")
        ));
        assert_eq!(
            specification.to_string(),
            "in z: Int\n\
             out b: Int\n\
             b = (a + z)\n\
             in q: Int\n\
             aux a: Int\n\
             a = (q + 1)\n\
             out c: Int\n\
             c = (z - q)\n"
        );
        assert_eq!(
            specification.stream_vars_in_order(),
            [VarName::new("b"), VarName::new("a"), VarName::new("c")]
        );
        assert_eq!(
            specification
                .semantic_entries()
                .iter()
                .filter_map(|entry| {
                    matches!(entry, SemanticEntry::Assignment { .. }).then(|| entry.name().clone())
                })
                .collect::<Vec<_>>(),
            [VarName::new("b"), VarName::new("a"), VarName::new("c")]
        );
    }

    #[test]
    fn duplicate_declarations_fail_common_validation_in_both_modes() {
        let specification = "in z: Int\nout a: Int\nin z: Bool\na = z"
            .parse::<DsrvSpecification>()
            .unwrap();
        for errors in [
            specification.clone().validate::<Local>().unwrap_err(),
            specification.validate::<Distributed>().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::DuplicateDeclaration { variable, .. }
                    if variable == &VarName::new("z")
            )));
        }
    }

    #[test]
    fn distributed_syntax_cannot_obtain_a_local_proof() {
        let specification = "out z: Float\nz = dist(node1, node2)"
            .parse::<DsrvSpecification>()
            .unwrap();
        assert!(specification.clone().validate::<Distributed>().is_ok());
        assert!(specification.validate::<Local>().is_err());
    }

    #[test]
    fn validation_and_typechecking_cover_the_local_distributed_mode_matrix() {
        let annotated = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        for mode in [TypeCheckMode::Strict, TypeCheckMode::Gradual] {
            let local = annotated
                .clone()
                .validate::<Local>()
                .expect("annotated local model should validate");
            let local_checked = local
                .type_check(mode)
                .expect("annotated local model should type-check");
            assert_eq!(
                local_checked
                    .var_expr_ref(&VarName::new("y"))
                    .unwrap()
                    .typ(),
                &TCType::Int
            );

            let distributed = annotated
                .clone()
                .validate::<Distributed>()
                .expect("ordinary model should validate in distributed mode");
            let distributed_checked = distributed
                .type_check(mode)
                .expect("ordinary model should type-check in distributed mode");
            assert_eq!(
                distributed_checked
                    .var_expr_ref(&VarName::new("y"))
                    .unwrap()
                    .typ(),
                &TCType::Int
            );
        }

        let unannotated = "in x\nout y\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let local = unannotated.clone().validate::<Local>().unwrap();
        local
            .clone()
            .type_check(TypeCheckMode::Gradual)
            .expect("gradual inference should accept arithmetic");
        assert!(
            local.type_check(TypeCheckMode::Strict).is_err(),
            "strict checking must not be implied by semantic validity"
        );
        let distributed = unannotated.validate::<Distributed>().unwrap();
        distributed
            .clone()
            .type_check(TypeCheckMode::Gradual)
            .expect("gradual inference should accept arithmetic");
        assert!(
            distributed.type_check(TypeCheckMode::Strict).is_err(),
            "strict checking must not be implied by semantic validity"
        );

        let distributed_operator = "out z: Float\nz = dist(node1, node2)"
            .parse::<DsrvSpecification>()
            .unwrap();
        assert!(
            distributed_operator
                .clone()
                .validate::<Distributed>()
                .is_ok()
        );
        let errors = distributed_operator
            .validate::<Local>()
            .expect_err("local mode must reject distributed operators");
        assert!(errors.iter().any(|error| matches!(
            error,
            SemanticError::UnsupportedDistributionConstraint(_, _)
        )));

        for source in [
            "out z\nz = Tuple(dist(node1, node2))",
            "out z\nz = if true then dist(node1, node2) else dist(node1, node2)",
            "out z\nz = (\\x: Int -> dist(node1, node2))",
            "out z\nz = dynamic(dist(node1, node2), {})",
        ] {
            let specification = source.parse::<DsrvSpecification>().unwrap();
            specification
                .clone()
                .validate::<Distributed>()
                .unwrap_or_else(|errors| {
                    panic!("nested distributed expression rejected: {errors:?}")
                });
            let errors = specification
                .validate::<Local>()
                .expect_err("nested distributed expression must be local-invalid");
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::UnsupportedDistributionConstraint(_, _)
            )));
        }
    }

    #[test]
    fn common_validation_errors_are_mode_independent_and_visit_all_roots() {
        fn duplicate(error: &SemanticError) -> bool {
            matches!(error, SemanticError::DuplicateDeclaration { variable, .. }
                if *variable == VarName::new("z"))
        }
        fn undeclared(error: &SemanticError) -> bool {
            matches!(error, SemanticError::UndeclaredVariable(message, _)
                if message.contains("missing"))
        }
        fn invalid_scope(error: &SemanticError) -> bool {
            matches!(error, SemanticError::InvalidRuntimeScope(message, _)
                if message.contains("missing"))
        }
        let cases = [
            (
                "in z\nin z\nout result\nresult = z",
                duplicate as fn(&SemanticError) -> bool,
            ),
            ("out result\nresult = missing", undeclared),
            (
                "in source: Str\nout result\nresult = dynamic(source: Int, {missing})",
                invalid_scope,
            ),
        ];
        for (source, expected) in cases {
            let specification = source.parse::<DsrvSpecification>().unwrap();
            let local_errors = specification.clone().validate::<Local>().unwrap_err();
            let distributed_errors = specification.validate::<Distributed>().unwrap_err();
            assert!(
                local_errors.iter().any(expected),
                "local errors: {local_errors:?}"
            );
            assert!(
                distributed_errors.iter().any(expected),
                "distributed errors: {distributed_errors:?}"
            );
        }

        for expression in [
            "dynamic(source: Int, {x, x})",
            "defer(source: Int, {x, x})",
            "dynamic(source: Int, {z})",
            "defer(source: Int, {z})",
            "dynamic(source: Int, {missing})",
            "defer(source: Int, {missing})",
        ] {
            let source = format!("in x: Int\nin source: Str\nout z: Int\nz = {expression}");
            let specification = source.parse::<DsrvSpecification>().unwrap();
            for errors in [
                specification.clone().validate::<Local>().unwrap_err(),
                specification.validate::<Distributed>().unwrap_err(),
            ] {
                assert!(
                    errors
                        .iter()
                        .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _)))
                );
            }
        }
        let empty_scope = "in source: Str\nout z: Int\nz = dynamic(source: Int, {})"
            .parse::<DsrvSpecification>()
            .unwrap();
        empty_scope.clone().validate::<Local>().unwrap();
        empty_scope.validate::<Distributed>().unwrap();

        let unused_aux = "in x\nout result\naux unused\nresult = x\nunused = missing"
            .parse::<DsrvSpecification>()
            .unwrap();
        for errors in [
            unused_aux.clone().validate::<Local>().unwrap_err(),
            unused_aux.validate::<Distributed>().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::UndeclaredVariable(message, _)
                    if message.contains("missing")
            )));
        }
    }

    #[test]
    fn lambda_shadowing_does_not_leak_between_siblings() {
        let shadowed = "in x: Int\nout result: Int\nresult = (\\x: Int -> x)(1) + x"
            .parse::<DsrvSpecification>()
            .unwrap();
        shadowed
            .clone()
            .validate::<Local>()
            .expect("lambda shadowing should be valid");
        shadowed
            .validate::<Distributed>()
            .expect("lambda shadowing should be valid in distributed mode");

        let sibling = "out result: Int\nresult = Tuple((\\x: Int -> x)(1), missing)"
            .parse::<DsrvSpecification>()
            .unwrap();
        for errors in [
            sibling.clone().validate::<Local>().unwrap_err(),
            sibling.validate::<Distributed>().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::UndeclaredVariable(message, _)
                    if message.contains("missing")
            )));
        }
    }

    #[test]
    fn duplicate_declaration_diagnostics_preserve_source_order_and_spans() {
        for source in [
            "in z: Int\nin z: Bool\nout result\nresult = z",
            "in z: Int\nin z: Bool\nin z: Str\nout result\nresult = z",
        ] {
            let specification = source.parse::<DsrvSpecification>().unwrap();
            let errors = specification.validate::<Local>().unwrap_err();
            let duplicate_errors = errors
                .iter()
                .filter_map(|error| match error {
                    SemanticError::DuplicateDeclaration {
                        variable,
                        first,
                        duplicate,
                    } if *variable == VarName::new("z") => Some((*first, *duplicate)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(duplicate_errors.len(), source.matches("in z:").count() - 1);
            for (index, (first, duplicate)) in duplicate_errors.into_iter().enumerate() {
                assert_eq!(&source[first.to_range()], "in z: Int");
                let expected_duplicate = match index {
                    0 => "in z: Bool",
                    1 => "in z: Str",
                    _ => unreachable!(),
                };
                assert_eq!(&source[duplicate.to_range()], expected_duplicate);
            }
        }
    }

    #[test]
    fn duplicate_declaration_matrix_covers_roles_annotations_and_orders() {
        let role_pairs = [
            ("in", "in"),
            ("out", "out"),
            ("aux", "aux"),
            ("in", "out"),
            ("out", "in"),
            ("in", "aux"),
            ("aux", "in"),
            ("out", "aux"),
            ("aux", "out"),
        ];
        let annotations = [
            ("", ""),
            (": Int", ": Int"),
            (": Int", ": Bool"),
            (": Int", ""),
        ];
        for (left_role, right_role) in role_pairs {
            for (left_annotation, right_annotation) in annotations {
                let source = format!(
                    "{left_role} z{left_annotation}\n\
                     {right_role} z{right_annotation}\n\
                     out result\n\
                     result = 1"
                );
                let specification = source.parse::<DsrvSpecification>().unwrap();
                assert_eq!(
                    specification
                        .semantic_entries()
                        .iter()
                        .filter(|entry| entry.name() == &VarName::new("z"))
                        .count(),
                    2
                );
                for errors in [
                    specification.clone().validate::<Local>().unwrap_err(),
                    specification.validate::<Distributed>().unwrap_err(),
                ] {
                    assert!(errors.iter().any(|error| matches!(
                        error,
                        SemanticError::DuplicateDeclaration { variable, .. }
                            if *variable == VarName::new("z")
                    )));
                }
            }
        }
    }

    #[test]
    fn duplicate_declaration_validation_reports_independent_conflicts_in_order() {
        let specification = "in z: Int\nout z: Bool\nin q\naux q\nout result\nresult = 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        let errors = specification.validate::<Local>().unwrap_err();
        let names = errors
            .iter()
            .filter_map(|error| match error {
                SemanticError::DuplicateDeclaration { variable, .. } => Some(variable.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(names, [VarName::new("z"), VarName::new("q")]);
    }
}
