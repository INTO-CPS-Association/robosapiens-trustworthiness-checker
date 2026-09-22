//! Standard parsing and type-checking entry points for DSRV specifications.

use std::str::FromStr;

#[cfg(test)]
use std::cell::Cell;

use super::{
    ast::{CheckedDsrvSpecification, DsrvSpecification, ValidatedDsrvSpecification},
    diagnostics::{SemanticAnalysisReport, SemanticResult},
    parser, type_checker,
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
    /// Check the invariants every specification must satisfy, whatever the
    /// checking policy. This neither infers types nor warns.
    pub fn validate(self) -> SemanticResult<ValidatedDsrvSpecification> {
        type_checker::validate(self)
    }

    /// Semantically check this specification using the requested policy,
    /// reporting its errors or the checked specification together with any
    /// warnings.
    pub fn check(
        self,
        options: TypeCheckOptions,
    ) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
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

impl ValidatedDsrvSpecification {
    /// Semantically check this validated specification using the requested
    /// policy.
    pub fn check(
        self,
        options: TypeCheckOptions,
    ) -> SemanticAnalysisReport<CheckedDsrvSpecification> {
        match options.mode {
            TypeCheckMode::Strict => type_checker::check_validated_strict(self),
            TypeCheckMode::Gradual => type_checker::check_validated_gradual(self),
        }
    }
}

impl CheckedDsrvSpecification {
    /// Parse and check a specification using the requested policy. A parse
    /// failure stops before checking; otherwise the check is reported.
    pub fn parse_with(
        source: &str,
        options: TypeCheckOptions,
    ) -> Result<SemanticAnalysisReport<Self>, parser::DsrvParseError> {
        Ok(source.parse::<DsrvSpecification>()?.check(options))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VarName,
        dsrv_fixtures::WithoutWarnings,
        lang::dsrv::{ast::Declaration, diagnostics::SemanticError, type_checker::TCType},
    };

    /// Parse and check `source`, which must parse and must not warn.
    #[track_caller]
    fn checked(
        source: &str,
        options: TypeCheckOptions,
    ) -> SemanticResult<CheckedDsrvSpecification> {
        CheckedDsrvSpecification::parse_with(source, options)
            .unwrap_or_else(|errors| panic!("{source}: {errors:?}"))
            .without_warnings()
    }

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
    fn operations_on_dynamic_values_are_checked_at_run_time() {
        // Each operation on a value of type `Any` gives the type the value
        // does not determine statically.
        let cases = [
            ("out y = Map.get(v, \"k\")", TCType::Any),
            ("out y = v.field", TCType::Any),
            ("out y = List.get(v, 0)", TCType::Any),
            ("out y = List.head(v)", TCType::Any),
            ("out y = List.tail(v)", TCType::Any),
            ("out y = List.len(v)", TCType::Int),
            ("out y = List.append(v, 1)", TCType::Any),
            ("out y = Map.insert(v, \"k\", 1)", TCType::Any),
            ("out y = Map.remove(v, \"k\")", TCType::Any),
            ("out y = Map.has_key(v, \"k\")", TCType::Bool),
            (
                "out y = List.map(\\x -> x, v)",
                TCType::List(Box::new(TCType::Any)),
            ),
            ("out y = v(1)", TCType::Any),
            ("out y = partial(v, 1)", TCType::Any),
        ];
        for (equation, expected) in cases {
            // Unannotated, under gradual checking.
            let source = format!("in v\n{equation}");
            let specification = checked(&source, TypeCheckOptions::GRADUAL)
                .unwrap_or_else(|errors| panic!("{source}: {errors:?}"));
            assert_eq!(
                specification
                    .var_expr_ref(&VarName::new("y"))
                    .unwrap()
                    .typ(),
                &expected,
                "{source}"
            );
            // Written as `Any`, under strict checking.
            let annotated = format!(
                "in v: Any\n{}",
                equation.replacen("out y", &format!("out y: {}", source_type(&expected)), 1)
            );
            checked(&annotated, TypeCheckOptions::STRICT)
                .unwrap_or_else(|errors| panic!("{annotated}: {errors:?}"));
        }
        // A value with a known type that is not a collection is still an error.
        assert!(
            checked(
                "in v: Int\nout y: Int = List.len(v)",
                TypeCheckOptions::STRICT
            )
            .is_err()
        );
    }

    fn source_type(typ: &TCType) -> &'static str {
        match typ {
            TCType::Any => "Any",
            TCType::Int => "Int",
            TCType::Bool => "Bool",
            TCType::List(_) => "List<Any>",
            other => panic!("no source spelling for {other}"),
        }
    }

    #[test]
    fn struct_equality_needs_operands_of_the_same_struct_type() {
        let strict = |source: &str| checked(source, TypeCheckOptions::STRICT);
        strict("in a: Struct<x: Int>\nin b: Struct<x: Int>\nout same: Bool = a == b").unwrap();
        strict("in n: Int\nout same: Bool = {x: n, y: 2} != {y: 2, x: 1}").unwrap();
        for source in [
            "in n: Int\nout same: Bool = {x: 1} == {x: \"a\"}",
            "in n: Int\nout same: Bool = {x: 1} == {x: 1, y: 2}",
            "in n: Int\nout same: Bool = {x: 1} == n",
        ] {
            let Err(errors) = strict(source) else {
                panic!("{source} must not type check");
            };
            assert!(
                errors
                    .iter()
                    .any(|error| format!("{error:?}").contains("equality operator")),
                "{source}: {errors:?}"
            );
        }
    }

    #[test]
    fn lambda_parameter_types_are_inferred_from_their_context() {
        let strict = |source: &str| {
            checked(source, TypeCheckOptions::STRICT)
                .unwrap_or_else(|errors| panic!("{source}: {errors:?}"))
        };
        for (source, output, expected) in [
            (
                "in xs: List<Int>\nout ys: List<Int> = List.map(\\x -> x + 1, xs)",
                "ys",
                TCType::List(Box::new(TCType::Int)),
            ),
            (
                "in xs: List<Int>\nout ys: List<Int> = List.filter(\\x -> x > 0, xs)",
                "ys",
                TCType::List(Box::new(TCType::Int)),
            ),
            (
                "in xs: List<Int>\nout s: Int = List.fold(\\acc, x -> acc + x, 0, xs)",
                "s",
                TCType::Int,
            ),
            (
                "in x: Int\nout y: Int = (\\n -> n * 2)(x)",
                "y",
                TCType::Int,
            ),
            (
                "in x: Float\nout y: Float = (\\n, m -> n + m)(x, 1.5)",
                "y",
                TCType::Float,
            ),
        ] {
            let checked = strict(source);
            assert_eq!(
                checked.var_expr_ref(&VarName::new(output)).unwrap().typ(),
                &expected,
                "{source}"
            );
        }

        // Annotations still win, and must agree with the context.
        strict("in xs: List<Int>\nout ys: List<Int> = List.map(\\x: Int -> x + 1, xs)");
        assert!(
            checked(
                "in xs: List<Int>\nout ys: List<Int> = List.map(\\x: Str -> 1, xs)",
                TypeCheckOptions::STRICT,
            )
            .is_err()
        );
    }

    #[test]
    fn a_lambda_parameter_without_annotation_or_context_is_strict_error_and_gradual_any() {
        // A lambda stored in a list is checked with nothing to infer from.
        let source = "in x: Int\nout y: Int = List.len(List(\\f -> 1))";
        let Err(errors) = checked(source, TypeCheckOptions::STRICT) else {
            panic!("strict checking must reject an uninferable parameter");
        };
        assert!(
            errors.iter().any(|error| matches!(
                error,
                SemanticError::MissingTypeAnnotation(message, Some(_), _)
                    if message.contains("lambda parameter `f`")
            )),
            "{errors:?}"
        );
        checked(source, TypeCheckOptions::GRADUAL)
            .expect("gradual checking gives the parameter the dynamic type");
    }

    #[test]
    fn inferred_lambda_parameters_print_without_annotations() {
        let source = "in xs: List<Int>\nout ys: List<Int>\nys = List.map(\\x -> x + 1, xs)\n";
        let spec = source.parse::<DsrvSpecification>().unwrap();
        let printed = spec.to_string();
        assert!(printed.contains("\\x -> "), "{printed}");
        assert_eq!(printed.parse::<DsrvSpecification>().unwrap(), spec);
    }

    #[test]
    fn check_reports_the_checked_specification_and_no_warnings() {
        let report = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap()
            .check(TypeCheckOptions::STRICT);
        assert!(report.warnings().is_empty());
        let (specification, warnings) = report.into_parts();
        assert!(warnings.is_empty());
        assert_eq!(
            specification
                .unwrap()
                .var_expr_ref(&VarName::new("y"))
                .unwrap()
                .typ(),
            &TCType::Int
        );
        let report = "in x\nout y\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap()
            .check(TypeCheckOptions::STRICT);
        assert!(report.warnings().is_empty());
        assert!(report.result().is_err());
    }

    #[test]
    fn every_check_entry_point_records_its_policy_and_elaboration_keeps_it() {
        let source = "in x: Int\nout y: Int\ny = x + 1";
        for options in [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL] {
            let spec = source.parse::<DsrvSpecification>().unwrap();
            let direct = spec.clone().check(options).without_warnings().unwrap();
            assert_eq!(direct.check_mode(), options.mode);
            let validated = spec.clone().validate().unwrap();
            let validated = validated.check(options).without_warnings().unwrap();
            assert_eq!(validated.check_mode(), options.mode);
            let parsed = checked(source, options).unwrap();
            assert_eq!(parsed.check_mode(), options.mode);

            let elaborated = parsed.elaborate();
            assert_eq!(elaborated.check_mode(), options.mode);
            assert_eq!(elaborated.source().check_mode(), options.mode);
            assert_eq!(elaborated.checked().check_mode(), options.mode);
            let elaborated = spec
                .check_and_elaborate(options)
                .without_warnings()
                .unwrap();
            assert_eq!(elaborated.check_mode(), options.mode);
            let parsed = crate::ElaboratedDsrvSpecification::parse_with(source, options)
                .unwrap()
                .without_warnings()
                .unwrap();
            assert_eq!(parsed.check_mode(), options.mode);
            // The policy is not part of the printed specification.
            assert_eq!(
                parsed.to_string(),
                source.parse::<DsrvSpecification>().unwrap().to_string()
            );
        }
    }

    #[test]
    fn parse_with_supports_gradual_checking() {
        let specification = checked("in x\nout y\ny = x + 1", TypeCheckOptions::GRADUAL).unwrap();

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
        assert!(
            CheckedDsrvSpecification::parse_with("out y\ny = ", TypeCheckOptions::STRICT).is_err()
        );
        let semantic = checked("out y\ny = missing", TypeCheckOptions::STRICT)
            .expect_err("undeclared references must fail common admission");
        assert!(semantic.iter().any(|error| matches!(
            error,
            SemanticError::UndeclaredVariable(message, _, _) if message.contains("missing")
        )));
        let type_error = checked("out y: Bool\ny = 1", TypeCheckOptions::STRICT)
            .expect_err("well-formed but ill-typed source must reach type checking");
        assert!(
            type_error
                .iter()
                .any(|error| matches!(error, SemanticError::TypeError(_)))
        );
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
            specification.declarations()[1],
            Declaration::Output {
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
                .declarations()
                .iter()
                .filter_map(|entry| {
                    matches!(entry, Declaration::Equation { .. })
                        .then(|| entry.stream().expect("a stream declaration").clone())
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
            specification.clone().validate().unwrap_err(),
            specification.validate().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::DuplicateDeclaration { variable, .. }
                    if variable == &VarName::new("z")
            )));
        }
    }

    #[test]
    fn distribution_primitives_need_the_distributed_dialect() {
        use crate::lang::dsrv::{DsrvParseError, LanguageError};
        for body in [
            "out z: Float\nz = dist(node1, node2)",
            "out z\nz = Tuple(dist(node1, node2))",
            "out z\nz = if true then dist(node1, node2) else dist(node1, node2)",
            "out z\nz = (\\x: Int -> dist(node1, node2))",
            "out z\nz = dynamic(dist(node1, node2), {})",
            "in x\nout z\nz = monitored_at(x, node1)",
        ] {
            assert!(
                matches!(
                    body.parse::<DsrvSpecification>(),
                    Err(DsrvParseError::Language(
                        LanguageError::NeedsDistributed { .. }
                    ))
                ),
                "{body}"
            );
            let specification = format!("language distributed\n{body}")
                .parse::<DsrvSpecification>()
                .unwrap_or_else(|error| panic!("{body}: {error}"));
            specification
                .validate()
                .unwrap_or_else(|errors| panic!("{body}: {errors:?}"));
        }
    }

    #[test]
    fn validation_and_typechecking_cover_the_local_distributed_mode_matrix() {
        let annotated = "in x: Int\nout y: Int\ny = x + 1"
            .parse::<DsrvSpecification>()
            .unwrap();
        for mode in [TypeCheckMode::Strict, TypeCheckMode::Gradual] {
            let local = annotated
                .clone()
                .validate()
                .expect("annotated local model should validate");
            let local_checked = local
                .check(TypeCheckOptions { mode })
                .without_warnings()
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
                .validate()
                .expect("ordinary model should validate in distributed mode");
            let distributed_checked = distributed
                .check(TypeCheckOptions { mode })
                .without_warnings()
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
        let local = unannotated.clone().validate().unwrap();
        local
            .clone()
            .check(TypeCheckOptions::GRADUAL)
            .without_warnings()
            .expect("gradual inference should accept arithmetic");
        assert!(
            local.check(TypeCheckOptions::STRICT).result().is_err(),
            "strict checking must not be implied by semantic validity"
        );
        let distributed = unannotated.validate().unwrap();
        distributed
            .clone()
            .check(TypeCheckOptions::GRADUAL)
            .without_warnings()
            .expect("gradual inference should accept arithmetic");
        assert!(
            distributed
                .check(TypeCheckOptions::STRICT)
                .result()
                .is_err(),
            "strict checking must not be implied by semantic validity"
        );
    }

    #[test]
    fn common_validation_errors_are_mode_independent_and_visit_all_roots() {
        fn duplicate(error: &SemanticError) -> bool {
            matches!(error, SemanticError::DuplicateDeclaration { variable, .. }
                if *variable == VarName::new("z"))
        }
        fn undeclared(error: &SemanticError) -> bool {
            matches!(error, SemanticError::UndeclaredVariable(message, _, _)
                if message.contains("missing"))
        }
        fn invalid_scope(error: &SemanticError) -> bool {
            matches!(error, SemanticError::InvalidRuntimeScope(message, _, _)
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
            let local_errors = specification.clone().validate().unwrap_err();
            let distributed_errors = specification.validate().unwrap_err();
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
                specification.clone().validate().unwrap_err(),
                specification.validate().unwrap_err(),
            ] {
                assert!(
                    errors
                        .iter()
                        .any(|error| matches!(error, SemanticError::InvalidRuntimeScope(_, _, _)))
                );
            }
        }
        let empty_scope = "in source: Str\nout z: Int\nz = dynamic(source: Int, {})"
            .parse::<DsrvSpecification>()
            .unwrap();
        empty_scope.clone().validate().unwrap();
        empty_scope.validate().unwrap();

        let unused_aux = "in x\nout result\naux unused\nresult = x\nunused = missing"
            .parse::<DsrvSpecification>()
            .unwrap();
        for errors in [
            unused_aux.clone().validate().unwrap_err(),
            unused_aux.validate().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::UndeclaredVariable(message, _, _)
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
            .validate()
            .expect("lambda shadowing should be valid");
        shadowed
            .validate()
            .expect("lambda shadowing should be valid in distributed mode");

        let sibling = "out result: Int\nresult = Tuple((\\x: Int -> x)(1), missing)"
            .parse::<DsrvSpecification>()
            .unwrap();
        for errors in [
            sibling.clone().validate().unwrap_err(),
            sibling.validate().unwrap_err(),
        ] {
            assert!(errors.iter().any(|error| matches!(
                error,
                SemanticError::UndeclaredVariable(message, _, _)
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
            let errors = specification.validate().unwrap_err();
            let duplicate_errors = errors
                .iter()
                .filter_map(|error| match error {
                    SemanticError::DuplicateDeclaration {
                        variable,
                        first,
                        duplicate,
                        ..
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
                        .declarations()
                        .iter()
                        .filter(|entry| entry.stream() == Some(&VarName::new("z")))
                        .count(),
                    2
                );
                for errors in [
                    specification.clone().validate().unwrap_err(),
                    specification.validate().unwrap_err(),
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
        let errors = specification.validate().unwrap_err();
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
