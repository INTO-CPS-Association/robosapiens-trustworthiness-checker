//! Pure, human-oriented rendering of expanded DSRV programs.
//!
//! This output deliberately resembles source to make inspection convenient,
//! but it is a report: it is not promised to parse, remain byte-stable, or be
//! suitable as a scripting format.

use std::fmt::Write;

use crate::core::StreamType;

use super::{
    ast::{CheckedDsrvSpecification, Declaration, DsrvSpecification},
    path::TypePath,
    program::{ActivatedModule, ModuleOrigin},
    source::SourceTypeDisplay,
};

pub struct ExpandedProgramView<'a> {
    pub specification: &'a DsrvSpecification,
    pub modules: &'a [ActivatedModule],
}

pub struct CheckedProgramView<'a> {
    pub specification: &'a CheckedDsrvSpecification,
    pub modules: &'a [ActivatedModule],
}

pub fn render_expanded_program(view: ExpandedProgramView<'_>) -> String {
    render(
        view.specification,
        view.modules,
        "expanded, unchecked",
        |_, annotation| annotation,
    )
}

pub fn render_checked_program(view: CheckedProgramView<'_>) -> String {
    render(
        view.specification.unchecked(),
        view.modules,
        match view.specification.check_mode() {
            super::TypeCheckMode::Strict => "checked (strict)",
            super::TypeCheckMode::Gradual => "checked (gradual)",
        },
        |name, _| view.specification.type_annotation(name),
    )
}

fn render<'a>(
    specification: &'a DsrvSpecification,
    modules: &[ActivatedModule],
    state: &str,
    annotation_for: impl Fn(&'a crate::VarName, Option<&'a StreamType>) -> Option<&'a StreamType>,
) -> String {
    let mut report = String::new();
    writeln!(
        report,
        "# tc-expand inspection ({state}); non-standalone source report"
    )
    .unwrap();
    writeln!(
        report,
        "# language: {}",
        specification.source_context().language()
    )
    .unwrap();
    writeln!(report, "# activated modules:").unwrap();
    if modules.is_empty() {
        writeln!(report, "#   (none)").unwrap();
    } else {
        for module in modules {
            match &module.origin {
                ModuleOrigin::Filesystem(file) => {
                    writeln!(report, "#   {} [filesystem: {file}]", module.path).unwrap()
                }
                ModuleOrigin::Embedded => {
                    writeln!(report, "#   {} [embedded catalogue]", module.path).unwrap()
                }
            }
        }
    }
    writeln!(report).unwrap();

    for declaration in specification.declarations() {
        match declaration {
            Declaration::Input {
                name, annotation, ..
            }
            | Declaration::Output {
                name, annotation, ..
            }
            | Declaration::Aux {
                name, annotation, ..
            } => {
                let keyword = match declaration {
                    Declaration::Input { .. } => "in",
                    Declaration::Output { .. } => "out",
                    Declaration::Aux { .. } => "aux",
                    _ => unreachable!(),
                };
                write!(report, "{keyword} {name}").unwrap();
                if let Some(typ) = annotation_for(name, annotation.as_ref()) {
                    write!(report, ": {}", SourceTypeDisplay(typ)).unwrap();
                }
                writeln!(report).unwrap();
            }
            Declaration::Equation { name, .. } => {
                let expression = specification
                    .var_expr_ref(name)
                    .expect("an expanded equation has an expression");
                writeln!(report, "{name} = {expression}").unwrap();
            }
            Declaration::TypeAlias { name, .. } => {
                let path = TypePath::local(name.clone());
                if let Some(alias) = specification.source_context().generic().get(&path) {
                    let parameters = alias
                        .parameters
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    writeln!(
                        report,
                        "type {name}<{parameters}> = <generic alias; expanded when instantiated>"
                    )
                    .unwrap();
                } else {
                    let typ = specification
                        .source_context()
                        .get(&path)
                        .expect("a non-generic alias has an expanded type");
                    writeln!(report, "type {name} = {}", SourceTypeDisplay(typ)).unwrap();
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::{TypeCheckOptions, parser::parse_str};

    #[test]
    fn expanded_report_marks_itself_and_keeps_declaration_order() {
        let specification = parse_str("out z: Int\nin x: Int\nz = x + 1").expect("fixture expands");
        let report = render_expanded_program(ExpandedProgramView {
            specification: &specification,
            modules: &[],
        });
        assert!(report.starts_with(
            "# tc-expand inspection (expanded, unchecked); non-standalone source report\n"
        ));
        assert!(report.find("out z").unwrap() < report.find("in x").unwrap());
        assert!(report.find("in x").unwrap() < report.find("z =").unwrap());
    }

    #[test]
    fn module_origins_and_checked_annotations_are_visible() {
        let specification = parse_str("out y\ny = 1").unwrap();
        let checked = specification
            .check(TypeCheckOptions::GRADUAL)
            .into_parts()
            .0
            .unwrap();
        let modules = [
            ActivatedModule {
                path: "local".into(),
                origin: ModuleOrigin::Filesystem("local.dsrv".into()),
            },
            ActivatedModule {
                path: "std::option".into(),
                origin: ModuleOrigin::Embedded,
            },
        ];
        let report = render_checked_program(CheckedProgramView {
            specification: &checked,
            modules: &modules,
        });
        assert!(report.contains("local [filesystem: local.dsrv]"));
        assert!(report.contains("std::option [embedded catalogue]"));
        assert!(report.contains("out y: Int"));
        assert!(!report.contains("type Option"));
    }

    #[test]
    fn generic_aliases_are_identified_as_generic_aliases() {
        let specification =
            parse_str("use experimental::{generics}\ntype Box<T> = List<T>\nin x: Box<Int>")
                .expect("fixture expands");
        let report = render_expanded_program(ExpandedProgramView {
            specification: &specification,
            modules: &[],
        });
        assert!(report.contains("type Box<T> = <generic alias; expanded when instantiated>"));
    }
}
