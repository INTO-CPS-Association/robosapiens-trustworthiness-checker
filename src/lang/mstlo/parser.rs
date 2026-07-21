use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use mstlo::parse_stl;

use super::MstloSpecification;
use crate::VarName;

struct ParsedFile<'a> {
    properties: Vec<ParsedProperty<'a>>,
    invalid_line: Option<usize>,
}

struct ParsedProperty<'a> {
    line_no: usize,
    name: &'a str,
    formula: String,
}

/// Parses an MSTLO property file.
///
/// Each non-empty line must define one named STL property using:
///
/// ```text
/// property_name: stl formula
/// ```
///
/// Lines may contain `#` comments. The STL formula part is parsed by
/// [`mstlo::parse_stl`], so it accepts the same runtime syntax as MSTLO itself,
/// e.g. `x > 5`, `G[0, 10](x > $threshold)`, and `x > 5 && y < 3`.
pub fn parse_named_properties(input: &str) -> anyhow::Result<MstloSpecification> {
    let parsed_file = property_file(input);

    if let Some(line_no) = parsed_file.invalid_line {
        return Err(anyhow!(
            "MSTLO property line {line_no} must have format `name: formula`"
        ));
    }

    let mut formulae = BTreeMap::new();
    for ParsedProperty {
        line_no,
        name,
        formula,
    } in parsed_file.properties
    {
        if name.is_empty() {
            return Err(anyhow!(
                "MSTLO property line {line_no} has an empty property name"
            ));
        }

        let formula = parse_stl(&formula)
            .map_err(|err| anyhow!(err.to_string()))
            .with_context(|| {
                format!("Failed to parse MSTLO property `{name}` on line {line_no}")
            })?;
        if formulae.insert(VarName::new(name), formula).is_some() {
            return Err(anyhow!(
                "Duplicate MSTLO property name `{name}` on line {line_no}"
            ));
        }
    }

    if formulae.is_empty() {
        return Err(anyhow!(
            "MSTLO property file did not contain any properties"
        ));
    }
    Ok(MstloSpecification::new(formulae))
}

fn property_file(input: &str) -> ParsedFile<'_> {
    let mut properties = Vec::new();
    let mut current_property: Option<ParsedProperty<'_>> = None;
    let mut invalid_line = None;

    for (idx, raw_line) in input.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line
            .split_once('#')
            .map_or(raw_line, |(line, _)| line)
            .trim();
        if line.is_empty() {
            continue;
        }

        if let Some((name, formula)) = line.split_once(':') {
            if let Some(property) = current_property.take() {
                properties.push(property);
            }
            current_property = Some(ParsedProperty {
                line_no,
                name: name.trim(),
                formula: formula.trim().to_owned(),
            });
        } else if let Some(property) = current_property.as_mut() {
            if !property.formula.is_empty() {
                property.formula.push('\n');
            }
            property.formula.push_str(line);
        } else {
            invalid_line = Some(line_no);
            break;
        }
    }

    if let Some(property) = current_property {
        properties.push(property);
    }

    ParsedFile {
        properties,
        invalid_line,
    }
}

pub async fn parse_file(path: &str) -> anyhow::Result<MstloSpecification> {
    let input = smol::fs::read_to_string(path)
        .await
        .with_context(|| format!("MSTLO property file `{path}` could not be read"))?;
    parse_named_properties(&input)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::Specification;

    use super::*;

    #[test]
    fn parses_named_properties_with_mstlo_parser() {
        let formula = parse_named_properties(
            r#"
            gt: x > 5
            temporal: G[0, 10](x > $threshold) && F[0, 5](y < 3)
            "#,
        )
        .unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("gt"), VarName::new("temporal")])
        );
        assert_eq!(
            formula.input_vars(),
            BTreeSet::from([VarName::new("x"), VarName::new("y")])
        );
    }

    #[test]
    fn ignores_comments_and_blank_lines() {
        let formula = parse_named_properties(
            r#"
            # comment
            gt: x > 5 # trailing comment

            lt: y < 3
            "#,
        )
        .unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("gt"), VarName::new("lt")])
        );
    }

    #[test]
    fn parses_crlf_property_files() {
        let formula =
            parse_named_properties("# comment\r\ngt: x > 5\r\n\r\nlt: y < 3\r\n").unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("gt"), VarName::new("lt")])
        );
    }

    #[test]
    fn property_wrapper_preserves_continuations_and_strips_comments() {
        let parsed = property_file(
            "first: x > 5 # ignored: text\n  && y < 3 # trailing comment\nsecond: z > 0",
        );

        assert_eq!(parsed.invalid_line, None);
        assert_eq!(parsed.properties.len(), 2);
        assert_eq!(parsed.properties[0].line_no, 1);
        assert_eq!(parsed.properties[0].name, "first");
        assert_eq!(parsed.properties[0].formula, "x > 5\n&& y < 3");
        assert_eq!(parsed.properties[1].name, "second");
        assert_eq!(parsed.properties[1].formula, "z > 0");
    }

    #[test]
    fn property_wrapper_splits_headers_at_first_colon() {
        let parsed = property_file("property: formula:with:colons");

        assert_eq!(parsed.invalid_line, None);
        assert_eq!(parsed.properties.len(), 1);
        assert_eq!(parsed.properties[0].name, "property");
        assert_eq!(parsed.properties[0].formula, "formula:with:colons");
    }

    #[test]
    fn rejects_lines_without_property_separator() {
        let err = parse_named_properties("gt x > 5").unwrap_err();

        assert!(
            err.to_string()
                .contains("MSTLO property line 1 must have format `name: formula`")
        );
    }

    #[test]
    fn parses_formula_after_linebreak_after_separator() {
        let formula = parse_named_properties(
            r#"
            temporal:
                G[0, 10](
                    x > $threshold
                )
            "#,
        )
        .unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("temporal")])
        );
        assert_eq!(formula.input_vars(), BTreeSet::from([VarName::new("x")]));
    }

    #[test]
    fn parses_multiple_multiline_properties() {
        let formula = parse_named_properties(
            r#"
            first:
                G[0, 10](
                    x > 5
                )
            second:
                F[0, 5](
                    y < 3
                )
            "#,
        )
        .unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("first"), VarName::new("second")])
        );
        assert_eq!(
            formula.input_vars(),
            BTreeSet::from([VarName::new("x"), VarName::new("y")])
        );
    }
}
