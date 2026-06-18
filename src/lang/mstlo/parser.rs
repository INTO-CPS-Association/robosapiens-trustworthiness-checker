use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use mstlo::parse_stl;
use winnow::{
    Parser, Result as WinnowResult,
    ascii::line_ending,
    combinator::{opt, terminated},
    token::{rest, take_till},
};

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
    let parsed_file = property_file.parse(input).map_err(|err| {
        anyhow!("MSTLO property file could not be parsed as named properties: {err}")
    })?;

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

fn property_file<'a>(input: &mut &'a str) -> WinnowResult<ParsedFile<'a>> {
    let lines: Vec<&'a str> = winnow::combinator::repeat(0.., physical_line).parse_next(input)?;

    let mut properties = Vec::new();
    let mut current_property: Option<ParsedProperty<'a>> = None;
    let mut invalid_line = None;

    for (idx, raw_line) in lines.into_iter().enumerate() {
        let line_no = idx + 1;
        let line = uncommented_line.parse(raw_line).unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }

        let mut property_line = line;
        if let Ok((name, formula)) = property_header.parse_next(&mut property_line) {
            if let Some(property) = current_property.take() {
                properties.push(property);
            }
            current_property = Some(ParsedProperty {
                line_no,
                name,
                formula: formula.to_owned(),
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

    Ok(ParsedFile {
        properties,
        invalid_line,
    })
}

fn physical_line<'a>(input: &mut &'a str) -> WinnowResult<&'a str> {
    let line = take_till(0.., ['\r', '\n']).parse_next(input)?;
    if line.is_empty() {
        line_ending.parse_next(input)?;
    } else {
        opt(line_ending).parse_next(input)?;
    }

    Ok(line)
}

fn uncommented_line<'a>(input: &mut &'a str) -> WinnowResult<&'a str> {
    terminated(take_till(0.., ['#']), opt(('#', rest))).parse_next(input)
}

fn property_header<'a>(input: &mut &'a str) -> WinnowResult<(&'a str, &'a str)> {
    (take_till(0.., [':']), ':', rest)
        .map(|(name, _, formula): (&str, char, &str)| (name.trim(), formula.trim()))
        .parse_next(input)
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
