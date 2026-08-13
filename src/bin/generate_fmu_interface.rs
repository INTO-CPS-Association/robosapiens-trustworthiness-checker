use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use trustworthiness_checker::core::StreamType;
use trustworthiness_checker::{DsrvSpecification, TypeCheckOptions};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    spec: PathBuf,
    #[arg(long)]
    annotations: Option<PathBuf>,
    #[arg(long)]
    model_description: PathBuf,
    #[arg(long)]
    interface: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FmiAnnotations {
    #[serde(default)]
    model: ModelAnnotations,
    #[serde(default)]
    variables: BTreeMap<String, VariableAnnotations>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelAnnotations {
    name: Option<String>,
    guid: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct VariableAnnotations {
    value_reference: Option<u32>,
    start: Option<toml::Value>,
    unit: Option<String>,
    description: Option<String>,
    variability: Option<String>,
}

#[derive(Serialize)]
struct Interface {
    model_name: String,
    variables: Vec<InterfaceVariable>,
}

#[derive(Serialize)]
struct InterfaceVariable {
    name: String,
    value_reference: u32,
    fmi_type: &'static str,
    causality: &'static str,
    start: JsonValue,
}

struct Variable {
    interface: InterfaceVariable,
    annotation: VariableAnnotations,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let spec_source = fs::read_to_string(&args.spec)
        .with_context(|| format!("failed to read {}", args.spec.display()))?;
    let untyped = spec_source
        .parse::<DsrvSpecification>()
        .map_err(|error| anyhow!("failed to parse {}: {error:?}", args.spec.display()))?;
    let typed = untyped
        .type_check(TypeCheckOptions::STRICT)
        .map_err(|error| anyhow!("failed to type-check {}: {error:?}", args.spec.display()))?;

    let mut annotations = match args.annotations.as_deref() {
        Some(path) => toml::from_str(
            &fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", path.display()))?,
        None => FmiAnnotations::default(),
    };
    merge_inline_annotations(
        &mut annotations,
        parse_inline_annotations(&spec_source).with_context(|| {
            format!("failed to parse FMI annotations in {}", args.spec.display())
        })?,
    )?;
    let model_name = annotations.model.name.clone().unwrap_or_else(|| {
        args.spec
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("trustworthiness_checker")
            .replace('-', "_")
    });
    let guid = annotations
        .model
        .guid
        .clone()
        .unwrap_or_else(|| format!("{model_name}-generated-interface"));

    let inputs = typed.input_vars();
    let outputs = typed.output_vars();
    let known = inputs
        .union(outputs)
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let unknown_annotations = annotations
        .variables
        .keys()
        .filter(|name| !known.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_annotations.is_empty() {
        bail!("FMI annotations reference unknown variables: {unknown_annotations:?}");
    }

    let mut used_references = BTreeSet::new();
    for annotation in annotations.variables.values() {
        if let Some(reference) = annotation.value_reference
            && !used_references.insert(reference)
        {
            bail!("duplicate FMI value reference {reference}");
        }
    }
    let mut next_reference = 0;
    let mut annotations_by_name = annotations.variables;
    let mut variables = Vec::new();
    for (names, causality) in [(inputs, "input"), (outputs, "output")] {
        for name in names {
            let stream_type = typed
                .type_annotation(name)
                .ok_or_else(|| anyhow!("missing type for variable {name}"))?;
            let fmi_type = fmi_type(stream_type)?;
            let annotation = annotations_by_name
                .remove(&name.to_string())
                .unwrap_or_default();
            let value_reference = match annotation.value_reference {
                Some(reference) => reference,
                None => {
                    while used_references.contains(&next_reference) {
                        next_reference += 1;
                    }
                    let reference = next_reference;
                    used_references.insert(reference);
                    next_reference += 1;
                    reference
                }
            };
            let start = match annotation.start.as_ref() {
                Some(value) => parse_start(fmi_type, value)
                    .with_context(|| format!("invalid start value for {name}"))?,
                None => default_start(fmi_type),
            };
            variables.push(Variable {
                interface: InterfaceVariable {
                    name: name.to_string(),
                    value_reference,
                    fmi_type,
                    causality,
                    start,
                },
                annotation,
            });
        }
    }

    let interface = Interface {
        model_name: model_name.clone(),
        variables: variables
            .iter()
            .map(|variable| InterfaceVariable {
                name: variable.interface.name.clone(),
                value_reference: variable.interface.value_reference,
                fmi_type: variable.interface.fmi_type,
                causality: variable.interface.causality,
                start: variable.interface.start.clone(),
            })
            .collect(),
    };
    write_file(&args.interface, &serde_json::to_string_pretty(&interface)?)?;
    write_file(
        &args.model_description,
        &render_model_description(
            &model_name,
            &guid,
            annotations.model.description.as_deref(),
            &variables,
        ),
    )?;
    Ok(())
}

fn parse_inline_annotations(source: &str) -> Result<BTreeMap<String, VariableAnnotations>> {
    let mut variables = BTreeMap::new();
    let mut pending: Option<(usize, VariableAnnotations)> = None;

    for (line_index, raw_line) in source.lines().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.trim();
        if let Some(annotation) = line.strip_prefix("// @fmi").filter(|annotation| {
            annotation.is_empty() || annotation.starts_with(char::is_whitespace)
        }) {
            let (_, metadata) =
                pending.get_or_insert_with(|| (line_number, VariableAnnotations::default()));
            parse_inline_annotation_fields(annotation.trim(), metadata, line_number)?;
            continue;
        }

        if let Some((annotation_line, metadata)) = pending.take() {
            let Some(name) = declaration_name(line) else {
                bail!(
                    "FMI annotation starting on line {annotation_line} must be followed immediately by an input or output declaration"
                );
            };
            if variables.insert(name.to_owned(), metadata).is_some() {
                bail!("multiple FMI annotation groups target variable {name}");
            }
        }
    }

    if let Some((annotation_line, _)) = pending {
        bail!(
            "FMI annotation starting on line {annotation_line} is not followed by an input or output declaration"
        );
    }

    Ok(variables)
}

fn declaration_name(line: &str) -> Option<&str> {
    let declaration = ["in", "out"].into_iter().find_map(|keyword| {
        line.strip_prefix(keyword)
            .filter(|rest| rest.starts_with(char::is_whitespace))
    })?;
    let (name, _) = declaration.trim_start().split_once(':')?;
    let name = name.trim();
    let mut characters = name.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    Some(name)
}

fn parse_inline_annotation_fields(
    annotation: &str,
    metadata: &mut VariableAnnotations,
    line_number: usize,
) -> Result<()> {
    let fields = tokenize_annotation_fields(annotation)
        .with_context(|| format!("invalid @fmi annotation on line {line_number}"))?;
    if fields.is_empty() {
        bail!("@fmi annotation on line {line_number} does not define any fields");
    }

    for (key, raw_value) in fields {
        match key.as_str() {
            "value-reference" => set_inline_field(
                &mut metadata.value_reference,
                raw_value.parse::<u32>().with_context(|| {
                    format!("invalid value-reference in @fmi annotation on line {line_number}")
                })?,
                &key,
                line_number,
            )?,
            "start" => set_inline_field(
                &mut metadata.start,
                parse_toml_value(&raw_value).with_context(|| {
                    format!("invalid start value in @fmi annotation on line {line_number}")
                })?,
                &key,
                line_number,
            )?,
            "unit" => set_inline_field(
                &mut metadata.unit,
                parse_annotation_string(&raw_value).with_context(|| {
                    format!("invalid unit in @fmi annotation on line {line_number}")
                })?,
                &key,
                line_number,
            )?,
            "description" => set_inline_field(
                &mut metadata.description,
                parse_annotation_string(&raw_value).with_context(|| {
                    format!("invalid description in @fmi annotation on line {line_number}")
                })?,
                &key,
                line_number,
            )?,
            "variability" => set_inline_field(
                &mut metadata.variability,
                parse_annotation_string(&raw_value).with_context(|| {
                    format!("invalid variability in @fmi annotation on line {line_number}")
                })?,
                &key,
                line_number,
            )?,
            unknown => bail!("unknown @fmi field '{unknown}' on line {line_number}"),
        }
    }
    Ok(())
}

fn tokenize_annotation_fields(mut annotation: &str) -> Result<Vec<(String, String)>> {
    let mut fields = Vec::new();
    while !annotation.trim_start().is_empty() {
        annotation = annotation.trim_start();
        let key_length = annotation
            .chars()
            .take_while(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
            .map(char::len_utf8)
            .sum::<usize>();
        if key_length == 0 {
            bail!("expected a field name near '{annotation}'");
        }
        let key = &annotation[..key_length];
        annotation = annotation[key_length..].trim_start();
        annotation = annotation
            .strip_prefix('=')
            .ok_or_else(|| anyhow!("expected '=' after @fmi field '{key}'"))?
            .trim_start();
        if annotation.is_empty() {
            bail!("missing value for @fmi field '{key}'");
        }

        let value_length = if annotation.starts_with('"') {
            quoted_value_length(annotation)?
        } else {
            annotation
                .find(char::is_whitespace)
                .unwrap_or(annotation.len())
        };
        let value = &annotation[..value_length];
        annotation = &annotation[value_length..];
        if !annotation.is_empty() && !annotation.starts_with(char::is_whitespace) {
            bail!("expected whitespace after value for @fmi field '{key}'");
        }
        fields.push((key.to_owned(), value.to_owned()));
    }
    Ok(fields)
}

fn quoted_value_length(value: &str) -> Result<usize> {
    let mut escaped = false;
    for (offset, character) in value[1..].char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            return Ok(offset + 2);
        }
    }
    bail!("unterminated quoted value")
}

fn parse_toml_value(raw_value: &str) -> Result<toml::Value> {
    let mut table = toml::from_str::<toml::Table>(&format!("value = {raw_value}"))?;
    table
        .remove("value")
        .ok_or_else(|| anyhow!("annotation value did not produce a TOML value"))
}

fn parse_annotation_string(raw_value: &str) -> Result<String> {
    if raw_value.starts_with('"') {
        return parse_toml_value(raw_value)?
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("expected a string value"));
    }
    Ok(raw_value.to_owned())
}

fn set_inline_field<T>(
    target: &mut Option<T>,
    value: T,
    field: &str,
    line_number: usize,
) -> Result<()> {
    if target.replace(value).is_some() {
        bail!("duplicate @fmi field '{field}' on line {line_number}");
    }
    Ok(())
}

fn merge_inline_annotations(
    annotations: &mut FmiAnnotations,
    inline_variables: BTreeMap<String, VariableAnnotations>,
) -> Result<()> {
    for (name, inline) in inline_variables {
        let sidecar = annotations.variables.entry(name.clone()).or_default();
        merge_annotation_field(
            &name,
            "value-reference",
            &mut sidecar.value_reference,
            inline.value_reference,
        )?;
        merge_annotation_field(&name, "start", &mut sidecar.start, inline.start)?;
        merge_annotation_field(&name, "unit", &mut sidecar.unit, inline.unit)?;
        merge_annotation_field(
            &name,
            "description",
            &mut sidecar.description,
            inline.description,
        )?;
        merge_annotation_field(
            &name,
            "variability",
            &mut sidecar.variability,
            inline.variability,
        )?;
    }
    Ok(())
}

fn merge_annotation_field<T: PartialEq + std::fmt::Debug>(
    variable: &str,
    field: &str,
    sidecar: &mut Option<T>,
    inline: Option<T>,
) -> Result<()> {
    match (sidecar.as_ref(), inline) {
        (Some(sidecar_value), Some(inline_value)) if sidecar_value != &inline_value => bail!(
            "conflicting FMI {field} for variable {variable}: fmi.toml has {sidecar_value:?}, spec.dsrv has {inline_value:?}"
        ),
        (None, Some(inline_value)) => *sidecar = Some(inline_value),
        _ => {}
    }
    Ok(())
}

fn fmi_type(stream_type: &StreamType) -> Result<&'static str> {
    match stream_type {
        StreamType::Int => Ok("Integer"),
        StreamType::Float => Ok("Real"),
        StreamType::Bool => Ok("Boolean"),
        StreamType::Str => Ok("String"),
        unsupported => bail!("DSRV type {unsupported} cannot be exposed through FMI 2.0"),
    }
}

fn default_start(fmi_type: &str) -> JsonValue {
    match fmi_type {
        "Integer" => 0.into(),
        "Real" => 0.0.into(),
        "Boolean" => false.into(),
        "String" => "".into(),
        _ => unreachable!(),
    }
}

fn parse_start(fmi_type: &str, value: &toml::Value) -> Result<JsonValue> {
    match (fmi_type, value) {
        ("Integer", toml::Value::Integer(value)) => Ok((*value).into()),
        ("Real", toml::Value::Float(value)) => Ok((*value).into()),
        ("Real", toml::Value::Integer(value)) => Ok((*value as f64).into()),
        ("Boolean", toml::Value::Boolean(value)) => Ok((*value).into()),
        ("String", toml::Value::String(value)) => Ok(value.clone().into()),
        _ => bail!("expected a {fmi_type} value, got {value}"),
    }
}

fn render_model_description(
    model_name: &str,
    guid: &str,
    description: Option<&str>,
    variables: &[Variable],
) -> String {
    let description = description
        .map(|value| format!("\n    description=\"{}\"", xml_escape(value)))
        .unwrap_or_default();
    let units = variables
        .iter()
        .filter_map(|variable| variable.annotation.unit.as_deref())
        .collect::<BTreeSet<_>>();
    let unit_definitions = if units.is_empty() {
        String::new()
    } else {
        let definitions = units
            .into_iter()
            .map(|unit| format!("        <Unit name=\"{}\" />", xml_escape(unit)))
            .collect::<Vec<_>>()
            .join("\n");
        format!("    <UnitDefinitions>\n{definitions}\n    </UnitDefinitions>\n")
    };
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<fmiModelDescription fmiVersion=\"2.0\" modelName=\"{}\" guid=\"{}\"{}\n\
    generationTool=\"RoboSAPIENS Trustworthiness Checker\" variableNamingConvention=\"flat\">\n\
    <CoSimulation modelIdentifier=\"unifmu\" needsExecutionTool=\"true\"\n\
        canHandleVariableCommunicationStepSize=\"true\" canInterpolateInputs=\"false\"\n\
        canRunAsynchronuously=\"false\" canBeInstantiatedOnlyOncePerProcess=\"false\"\n\
        canNotUseMemoryManagementFunctions=\"true\" canGetAndSetFMUstate=\"false\"\n\
        canSerializeFMUstate=\"false\" />\n{unit_definitions}    <ModelVariables>\n",
        xml_escape(model_name),
        xml_escape(guid),
        description,
    );
    for variable in variables {
        let initial = if variable.interface.causality == "output" {
            " initial=\"calculated\""
        } else {
            ""
        };
        let variability = variable
            .annotation
            .variability
            .as_deref()
            .unwrap_or("discrete");
        let description = variable
            .annotation
            .description
            .as_deref()
            .map(|value| format!(" description=\"{}\"", xml_escape(value)))
            .unwrap_or_default();
        xml.push_str(&format!(
            "        <ScalarVariable name=\"{}\" valueReference=\"{}\" causality=\"{}\" variability=\"{}\"{}{}>\n",
            xml_escape(&variable.interface.name),
            variable.interface.value_reference,
            variable.interface.causality,
            xml_escape(variability),
            initial,
            description,
        ));
        let unit = variable
            .annotation
            .unit
            .as_deref()
            .filter(|_| variable.interface.fmi_type == "Real")
            .map(|value| format!(" unit=\"{}\"", xml_escape(value)))
            .unwrap_or_default();
        if variable.interface.causality == "input" {
            xml.push_str(&format!(
                "            <{} start=\"{}\"{} />\n",
                variable.interface.fmi_type,
                xml_escape(&json_scalar(&variable.interface.start)),
                unit,
            ));
        } else {
            xml.push_str(&format!(
                "            <{}{} />\n",
                variable.interface.fmi_type, unit,
            ));
        }
        xml.push_str("        </ScalarVariable>\n");
    }
    xml.push_str("    </ModelVariables>\n    <ModelStructure>\n        <Outputs>\n");
    let input_indices = variables
        .iter()
        .enumerate()
        .filter(|(_, variable)| variable.interface.causality == "input")
        .map(|(index, _)| (index + 1).to_string())
        .collect::<Vec<_>>()
        .join(" ");
    for (index, variable) in variables.iter().enumerate() {
        if variable.interface.causality == "output" {
            let kinds = std::iter::repeat_n("dependent", input_indices.split_whitespace().count())
                .collect::<Vec<_>>()
                .join(" ");
            xml.push_str(&format!(
                "            <Unknown index=\"{}\" dependencies=\"{}\" dependenciesKind=\"{}\" />\n",
                index + 1,
                input_indices,
                kinds,
            ));
        }
    }
    xml.push_str("        </Outputs>\n        <InitialUnknowns>\n");
    for (index, variable) in variables.iter().enumerate() {
        if variable.interface.causality == "output" {
            xml.push_str(&format!(
                "            <Unknown index=\"{}\" />\n",
                index + 1
            ));
        }
    }
    xml.push_str("        </InitialUnknowns>\n    </ModelStructure>\n</fmiModelDescription>\n");
    xml
}

fn json_scalar(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn write_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact_and_multiline_inline_annotations() {
        let annotations = parse_inline_annotations(
            r#"
// @fmi start=1.5 unit=m/s description="Observed velocity"
in velocity: Float
// @fmi start=false
// @fmi description="Emergency stop state"
in emergency_stop: Bool
out verdict: Bool
verdict = velocity <= 5.0 || emergency_stop
"#,
        )
        .unwrap();

        let velocity = &annotations["velocity"];
        assert_eq!(velocity.start, Some(toml::Value::Float(1.5)));
        assert_eq!(velocity.unit.as_deref(), Some("m/s"));
        assert_eq!(velocity.description.as_deref(), Some("Observed velocity"));

        let emergency_stop = &annotations["emergency_stop"];
        assert_eq!(emergency_stop.start, Some(toml::Value::Boolean(false)));
        assert_eq!(
            emergency_stop.description.as_deref(),
            Some("Emergency stop state")
        );
        assert!(!annotations.contains_key("verdict"));
    }

    #[test]
    fn parses_all_supported_inline_fields() {
        let annotations = parse_inline_annotations(
            r#"
// @fmi value-reference=42 start="idle" unit=state variability=discrete
// @fmi description="State with an escaped quote: \"idle\""
in state: Str
out verdict: Bool
verdict = true
"#,
        )
        .unwrap();
        let state = &annotations["state"];

        assert_eq!(state.value_reference, Some(42));
        assert_eq!(state.start, Some(toml::Value::String("idle".to_owned())));
        assert_eq!(state.unit.as_deref(), Some("state"));
        assert_eq!(state.variability.as_deref(), Some("discrete"));
        assert_eq!(
            state.description.as_deref(),
            Some("State with an escaped quote: \"idle\"")
        );
    }

    #[test]
    fn ignores_comments_without_the_exact_fmi_marker() {
        let annotations = parse_inline_annotations(
            "// @fmish this is prose\nin velocity: Float\nout verdict: Bool\nverdict = true\n",
        )
        .unwrap();
        assert!(annotations.is_empty());
    }

    #[test]
    fn rejects_duplicate_inline_fields_across_lines() {
        let error =
            parse_inline_annotations("// @fmi start=0.0\n// @fmi start=1.0\nin velocity: Float\n")
                .unwrap_err();
        assert!(error.to_string().contains("duplicate @fmi field 'start'"));
    }

    #[test]
    fn rejects_unknown_and_unattached_inline_annotations() {
        let unknown =
            parse_inline_annotations("// @fmi unknown=value\nin velocity: Float\n").unwrap_err();
        assert!(unknown.to_string().contains("unknown @fmi field 'unknown'"));

        let unattached =
            parse_inline_annotations("// @fmi start=0.0\n\nin velocity: Float\n").unwrap_err();
        assert!(
            unattached
                .to_string()
                .contains("must be followed immediately")
        );
    }

    #[test]
    fn rejects_malformed_inline_values() {
        let error =
            parse_inline_annotations("// @fmi description=\"unterminated\nin velocity: Float\n")
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid @fmi annotation on line 1")
        );
    }

    #[test]
    fn merges_complementary_and_identical_sidecar_metadata() {
        let mut annotations: FmiAnnotations = toml::from_str(
            r#"
[variables.velocity]
start = 0.0
unit = "m/s"
"#,
        )
        .unwrap();
        let inline = parse_inline_annotations(
            "// @fmi unit=m/s description=\"Observed velocity\"\nin velocity: Float\n",
        )
        .unwrap();

        merge_inline_annotations(&mut annotations, inline).unwrap();
        let velocity = &annotations.variables["velocity"];
        assert_eq!(velocity.start, Some(toml::Value::Float(0.0)));
        assert_eq!(velocity.unit.as_deref(), Some("m/s"));
        assert_eq!(velocity.description.as_deref(), Some("Observed velocity"));
    }

    #[test]
    fn rejects_conflicting_sidecar_and_inline_metadata() {
        let mut annotations: FmiAnnotations = toml::from_str(
            r#"
[variables.velocity]
unit = "km/h"
"#,
        )
        .unwrap();
        let inline = parse_inline_annotations("// @fmi unit=m/s\nin velocity: Float\n").unwrap();

        let error = merge_inline_annotations(&mut annotations, inline).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicting FMI unit for variable velocity")
        );
    }

    #[test]
    fn rejects_unknown_sidecar_fields() {
        let error = toml::from_str::<FmiAnnotations>(
            r#"
[variables.velocity]
unknown = "value"
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}
