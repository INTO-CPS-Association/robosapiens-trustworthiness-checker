use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use trustworthiness_checker::core::StreamType;
use trustworthiness_checker::lang::dsrv::parser::parse_str;
use trustworthiness_checker::lang::dsrv::type_checker::type_check;

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

#[derive(Default, Deserialize)]
struct FmiAnnotations {
    #[serde(default)]
    model: ModelAnnotations,
    #[serde(default)]
    variables: BTreeMap<String, VariableAnnotations>,
}

#[derive(Default, Deserialize)]
struct ModelAnnotations {
    name: Option<String>,
    guid: Option<String>,
    description: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
    let parser_input = spec_source.as_str();
    let untyped = parse_str(parser_input)
        .map_err(|error| anyhow!("failed to parse {}: {error:?}", args.spec.display()))?;
    let typed = type_check(untyped, false)
        .map_err(|error| anyhow!("failed to type-check {}: {error:?}", args.spec.display()))?;

    let annotations = match args.annotations.as_deref() {
        Some(path) => toml::from_str(
            &fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", path.display()))?,
        None => FmiAnnotations::default(),
    };
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
