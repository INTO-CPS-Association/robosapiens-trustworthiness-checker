//! DSRV declarations translated mechanically from an application's
//! observation schema.
//!
//! The application authors `observation_schema.json` once:
//!
//! ```json
//! {"schema_version": 4,
//!  "knowledge": {"FrameRef": {"frame_path": "Option<Str>", "brightness": "Option<Float>"}},
//!  "message": {"reason": "Option<Str>"},
//!  "trigger": {"detection_type": "Option<Str>"}}
//! ```
//!
//! Each field type is DSRV type syntax, pasted verbatim. This module emits one
//! open struct per Knowledge key, the `Knowledge` union of them, the open
//! `Message` (sent) and `Trigger` (received) structs, and one accessor per key
//! that selects a cited `Resolved` tuple of that key. Nothing else: the
//! journal's own vocabulary is `rpio::journal`, and every application check is
//! the specification's.

use std::collections::BTreeMap;

use anyhow::Context as _;

/// The only schema version this generator reads.
pub const SCHEMA_VERSION: u32 = 4;

/// Field name to DSRV type, in the schema's (sorted) order.
pub type Fields = BTreeMap<String, String>;

/// An application's observation schema.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationSchema {
    pub schema_version: u32,
    pub knowledge: BTreeMap<String, Fields>,
    #[serde(default)]
    pub message: Fields,
    #[serde(default)]
    pub trigger: Fields,
}

/// Names the generated module defines itself, which a key may not take.
const RESERVED: [&str; 3] = ["Knowledge", "Message", "Trigger"];

impl ObservationSchema {
    pub fn read(path: &str) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("observation schema `{path}` could not be read"))?;
        Self::parse(&text).with_context(|| format!("observation schema `{path}`"))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let schema: Self = serde_json::from_str(text)?;
        schema.validate()?;
        Ok(schema)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema_version == SCHEMA_VERSION,
            "schema_version is {}, and only version {SCHEMA_VERSION} is read",
            self.schema_version
        );
        for (key, fields) in &self.knowledge {
            anyhow::ensure!(
                identifier(key) && key.starts_with(|c: char| c.is_ascii_uppercase()),
                "Knowledge key `{key}` is not a type name"
            );
            anyhow::ensure!(
                !RESERVED.contains(&key.as_str()),
                "Knowledge key `{key}` would hide the generated type of that name"
            );
            check_fields(&format!("Knowledge key `{key}`"), fields)?;
        }
        check_fields("message", &self.message)?;
        check_fields("trigger", &self.trigger)
    }
}

fn check_fields(owner: &str, fields: &Fields) -> anyhow::Result<()> {
    for (name, ty) in fields {
        anyhow::ensure!(identifier(name), "{owner} field `{name}` is not a name");
        anyhow::ensure!(
            !ty.trim().is_empty() && !ty.contains(['\n', '\r']),
            "{owner} field `{name}` has no type on one line"
        );
    }
    Ok(())
}

fn identifier(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// An open struct of `fields`.
fn open_struct(fields: &Fields) -> String {
    let mut members: Vec<String> = fields
        .iter()
        .map(|(name, ty)| format!("{name}: {}", ty.trim()))
        .collect();
    members.push("...".into());
    format!("Struct<{}>", members.join(", "))
}

/// `FrameRef` -> `frame_ref`.
fn snake(name: &str) -> String {
    let mut out = String::new();
    for (index, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The DSRV module text for `schema`, recording `source` as its origin.
pub fn generate(schema: &ObservationSchema, source: &str) -> String {
    let mut out = String::new();
    let mut line = |text: &str| {
        out.push_str(text);
        out.push('\n');
    };
    line(&format!(
        "// GENERATED from {source} by generate_rpio_declarations; do not edit."
    ));
    line("use experimental::high_level_dsrv");
    line("use rpio::journal::{Resolved}");
    line("use std::option::Option");
    line("");
    line("// Knowledge values, one type per key.");
    for (key, fields) in &schema.knowledge {
        line(&format!("type {key} = {}", open_struct(fields)));
    }
    let arms: Vec<String> = schema
        .knowledge
        .keys()
        .map(|key| format!("{key}: {key}"))
        .collect();
    line(&format!("type Knowledge = Union<{}>", arms.join(", ")));
    line("");
    line("// The messages callbacks send (Event) and the messages that start them (Start).");
    line(&format!("type Message = {}", open_struct(&schema.message)));
    line(&format!("type Trigger = {}", open_struct(&schema.trigger)));
    line("");
    line("// The value of a cited tuple, when it is of that key.");
    for key in schema.knowledge.keys() {
        line(&format!(
            "def {}(r: Option<Resolved<Knowledge>>) -> Option<{key}> =\n  \
             match(r) {{ Some({{ value: {key}(v), .. }}) -> Some(v), _ -> None }}",
            snake(key)
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCREW: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/screw-detection/observation_schema.json"
    );

    #[test]
    fn the_screw_detection_schema_generates_its_checked_in_declarations() {
        let schema = ObservationSchema::read(SCREW).unwrap();
        let text = generate(&schema, "observation_schema.json");
        let checked_in = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/examples/screw-detection/declarations.dsrv"
        ))
        .unwrap();
        assert_eq!(text, checked_in);
        assert!(
            text.contains("def frame_ref(r: Option<Resolved<Knowledge>>) -> Option<FrameRef> =")
        );
        assert!(text.contains("type Trigger = Struct<attempt_id: Option<Str>,"));
    }

    #[test]
    fn a_schema_is_refused_unless_it_names_types_and_fields() {
        for (text, problem) in [
            (r#"{"schema_version":3,"knowledge":{}}"#, "schema_version"),
            (
                r#"{"schema_version":4,"knowledge":{"frame":{}}}"#,
                "type name",
            ),
            (r#"{"schema_version":4,"knowledge":{"Message":{}}}"#, "hide"),
            (
                r#"{"schema_version":4,"knowledge":{"A":{"x y":"Int"}}}"#,
                "not a name",
            ),
            (
                r#"{"schema_version":4,"knowledge":{"A":{"x":" "}}}"#,
                "no type",
            ),
            (
                r#"{"schema_version":4,"knowledge":{},"extra":1}"#,
                "unknown field",
            ),
        ] {
            let error = format!("{:#}", ObservationSchema::parse(text).unwrap_err());
            assert!(error.contains(problem), "{text}: {error}");
        }
    }

    #[test]
    fn accessors_are_snake_case() {
        assert_eq!(snake("FrameRef"), "frame_ref");
        assert_eq!(snake("LegitResult"), "legit_result");
        assert_eq!(snake("A"), "a");
    }
}
