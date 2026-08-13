use serde::Serialize;

/// Serialize as strict JSON unless the value requires JSON5 extensions.
pub(crate) fn encode_json_or_json5<T: Serialize>(
    value: &T,
    requires_json5: bool,
) -> anyhow::Result<String> {
    if requires_json5 {
        json5::to_string(value)
            .map(|json| compact_json5(&json))
            .map_err(|error| anyhow::anyhow!("failed to encode value as JSON5: {error}"))
    } else {
        serde_json::to_string(value)
            .map_err(|error| anyhow::anyhow!("failed to encode value as JSON: {error}"))
    }
}

fn compact_json5(json: &str) -> String {
    let mut compact = String::with_capacity(json.len());
    let mut quote = None;
    let mut escaped = false;

    for character in json.chars() {
        if let Some(delimiter) = quote {
            compact.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
        } else if matches!(character, '"' | '\'') {
            quote = Some(character);
            compact.push(character);
        } else if !character.is_whitespace() {
            compact.push(character);
        }
    }

    compact
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::encode_json_or_json5;

    #[derive(Serialize)]
    struct Sample<'a> {
        label: &'a str,
        value: f64,
    }

    #[test]
    fn json5_encoding_is_compact_and_preserves_string_whitespace() {
        let encoded = encode_json_or_json5(
            &Sample {
                label: r#"space, quote: " and slash: \"#,
                value: f64::INFINITY,
            },
            true,
        )
        .unwrap();

        assert!(!encoded.contains('\n'));
        assert!(!encoded.contains("label: "));
        assert!(encoded.contains("value:Infinity"));
        let decoded = json5::from_str::<serde_json::Value>(&encoded).unwrap();
        assert_eq!(decoded["label"], r#"space, quote: " and slash: \"#);
    }
}
