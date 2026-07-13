use std::fmt::Debug;

use anyhow::{self};

use tracing::debug;
use winnow::{Parser, error::ContextError};

pub async fn parse_file<O: Clone + Debug>(
    // The for<'a> syntax is a higher-ranked trait bound which is
    // necessary to specify that the lifetime of the string passed
    // into the parser does not need to outlive this function call
    // (i.e. it needs to admit arbitrarily short lifetimes)
    // see: https://doc.rust-lang.org/nomicon/hrtb.html
    mut parser: impl for<'a> Parser<&'a str, O, ContextError>,
    file: &str,
) -> anyhow::Result<O> {
    let contents = smol::fs::read_to_string(file).await?;
    let result = parser.parse(contents.as_str()).map_err(|e| {
        anyhow::anyhow!(e.to_string()).context(format!("Failed to parse file {}", file))
    });
    debug!(result=?result, "Parsed file with result");
    result
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use crate::{Value, io::file};

    use super::*;
    use crate::async_test;
    use futures::StreamExt;
    use macro_rules_attribute::apply;

    async fn values(data: crate::io::file::UntimedInputFileData, var: &str) -> Vec<Value> {
        let input = file::input_stream(data, std::collections::BTreeSet::from([var.into()]));
        crate::into_tick_stream(input)
            .map(Result::unwrap)
            .flat_map(futures::stream::iter)
            .map(|event| event.value)
            .collect()
            .await
    }

    #[apply(async_test)]
    async fn test_parse_file() {
        let parser = crate::lang::untimed_input::untimed_input_file;
        let file = "fixtures/simple_add.input";
        let input = file::input_stream(
            parse_file(parser, file).await.unwrap(),
            std::collections::BTreeSet::from(["x".into()]),
        );
        let x_vals = crate::into_tick_stream(input)
            .map(|tick| tick.unwrap().into_iter().next().unwrap().value)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(x_vals, vec![Value::Int(1), Value::Int(3)]);
    }

    #[apply(async_test)]
    async fn test_parse_json_object_literal_file() {
        let parser = crate::lang::untimed_input::untimed_input_file;
        let file = "fixtures/object_literal.input";
        let input = file::input_stream(
            parse_file(parser, file).await.unwrap(),
            std::collections::BTreeSet::from(["payload".into()]),
        );
        let payload_vals = crate::into_tick_stream(input)
            .map(|tick| tick.unwrap().into_iter().next().unwrap().value)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            payload_vals,
            vec![
                Value::Map(BTreeMap::from([
                    ("x".into(), Value::Int(10)),
                    ("y".into(), Value::Int(20)),
                ])),
                Value::Map(BTreeMap::from([
                    ("x".into(), Value::Int(30)),
                    ("y".into(), Value::Int(40)),
                ])),
            ]
        );
    }

    #[apply(async_test)]
    async fn test_parse_boolean_file() {
        let parser = crate::lang::untimed_input::untimed_input_file;
        let file = "fixtures/maple_sequence_true.input";
        let data = parse_file(parser, file).await.unwrap();
        let m_vals = values(data.clone(), "m").await;
        assert_eq!(
            m_vals,
            vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false)
            ],
        );
        let a_vals = values(data.clone(), "a").await;
        assert_eq!(
            a_vals,
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false)
            ],
        );
        let p_vals = values(data.clone(), "p").await;
        assert_eq!(
            p_vals,
            vec![
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false)
            ],
        );
        let l_vals = values(data.clone(), "l").await;
        assert_eq!(
            l_vals,
            vec![
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false)
            ],
        );
        let e_vals = values(data, "e").await;
        assert_eq!(
            e_vals,
            vec![
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true)
            ],
        );
    }
}
