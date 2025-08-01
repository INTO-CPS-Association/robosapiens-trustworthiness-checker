use std::fmt::Debug;

use anyhow::{self};

// use tokio::{fs::File, io::AsyncReadExt};
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
    debug!(name: "Parsing file",
        contents=?parser.parse_next(&mut contents.as_str()).unwrap());
    parser.parse(contents.as_str()).map_err(|e| {
        anyhow::anyhow!(e.to_string()).context(format!("Failed to parse file {}", file))
    })
}

#[cfg(test)]
mod tests {

    use crate::{InputProvider, Value};

    use super::*;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use test_log::test;

    #[test(apply(smol_test))]
    async fn test_parse_file() {
        let parser = crate::lang::untimed_input::untimed_input_file;
        let file = "fixtures/simple_add.input";
        let mut data = parse_file(parser, file).await.unwrap();
        let x_vals = data
            .input_stream(&"x".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(x_vals, vec![Value::Int(1), Value::Int(3)]);
    }

    #[test(apply(smol_test))]
    async fn test_parse_boolean_file() {
        let parser = crate::lang::untimed_input::untimed_input_file;
        let file = "fixtures/maple_sequence_true.input";
        let mut data = parse_file(parser, file).await.unwrap();
        let m_vals = data
            .input_stream(&"m".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
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
        let a_vals = data
            .input_stream(&"a".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
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
        let p_vals = data
            .input_stream(&"p".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
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
        let l_vals = data
            .input_stream(&"l".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
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
        let e_vals = data
            .input_stream(&"e".into())
            .unwrap()
            .collect::<Vec<_>>()
            .await;
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
