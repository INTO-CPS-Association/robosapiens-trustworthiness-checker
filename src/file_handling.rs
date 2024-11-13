use std::{
    error::Error,
    fmt::{Debug, Display},
};

use tokio::{fs::File, io::AsyncReadExt};
use winnow::Parser;

#[derive(Debug)]
struct FileParseError {
    error: String,
}

impl FileParseError {
    fn new(error: String) -> Self {
        Self { error }
    }
}

impl Display for FileParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Error parsing file: {}", self.error)
    }
}

impl Error for FileParseError {}

pub async fn parse_file<O: Clone + Debug, E: Debug>(
    // The for<'a> syntax is a higher-ranked trait bound which is
    // necessary to specify that the lifetime of the string passed
    // into the parser does not need to outlive this function call
    // (i.e. it needs to admit arbitrarily short lifetimes)
    // see: https://doc.rust-lang.org/nomicon/hrtb.html
    mut parser: impl for<'a> Parser<&'a str, O, E>,
    file: &str,
) -> Result<O, Box<dyn Error>> {
    let mut file = File::open(file).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    // println!(
    //     "Parsed file: {:?}",
    //     parser.parse_next(&mut contents.as_str().into()).unwrap()
    // );
    parser
        .parse_next(&mut contents.as_str().into())
        .map_err(|e| Box::new(FileParseError::new(e.to_string())) as Box<dyn Error>)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::core::VarName;
    use crate::{Value, InputProvider};

    use super::*;

    #[tokio::test]
    async fn test_parse_file() {
        let parser = crate::parser::lola_input_file;
        let file = "examples/simple_add.input";
        let mut data = parse_file(parser, file).await.unwrap();
        let x_vals = data
            .input_stream(&VarName("x".into()))
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            x_vals,
            vec![Value::Int(1), Value::Int(3)]
        );
    }
}
