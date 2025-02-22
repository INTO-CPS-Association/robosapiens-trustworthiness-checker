use winnow::{
    Result,
    ascii::line_ending,
    combinator::{alt, delimited, separated, seq},
    token::{literal, take_until},
};

use crate::Value;
use std::fmt::Debug;
use winnow::Parser;
pub use winnow::ascii::alphanumeric1 as ident;
pub use winnow::ascii::dec_int as integer;
pub use winnow::ascii::space0 as whitespace;

pub fn presult_to_string<T: Debug>(e: &Result<T>) -> String {
    format!("{:?}", e)
}

// Used for Lists in input streams (can only be Values)
pub fn value_list(s: &mut &str) -> Result<Vec<Value>> {
    delimited(
        seq!("List", whitespace, '('),
        separated(0.., val, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .parse_next(s)
}

pub fn string<'a>(s: &mut &'a str) -> Result<&'a str> {
    delimited('"', take_until(0.., "\""), '\"').parse_next(s)
}

pub fn val(s: &mut &str) -> Result<Value> {
    delimited(
        whitespace,
        alt((
            integer.map(Value::Int),
            string.map(|s: &str| Value::Str(s.into())),
            literal("true").map(|_| Value::Bool(true)),
            literal("false").map(|_| Value::Bool(false)),
            value_list.map(Value::List),
        )),
        whitespace,
    )
    .parse_next(s)
}

pub fn linebreak(s: &mut &str) -> Result<()> {
    delimited(whitespace, line_ending, whitespace)
        .map(|_| ())
        .parse_next(s)
}
