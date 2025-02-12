use winnow::{
    ascii::line_ending,
    combinator::{alt, delimited, separated, seq},
    token::{literal, take_until},
    PResult,
};

use crate::Value;
use std::fmt::Debug;
pub use winnow::ascii::alphanumeric1 as ident;
pub use winnow::ascii::dec_int as integer;
pub use winnow::ascii::space0 as whitespace;
use winnow::Parser;

pub fn presult_to_string<T: Debug>(e: &PResult<T>) -> String {
    format!("{:?}", e)
}

// Used for Lists in input streams (can only be Values)
pub fn value_list(s: &mut &str) -> PResult<Vec<Value>> {
    delimited(
        seq!("List", whitespace, '('),
        separated(0.., val, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .parse_next(s)
}

pub fn string<'a>(s: &mut &'a str) -> PResult<&'a str> {
    delimited('"', take_until(0.., "\""), '\"').parse_next(s)
}

pub fn val(s: &mut &str) -> PResult<Value> {
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

pub fn linebreak(s: &mut &str) -> PResult<()> {
    delimited(whitespace, line_ending, whitespace)
        .map(|_| ())
        .parse_next(s)
}
