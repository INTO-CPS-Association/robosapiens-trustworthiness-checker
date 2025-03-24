use ecow::EcoVec;
use winnow::{
    Result,
    ascii::{line_ending, multispace1},
    combinator::{alt, delimited, opt, separated, seq},
    error::ContextError,
    token::{literal, take_until},
};

use crate::Value;
use std::fmt::Debug;
use winnow::Parser;
pub use winnow::ascii::alphanumeric1 as ident;
pub use winnow::ascii::dec_int as integer;
pub use winnow::ascii::float;
pub use winnow::ascii::space0 as whitespace;

pub fn presult_to_string<T: Debug>(e: &Result<T>) -> String {
    format!("{:?}", e)
}

// Used for Lists in input streams (can only be Values)
pub fn value_list(s: &mut &str) -> Result<EcoVec<Value>> {
    delimited(
        seq!("List", whitespace, '('),
        separated(0.., val, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .map(|v: Vec<_>| EcoVec::from(v))
    .parse_next(s)
}

pub fn string<'a>(s: &mut &'a str) -> Result<&'a str> {
    delimited('"', take_until(0.., "\""), '\"').parse_next(s)
}

pub fn val(s: &mut &str) -> Result<Value> {
    delimited(
        whitespace,
        alt((
            // Order matters here, as prefixes of float can also be parsed as
            // an integer
            // We also specifically excude integrers from being parsed as floats
            // (e.g. 1.0 is a float, 1 is an integer)
            float.with_taken().verify_map(|(x, s): (f32, &str)| {
                match (integer::<&str, i64, ContextError>).parse(s) {
                    Ok(_i) => None,
                    Err(_) => Some(Value::Float(x)),
                }
            }),
            // This is a separate case, since a i64 could overflow a f32
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

pub fn line_comment(s: &mut &str) -> Result<()> {
    delimited(
        whitespace,
        seq!("//", opt(take_until(0.., '\n')), opt(line_ending)),
        whitespace,
    )
    .map(|_| ())
    .parse_next(s)
}

// Linebreak or Line Comment
pub fn lb_or_lc(s: &mut &str) -> Result<()> {
    alt((linebreak.void(), line_comment.void())).parse_next(s)
}

pub fn loop_ms_or_lb_or_lc(s: &mut &str) -> Result<()> {
    loop {
        let res = alt((multispace1.void(), lb_or_lc)).parse_next(s);
        if res.is_err() {
            // When neither matches - not an error, we are just done
            return Ok(());
        }
    }
}
