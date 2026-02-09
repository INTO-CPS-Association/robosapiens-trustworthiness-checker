use ecow::{EcoString, EcoVec};
use winnow::{
    Result,
    ascii::{line_ending, multispace1},
    combinator::{alt, delimited, opt, separated, seq},
    error::ContextError,
    token::{literal, take_until},
};

use crate::{Specification, Value};
use std::{collections::BTreeMap, fmt::Debug};
use winnow::Parser;
pub use winnow::ascii::alphanumeric1 as ident;
pub use winnow::ascii::dec_int as integer;
pub use winnow::ascii::float;
pub use winnow::ascii::space0 as whitespace;

pub trait ExprParser<Expr>: Clone {
    fn parse(input: &mut &str) -> anyhow::Result<Expr>;
}
pub trait SpecParser<Spec: Specification>: Clone + 'static {
    fn parse(input: &mut &str) -> anyhow::Result<Spec>;
}

pub fn presult_to_string<T: Debug, E: Debug>(e: &Result<T, E>) -> String {
    format!("{:?}", e)
}

// Used for Lists in input streams (can only be Values)
fn value_list(s: &mut &str) -> Result<EcoVec<Value>> {
    delimited(
        seq!("List", whitespace, '('),
        separated(0.., val_or_container, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .map(|v: Vec<_>| EcoVec::from(v))
    .parse_next(s)
}

fn key_value(s: &mut &str) -> Result<(EcoString, Value)> {
    seq!(_: whitespace, string, _: whitespace, _: ':', _: whitespace, val_or_container,)
        .map(|(key, value)| (key.into(), value))
        .parse_next(s)
}

// Used for Maps in input streams (can only be Values)
fn value_map(s: &mut &str) -> Result<BTreeMap<EcoString, Value>> {
    delimited(
        seq!("Map", whitespace, '('),
        separated(0.., key_value, seq!(whitespace, ',', whitespace)),
        ')',
    )
    .map(|v: Vec<_>| BTreeMap::from_iter(v.into_iter()))
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
            // We also specifically exclude integers from being parsed as floats
            // (e.g. 1.0 is a float, 1 is an integer)
            float.with_taken().verify_map(|(x, s): (f64, &str)| {
                match (integer::<&str, i64, ContextError>).parse(s) {
                    Ok(_i) => None,
                    Err(_) => Some(Value::Float(x)),
                }
            }),
            // This is a separate case, since a i64 could overflow a f64
            integer.map(Value::Int),
            string.map(|s: &str| Value::Str(s.into())),
            literal("true").map(|_| Value::Bool(true)),
            literal("false").map(|_| Value::Bool(false)),
        )),
        whitespace,
    )
    .parse_next(s)
}

pub fn val_or_container(s: &mut &str) -> Result<Value> {
    delimited(
        whitespace,
        alt((val, value_list.map(Value::List), value_map.map(Value::Map))),
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
