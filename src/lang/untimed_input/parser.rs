use std::collections::BTreeMap;

use crate::lang::core::parser::*;
use crate::{Value, VarName};
use winnow::{
    Parser, Result,
    ascii::dec_uint,
    combinator::{alt, empty, repeat, separated, seq},
    token::literal,
};

use super::UntimedInputFileData;

fn value_assignment(s: &mut &str) -> Result<(VarName, Value)> {
    seq!((
        _: whitespace,
        ident,
        _: whitespace,
        _: literal("="),
        _: whitespace,
        val,
        _: whitespace,
    ))
    .map(|(name, value)| (name.into(), value))
    .parse_next(s)
}

fn value_assignments(s: &mut &str) -> Result<BTreeMap<VarName, Value>> {
    seq!((
        separated(0.., value_assignment, lb_or_lc),
        _: alt((lb_or_lc, empty)),
    ))
    .map(|(x,)| x)
    .parse_next(s)
}

fn time_stamped_assignments(s: &mut &str) -> Result<(usize, BTreeMap<VarName, Value>)> {
    seq!((
        _: whitespace,
        dec_uint,
        _: whitespace,
        _: literal(":"),
        _: separated(0.., whitespace, lb_or_lc).map(|_: Vec<_>| ()),
        value_assignments
    ))
    .map(|(time, assignments)| (time, assignments))
    .parse_next(s)
}

fn timed_assignments(s: &mut &str) -> Result<UntimedInputFileData> {
    repeat(0.., time_stamped_assignments).parse_next(s)
}

pub fn untimed_input_file(s: &mut &str) -> Result<UntimedInputFileData> {
    timed_assignments(s)
}

#[cfg(test)]
mod tests {
    use winnow::error::ContextError;

    use super::*;
    use crate::{Value, lang::untimed_input::parser::value_assignment};

    #[test]
    fn test_value_assignment() -> Result<(), ContextError> {
        assert_eq!(
            value_assignment(&mut (*"x = 42".to_string()).into())?,
            ("x".into(), Value::Int(42)),
        );
        assert_eq!(
            value_assignment(&mut (*"y = 3".to_string()).into())?,
            ("y".into(), Value::Int(3)),
        );
        Ok(())
    }

    #[test]
    fn test_value_assignments() -> Result<(), ContextError> {
        assert_eq!(
            value_assignments(&mut (*"x = 42\ny = 3".to_string()).into())?,
            BTreeMap::from([("x".into(), Value::Int(42)), ("y".into(), Value::Int(3)),]),
        );
        assert_eq!(
            value_assignments(&mut (*"".to_string()).into())?,
            BTreeMap::new(),
        );
        Ok(())
    }

    #[test]
    fn test_time_stamped_assignment() -> Result<(), ContextError> {
        assert_eq!(
            time_stamped_assignments(&mut (*"0: x = 42".to_string()).into())?,
            (0, BTreeMap::from([("x".into(), Value::Int(42))])),
        );
        assert_eq!(
            time_stamped_assignments(&mut (*"1: x = 42\ny = 3".to_string()).into())?,
            (
                1,
                BTreeMap::from([("x".into(), Value::Int(42)), ("y".into(), Value::Int(3))])
            ),
        );
        assert_eq!(
            time_stamped_assignments(&mut (*"2:\n x = 42\ny = 3".to_string()).into())?,
            (
                2,
                BTreeMap::from([("x".into(), Value::Int(42)), ("y".into(), Value::Int(3))])
            ),
        );
        Ok(())
    }

    #[test]
    fn test_list_assignments() {
        assert_eq!(
            presult_to_string(&value_assignment(&mut "y = List()")),
            r#"Ok((VarName::new("y"), List([])))"#
        );
        // Difference between value assignment and sexpr assignment
        assert_eq!(
            value_assignment(&mut "y = List()"),
            Ok(("y".into(), Value::List(vec![].into())))
        );
    }

    #[test]
    fn test_float_assignments() {
        assert_eq!(
            presult_to_string(&value_assignment(&mut "y = 3.4")),
            r#"Ok((VarName::new("y"), Float(3.4)))"#
        );
        assert_eq!(
            presult_to_string(&value_assignment(&mut "y = 1e-3")),
            r#"Ok((VarName::new("y"), Float(0.001)))"#
        );
    }
}
