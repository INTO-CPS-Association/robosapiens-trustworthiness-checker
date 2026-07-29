use std::collections::{BTreeMap, BTreeSet};

use crate::lang::core::parser::*;
use crate::{Value, VarName};
use winnow::{
    Parser, Result,
    ascii::dec_uint,
    combinator::{alt, empty, repeat, separated, seq},
    token::literal,
};

use super::UntimedInputFileData;

fn borrowed_value_assignment<'a>(s: &mut &'a str) -> Result<(&'a str, Value)> {
    seq!((
        _: whitespace,
        ident,
        _: whitespace,
        _: literal("="),
        _: whitespace,
        val_or_container,
        _: whitespace,
    ))
    .parse_next(s)
}

fn value_assignment(s: &mut &str) -> Result<(VarName, Value)> {
    borrowed_value_assignment
        .map(|(name, value)| (name.into(), value))
        .parse_next(s)
}

fn value_assignments_vec(s: &mut &str) -> Result<Vec<(VarName, Value)>> {
    seq!((
        separated(0.., value_assignment, lb_or_lc),
        _: alt((lb_or_lc, empty)),
    ))
    .map(|(x,)| x)
    .parse_next(s)
}

fn value_assignments(s: &mut &str) -> Result<BTreeMap<VarName, Value>> {
    value_assignments_vec
        .map(|assignments| assignments.into_iter().collect())
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

/// Sparse fixed-layout rows parsed for the production file-input path.
///
/// Missing timestamps are deliberately not materialized here. The file input
/// stream expands them into bounded transport batches as they are consumed.
#[derive(Debug)]
pub(crate) struct PackedUntimedInput {
    pub(crate) layout: Box<[VarName]>,
    pub(crate) rows: BTreeMap<usize, Vec<Value>>,
    pub(crate) end_time: Option<usize>,
}

/// Parse untimed file input directly into sparse, fixed-layout rows.
///
/// This is the production file-input path. The general-purpose public parser
/// remains available for callers that need the timestamp-indexed representation.
/// Production input timestamps must be nondecreasing. Repeating a timestamp
/// extends the same logical row, and the last assignment to a selected variable
/// at that timestamp wins.
pub(crate) fn packed_untimed_input(
    contents: &str,
    vars: BTreeSet<VarName>,
) -> anyhow::Result<PackedUntimedInput> {
    if vars.is_empty() {
        return Ok(PackedUntimedInput {
            layout: Box::new([]),
            rows: BTreeMap::new(),
            end_time: None,
        });
    }

    let vars = vars.into_iter().collect::<Vec<_>>();
    let slots = vars
        .iter()
        .enumerate()
        .map(|(slot, var)| (var.name(), slot))
        .collect::<BTreeMap<_, _>>();
    let tick_width = vars.len();
    let mut rows = BTreeMap::new();
    let mut current_time = None;

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        let mut assignment = line;
        if let Some((candidate, remainder)) = line.split_once(':') {
            let candidate = candidate.trim();
            if !candidate.is_empty() && candidate.bytes().all(|byte| byte.is_ascii_digit()) {
                let timestamp = candidate.parse::<usize>().map_err(|error| {
                    anyhow::anyhow!("invalid timestamp at line {line_number}: {error}")
                })?;
                if let Some(previous) = current_time {
                    anyhow::ensure!(
                        timestamp >= previous,
                        "input timestamp decreased from {previous} to {timestamp} at line {line_number}"
                    );
                }
                current_time = Some(timestamp);
                assignment = remainder.trim();
                if assignment.is_empty() {
                    continue;
                }
            }
        }

        anyhow::ensure!(
            current_time.is_some(),
            "input assignment has no timestamp at line {line_number}"
        );
        let mut assignment_source = assignment;
        let (name, value) = borrowed_value_assignment
            .parse_next(&mut assignment_source)
            .map_err(|error| {
                anyhow::anyhow!("invalid assignment at line {line_number}: {error}")
            })?;
        if !assignment_source.is_empty() {
            line_comment.parse(assignment_source).map_err(|error| {
                anyhow::anyhow!("invalid trailing input at line {line_number}: {error}")
            })?;
        }
        if let Some(&slot) = slots.get(name) {
            rows.entry(current_time.expect("timestamp checked above"))
                .or_insert_with(|| vec![Value::NoVal; tick_width])[slot] = value;
        }
    }

    Ok(PackedUntimedInput {
        layout: vars.into_boxed_slice(),
        rows,
        end_time: current_time,
    })
}

#[cfg(test)]
mod tests {
    use winnow::error::ContextError;

    use super::*;
    use crate::{Value, lang::untimed_input::parser::value_assignment};

    fn presult_to_string<T: std::fmt::Debug, E: std::fmt::Debug>(
        result: &std::result::Result<T, E>,
    ) -> String {
        format!("{result:?}")
    }

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
    fn test_json_object_literal_assignments() {
        let expected = Value::Map(BTreeMap::from([
            ("x".into(), Value::Int(10)),
            ("y".into(), Value::Int(20)),
        ]));
        assert_eq!(
            value_assignment(&mut r#"payload = {"x": 10, "y": 20}"#),
            Ok(("payload".into(), expected.clone()))
        );
        assert_eq!(
            untimed_input_file(&mut r#"0: payload = {"x": 10, "y": 20}"#),
            Ok(BTreeMap::from([(
                0,
                BTreeMap::from([("payload".into(), expected)])
            )]))
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

    #[test]
    fn packed_file_input_preserves_sparse_multi_input_rows() {
        let input = "0: x = 1\n   y = 10\n3: x = 4\n   y = 40";
        let packed = packed_untimed_input(
            input,
            BTreeSet::from([VarName::new("x"), VarName::new("y")]),
        )
        .unwrap();
        let PackedUntimedInput {
            layout,
            rows,
            end_time,
        } = packed;

        assert_eq!(layout.as_ref(), [VarName::new("x"), VarName::new("y")]);
        assert_eq!(end_time, Some(3));
        assert_eq!(
            rows,
            BTreeMap::from([
                (0, vec![Value::Int(1), Value::Int(10)]),
                (3, vec![Value::Int(4), Value::Int(40)]),
            ])
        );
    }

    #[test]
    fn packed_file_input_accepts_repeated_timestamps_and_filters_variables() {
        let input = "0: ignored = 9\n0: x = 1 // first value\n0: x = 2\n0: x = 3 // last value";
        let packed = packed_untimed_input(input, BTreeSet::from([VarName::new("x")])).unwrap();
        let PackedUntimedInput { rows, end_time, .. } = packed;

        assert_eq!(end_time, Some(0));
        assert_eq!(rows, BTreeMap::from([(0, vec![Value::Int(3)])]));
    }

    #[test]
    fn packed_file_input_does_not_materialize_large_timestamp_gaps() {
        let input = "0: x = 1\n1000000000: x = 2";
        let packed = packed_untimed_input(input, BTreeSet::from([VarName::new("x")])).unwrap();
        let PackedUntimedInput { rows, end_time, .. } = packed;

        assert_eq!(end_time, Some(1_000_000_000));
        assert_eq!(rows.len(), 2);
    }
}
