use std::collections::BTreeMap;

use crate::core::input::empty_input_stream;
use crate::{InputBatch, InputStream, Value, VarName};

const DEFAULT_ROWS_PER_BATCH: usize = 256;

/// Convert columns of values into packed, simultaneous input steps.
pub fn input_stream(data: BTreeMap<VarName, Vec<Value>>) -> InputStream<Value> {
    if data.is_empty() {
        return empty_input_stream();
    }
    let columns = data.into_iter().collect::<Vec<_>>();
    let rows = columns
        .iter()
        .map(|(_, values)| values.len())
        .max()
        .unwrap_or(0);
    let layout = columns
        .iter()
        .map(|(var, _)| var.clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut column_values = columns
        .into_iter()
        .map(|(_, values)| values.into_iter())
        .collect::<Vec<_>>();
    let rows_per_batch = DEFAULT_ROWS_PER_BATCH;
    Box::pin(async_stream::try_stream! {
        for start in (0..rows).step_by(rows_per_batch) {
            let end = (start + rows_per_batch).min(rows);
            let mut values = Vec::with_capacity((end - start) * column_values.len());
            for _ in start..end {
                for column in &mut column_values {
                    values.push(column.next().unwrap_or(Value::NoVal));
                }
            }
            let batch = InputBatch::packed_rows(layout.clone(), values)?;
            yield batch;
        }
    })
}

/// Build a packed input stream for homogeneous, fixed-width columns.
///
/// Unlike [`input_stream`], this variant does not need to synthesize dynamic
/// `Value::NoVal` entries for short columns. It can therefore retain the
/// packed-row representation all the way to a typed runtime, keeping variable
/// names out of the per-sample storage.
pub fn typed_input_stream<V: 'static>(
    data: BTreeMap<VarName, Vec<V>>,
) -> anyhow::Result<InputStream<V>> {
    if data.is_empty() {
        return Ok(empty_input_stream());
    }

    let columns = data.into_iter().collect::<Vec<_>>();
    let (first_var, first_values) = &columns[0];
    let rows = first_values.len();
    for (var, values) in columns.iter().skip(1) {
        anyhow::ensure!(
            values.len() == rows,
            "typed input columns have unequal lengths: variable `{var}` has length {}, but variable `{first_var}` has length {rows}",
            values.len()
        );
    }

    let layout = columns
        .iter()
        .map(|(var, _)| var.clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let mut column_values = columns
        .into_iter()
        .map(|(_, values)| values.into_iter())
        .collect::<Vec<_>>();
    let rows_per_batch = DEFAULT_ROWS_PER_BATCH;
    Ok(Box::pin(async_stream::try_stream! {
        for start in (0..rows).step_by(rows_per_batch) {
            let end = (start + rows_per_batch).min(rows);
            let mut values = Vec::with_capacity((end - start) * column_values.len());
            for _ in start..end {
                for column in &mut column_values {
                    values.push(column.next().expect("validated typed columns contain every row"));
                }
            }
            let batch = InputBatch::packed_rows(layout.clone(), values)?;
            yield batch;
        }
    }))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::collections::BTreeMap;

    use crate::{InputStream, Value, VarName};

    use super::{input_stream, typed_input_stream};

    #[test]
    fn stream_preserves_step_rows() {
        smol::block_on(async {
            let mut batches = input_stream(BTreeMap::from([
                (VarName::new("x"), vec![Value::Int(1), Value::Int(2)]),
                (VarName::new("y"), vec![Value::Int(10), Value::Int(20)]),
            ]));
            let mut rows = Vec::new();
            while let Some(batch) = batches.next().await {
                let batch = batch.unwrap();
                rows.extend(batch.ticks().map(|row| {
                    row.iter()
                        .map(|event| event.value.clone())
                        .collect::<Vec<_>>()
                }));
            }
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int(1), Value::Int(10)],
                    vec![Value::Int(2), Value::Int(20)],
                ]
            );
        });
    }

    #[test]
    fn map_input_is_step_shaped() {
        smol::block_on(async {
            // Map columns define simultaneous rows, so both constructors
            // produce `InputStream`s of fixed-width steps.
            let mut batches: InputStream<Value> = input_stream(BTreeMap::from([
                (VarName::new("x"), vec![Value::Int(1), Value::Int(2)]),
                (VarName::new("y"), vec![Value::Int(10), Value::Int(20)]),
            ]));
            let batch = batches.next().await.unwrap().unwrap();
            let (layout, _values) = batch
                .segments()
                .next()
                .and_then(|segment| segment.packed_rows())
                .expect("map input stays packed");
            assert_eq!(layout.len(), 2);
            assert_eq!(batch.tick_count(), 2);

            let mut typed: InputStream<i32> = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), vec![1, 2]),
                (VarName::new("y"), vec![10, 20]),
            ]))
            .unwrap();
            let batch = typed.next().await.unwrap().unwrap();
            let (_layout, values) = batch
                .segments()
                .next()
                .and_then(|segment| segment.packed_rows())
                .expect("typed map input stays packed");
            assert_eq!(values, [1, 10, 2, 20]);
        });
    }

    #[test]
    fn typed_stream_preserves_packed_rows() {
        smol::block_on(async {
            let mut batches = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), vec![1, 2]),
                (VarName::new("y"), vec![10, 20]),
            ]))
            .unwrap();
            let mut rows = Vec::new();
            while let Some(batch) = batches.next().await {
                let batch = batch.unwrap();
                rows.extend(
                    batch
                        .ticks()
                        .map(|row| row.iter().map(|event| *event.value).collect::<Vec<_>>()),
                );
            }
            assert_eq!(rows, vec![vec![1, 10], vec![2, 20]]);
        });
    }

    #[test]
    fn typed_stream_rejects_unequal_columns_during_construction() {
        let result = typed_input_stream(BTreeMap::from([
            (VarName::new("x"), vec![1, 2]),
            (VarName::new("y"), vec![10]),
        ]));
        let error = match result {
            Ok(_) => panic!("unequal typed columns must fail during construction"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("unequal lengths"));
        assert!(message.contains("x"));
        assert!(message.contains("y"));
        assert!(message.contains("length 1"));
        assert!(message.contains("length 2"));
    }

    #[test]
    fn typed_stream_accepts_empty_columns() {
        smol::block_on(async {
            let mut batches = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), Vec::<i32>::new()),
                (VarName::new("y"), Vec::<i32>::new()),
            ]))
            .unwrap();
            assert!(batches.next().await.is_none());
        });
    }
}
