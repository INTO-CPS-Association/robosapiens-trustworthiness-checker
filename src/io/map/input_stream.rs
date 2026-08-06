use std::{collections::BTreeMap, num::NonZeroUsize};

use crate::{InputBatch, InputStream, Value, VarName};

const DEFAULT_ROWS_PER_BATCH: usize = 256;

fn packed_rows<V: Clone>(
    columns: &[(VarName, Vec<V>)],
    start: usize,
    end: usize,
    missing: Option<fn() -> V>,
) -> Vec<V> {
    let mut values = Vec::with_capacity((end - start) * columns.len());
    for row in start..end {
        for (_, column) in columns {
            let value = match column.get(row) {
                Some(value) => value.clone(),
                None => missing
                    .map(|make_missing| make_missing())
                    .expect("equal-length typed columns must contain every row"),
            };
            values.push(value);
        }
    }
    values
}

/// Convert columns of values into packed, simultaneous input steps.
pub fn input_stream(data: BTreeMap<VarName, Vec<Value>>) -> InputStream<Value> {
    if data.is_empty() {
        return Box::pin(futures::stream::empty());
    }
    let columns = data.into_iter().collect::<Vec<_>>();
    let rows = columns
        .iter()
        .map(|(_, values)| values.len())
        .max()
        .unwrap_or(0);

    let tick_width = columns.len();
    let rows_per_batch = DEFAULT_ROWS_PER_BATCH;
    let tick_width =
        NonZeroUsize::new(tick_width).expect("non-empty input has non-zero tick width");
    Box::pin(async_stream::try_stream! {
        for start in (0..rows).step_by(rows_per_batch) {
            let end = (start + rows_per_batch).min(rows);
            let values = packed_rows(&columns, start, end, Some(|| Value::NoVal));
            let batch = values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    let var = &columns[index % columns.len()].0;
                    crate::InputEvent::new(var.clone(), value)
                })
                .collect();
            yield InputBatch::packed_steps(tick_width, batch)?;
        }
    })
}

/// Build a packed input stream for homogeneous, fixed-width columns.
///
/// Unlike [`input_stream`], this variant does not need to synthesize dynamic
/// `Value::NoVal` entries for short columns. It can therefore retain the
/// packed-row representation all the way to a typed runtime, keeping variable
/// names out of the per-sample storage.
pub fn typed_input_stream<V: Clone + 'static>(data: BTreeMap<VarName, Vec<V>>) -> InputStream<V> {
    if data.is_empty() {
        return Box::pin(futures::stream::empty());
    }

    let columns = data.into_iter().collect::<Vec<_>>();
    let rows = columns[0].1.len();
    if columns.iter().any(|(_, values)| values.len() != rows) {
        return Box::pin(futures::stream::once(async {
            Err(anyhow::anyhow!(
                "typed input columns must all have the same length"
            ))
        }));
    }

    let layout = columns
        .iter()
        .map(|(var, _)| var.clone())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let rows_per_batch = DEFAULT_ROWS_PER_BATCH;
    Box::pin(async_stream::try_stream! {
        for start in (0..rows).step_by(rows_per_batch) {
            let end = (start + rows_per_batch).min(rows);
            let values = packed_rows(&columns, start, end, None);
            yield InputBatch::trusted_packed_rows(layout.clone(), values);
        }
    })
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::collections::BTreeMap;

    use crate::{Value, VarName};

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
    fn typed_stream_preserves_packed_rows() {
        smol::block_on(async {
            let mut batches = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), vec![1, 2]),
                (VarName::new("y"), vec![10, 20]),
            ]));
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
    fn typed_stream_rejects_unequal_columns() {
        smol::block_on(async {
            let mut batches = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), vec![1, 2]),
                (VarName::new("y"), vec![10]),
            ]));
            assert_eq!(
                batches.next().await.unwrap().unwrap_err().to_string(),
                "typed input columns must all have the same length"
            );
            assert!(batches.next().await.is_none());
        });
    }

    #[test]
    fn typed_stream_accepts_empty_columns() {
        smol::block_on(async {
            let mut batches = typed_input_stream(BTreeMap::from([
                (VarName::new("x"), Vec::<i32>::new()),
                (VarName::new("y"), Vec::<i32>::new()),
            ]));
            assert!(batches.next().await.is_none());
        });
    }
}
