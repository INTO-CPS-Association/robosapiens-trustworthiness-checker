use std::{collections::BTreeMap, num::NonZeroUsize};

use crate::{InputBatch, InputStream, Value, VarName};

const DEFAULT_ROWS_PER_BATCH: usize = 256;

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
            let mut batch = Vec::with_capacity((end - start) * tick_width.get());
            for row in start..end {
                batch.extend(columns.iter().map(|(var, values)| {
                    crate::InputEvent::new(
                        var.clone(),
                        values.get(row).cloned().unwrap_or(Value::NoVal),
                    )
                }));
            }
            yield InputBatch::packed_steps(tick_width, batch)?;
        }
    })
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::collections::BTreeMap;

    use crate::{Value, VarName};

    use super::input_stream;

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
}
