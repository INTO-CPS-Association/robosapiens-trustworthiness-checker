use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use futures::{FutureExt, StreamExt};
use futures::{future::LocalBoxFuture, stream::FuturesUnordered};
use smol::LocalExecutor;

use crate::causal::{CausalDomain, CausalValue, report_batch_json_line};
use crate::core::{LocalStream, VarName};

/// A standalone causal report adapter that merges named streams into JSONL rows.
///
/// The historical `CausalJsonlOutputHandler` name is retained for compatibility
/// with the public causal export. It consumes one value from every declared
/// stream per row and never emits a partial row.
pub struct CausalJsonlOutputHandler<D: CausalDomain> {
    var_names: BTreeSet<VarName>,
    streams: Option<BTreeMap<VarName, LocalStream<CausalValue<D>>>>,
    path: PathBuf,
}

impl<D: CausalDomain> CausalJsonlOutputHandler<D> {
    pub fn new(
        _executor: Rc<LocalExecutor<'static>>,
        var_names: BTreeSet<VarName>,
        path: impl AsRef<Path>,
    ) -> Self {
        Self {
            var_names,
            streams: None,
            path: path.as_ref().to_owned(),
        }
    }

    pub fn provide_streams(&mut self, streams: BTreeMap<VarName, LocalStream<CausalValue<D>>>) {
        assert_eq!(
            self.var_names,
            streams.keys().cloned().collect(),
            "Variable names provided do not match the names supplied to the causal JSONL adapter."
        );
        self.streams = Some(streams);
    }

    pub fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let streams = self
            .streams
            .take()
            .expect("causal JSONL output streams were already consumed");
        let (variables, mut streams): (Vec<_>, Vec<_>) = streams.into_iter().unzip();
        let path = self.path.clone();

        Box::pin(async move {
            let mut file = open_append(&path)?;
            if variables.is_empty() {
                return Ok(());
            }

            loop {
                let Some(values) = next_equal_row(&variables, &mut streams).await? else {
                    break;
                };
                let batch = variables
                    .iter()
                    .cloned()
                    .zip(values)
                    .collect::<BTreeMap<_, _>>();
                file.write_all(report_batch_json_line(batch)?.as_bytes())?;
            }
            Ok(())
        })
    }
}

async fn next_equal_row<D: CausalDomain>(
    variables: &[VarName],
    streams: &mut [LocalStream<CausalValue<D>>],
) -> anyhow::Result<Option<Vec<CausalValue<D>>>> {
    let stream_count = streams.len();
    let mut nexts = streams
        .iter_mut()
        .enumerate()
        .map(|(index, stream)| async move { (index, stream.next().await) })
        .collect::<FuturesUnordered<_>>();
    let mut values = (0..stream_count)
        .map(|_| None)
        .collect::<Vec<Option<CausalValue<D>>>>();
    let mut resolved = vec![false; stream_count];
    let mut ended = vec![false; stream_count];

    while let Some((index, value)) = nexts.next().await {
        resolved[index] = true;
        match value {
            Some(value) => values[index] = Some(value),
            None => {
                ended[index] = true;

                // Observe siblings that are already ready so the error can say
                // which values would otherwise have been discarded. Do not
                // await the rest: dropping `nexts` below cancels those futures.
                while let Some(Some((index, value))) = nexts.next().now_or_never() {
                    resolved[index] = true;
                    match value {
                        Some(value) => values[index] = Some(value),
                        None => ended[index] = true,
                    }
                }

                let has_completed_sibling = values.iter().any(Option::is_some);
                let has_pending_sibling = resolved.iter().any(|resolved| !*resolved);
                if has_completed_sibling || has_pending_sibling {
                    let error = unequal_row_error(variables, &values, &resolved, &ended);
                    drop(nexts);
                    return Err(error);
                }

                return Ok(None);
            }
        }
    }

    Ok(Some(
        values
            .into_iter()
            .map(|value| value.expect("a complete causal output row has one value per variable"))
            .collect(),
    ))
}

fn unequal_row_error<T>(
    variables: &[VarName],
    values: &[Option<T>],
    resolved: &[bool],
    ended: &[bool],
) -> anyhow::Error {
    let ended = format_variables(
        variables,
        ended
            .iter()
            .enumerate()
            .filter_map(|(index, ended)| (*ended).then_some(index)),
    );
    let completed = format_variables(
        variables,
        values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.as_ref().map(|_| index)),
    );
    let pending = format_variables(
        variables,
        resolved
            .iter()
            .enumerate()
            .filter_map(|(index, resolved)| (!*resolved).then_some(index)),
    );

    anyhow::anyhow!(
        "causal output streams must have equal row counts: EOF in variables {ended} before the row was complete; completed variables: {completed}; pending variables: {pending}. Ensure every declared causal output stream yields the same number of rows."
    )
}

fn format_variables(variables: &[VarName], indices: impl IntoIterator<Item = usize>) -> String {
    let names = indices
        .into_iter()
        .map(|index| variables[index].to_string())
        .collect::<Vec<_>>();
    format!("[{}]", names.join(", "))
}

fn open_append(path: &Path) -> anyhow::Result<File> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;

    use futures::stream;
    use macro_rules_attribute::apply;
    use uuid::Uuid;

    use crate::async_test;
    use crate::causal::{CausalDomain, CausalSet, CausalValue, TimedAtom};
    use crate::{Value, VarName};

    use super::CausalJsonlOutputHandler;

    #[apply(async_test)]
    async fn writes_the_shared_report_schema_to_jsonl(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "trustworthiness-checker-causal-report-{}.jsonl",
            Uuid::new_v4()
        ));
        let mut handler = CausalJsonlOutputHandler::<CausalSet>::new(
            executor,
            BTreeSet::from([VarName::new("verdict")]),
            &path,
        );
        let output: crate::LocalStream<CausalValue<CausalSet>> =
            Box::pin(stream::iter([CausalValue::new(
                Value::Bool(false),
                CausalSet::atom(TimedAtom::new("velocity".into(), 0)),
            )]));
        handler.provide_streams(BTreeMap::from([(VarName::new("verdict"), output)]));

        handler.run().await.unwrap();
        let jsonl = fs::read_to_string(&path).unwrap();
        assert_eq!(
            jsonl,
            r#"{"values":{"verdict":false},"causality":{"verdict":{"alternatives":[{"causes":[{"input":"velocity","logical_tick":0,"roles":[]}]}]}}}
"#
        );
        fs::remove_file(path).unwrap();
    }

    #[apply(async_test)]
    async fn writes_equal_rows_from_two_streams_without_crossing_values(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "trustworthiness-checker-causal-report-{}.jsonl",
            Uuid::new_v4()
        ));
        let mut handler = CausalJsonlOutputHandler::<CausalSet>::new(
            executor,
            BTreeSet::from([VarName::new("x"), VarName::new("y")]),
            &path,
        );
        let x: crate::LocalStream<CausalValue<CausalSet>> = Box::pin(stream::iter([
            CausalValue::constant(Value::Int(1)),
            CausalValue::constant(Value::Int(2)),
        ]));
        let y: crate::LocalStream<CausalValue<CausalSet>> = Box::pin(stream::iter([
            CausalValue::constant(Value::Int(10)),
            CausalValue::constant(Value::Int(20)),
        ]));
        handler.provide_streams(BTreeMap::from([
            (VarName::new("x"), x),
            (VarName::new("y"), y),
        ]));

        handler.run().await.unwrap();
        let rows = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["values"]["x"], 1);
        assert_eq!(rows[0]["values"]["y"], 10);
        assert_eq!(rows[1]["values"]["x"], 2);
        assert_eq!(rows[1]["values"]["y"], 20);
        fs::remove_file(path).unwrap();
    }

    #[apply(async_test)]
    async fn rejects_early_eof_without_discarding_a_ready_sibling(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "trustworthiness-checker-causal-report-{}.jsonl",
            Uuid::new_v4()
        ));
        let mut handler = CausalJsonlOutputHandler::<CausalSet>::new(
            executor,
            BTreeSet::from([VarName::new("ended"), VarName::new("ready")]),
            &path,
        );
        let ended: crate::LocalStream<CausalValue<CausalSet>> = Box::pin(stream::empty());
        let ready: crate::LocalStream<CausalValue<CausalSet>> =
            Box::pin(stream::iter([CausalValue::constant(Value::Int(1))]));
        handler.provide_streams(BTreeMap::from([
            (VarName::new("ended"), ended),
            (VarName::new("ready"), ready),
        ]));

        let error = handler.run().await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("equal row counts"), "{message}");
        assert!(message.contains("EOF in variables [ended]"), "{message}");
        assert!(
            message.contains("completed variables: [ready]"),
            "{message}"
        );
        assert!(message.contains("pending variables: []"), "{message}");
        assert!(fs::read_to_string(&path).unwrap().is_empty());
        fs::remove_file(path).unwrap();
    }

    #[apply(async_test)]
    async fn rejects_early_eof_and_cancels_a_pending_sibling(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "trustworthiness-checker-causal-report-{}.jsonl",
            Uuid::new_v4()
        ));
        let mut handler = CausalJsonlOutputHandler::<CausalSet>::new(
            executor,
            BTreeSet::from([VarName::new("ended"), VarName::new("pending")]),
            &path,
        );
        let ended: crate::LocalStream<CausalValue<CausalSet>> = Box::pin(stream::empty());
        let pending: crate::LocalStream<CausalValue<CausalSet>> = Box::pin(stream::pending());
        handler.provide_streams(BTreeMap::from([
            (VarName::new("ended"), ended),
            (VarName::new("pending"), pending),
        ]));

        let error = match futures::future::select(
            Box::pin(handler.run()),
            Box::pin(async {
                smol::Timer::after(std::time::Duration::from_secs(1)).await;
            }),
        )
        .await
        {
            futures::future::Either::Left((result, _)) => result.unwrap_err(),
            futures::future::Either::Right((_, _)) => {
                panic!("causal JSONL adapter waited for a pending sibling after EOF")
            }
        };
        let message = error.to_string();
        assert!(message.contains("EOF in variables [ended]"), "{message}");
        assert!(message.contains("completed variables: []"), "{message}");
        assert!(
            message.contains("pending variables: [pending]"),
            "{message}"
        );
        assert!(fs::read_to_string(&path).unwrap().is_empty());
        fs::remove_file(path).unwrap();
    }

    #[apply(async_test)]
    async fn writes_non_finite_values_as_one_json5_line(
        executor: std::rc::Rc<smol::LocalExecutor<'static>>,
    ) {
        let path = std::env::temp_dir().join(format!(
            "trustworthiness-checker-causal-report-{}.jsonl",
            Uuid::new_v4()
        ));
        let mut handler = CausalJsonlOutputHandler::<CausalSet>::new(
            executor,
            BTreeSet::from([VarName::new("measurement")]),
            &path,
        );
        let output: crate::LocalStream<CausalValue<CausalSet>> =
            Box::pin(stream::iter([CausalValue::constant(Value::Float(
                f64::INFINITY,
            ))]));
        handler.provide_streams(BTreeMap::from([(VarName::new("measurement"), output)]));

        handler.run().await.unwrap();
        let jsonl = fs::read_to_string(&path).unwrap();
        let lines = jsonl.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("measurement:Infinity"), "{jsonl}");
        assert!(json5::from_str::<serde_json::Value>(lines[0]).is_ok());
        fs::remove_file(path).unwrap();
    }
}
