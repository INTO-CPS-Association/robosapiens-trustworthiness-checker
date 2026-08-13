use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use futures::{FutureExt, StreamExt};
use smol::LocalExecutor;

use crate::causal::{CausalDomain, CausalValue, report_batch_json_line};
use crate::core::{OutputHandler, OutputStream, VarName};
use crate::io::testing::ManualOutputHandler;

/// A reusable causal report output adapter that writes one JSON object per
/// output batch, using JSON5 only when a value contains a non-finite float.
pub struct CausalJsonlOutputHandler<D: CausalDomain> {
    inner: Option<ManualOutputHandler<CausalValue<D>>>,
    path: PathBuf,
}

impl<D: CausalDomain> CausalJsonlOutputHandler<D> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_names: std::collections::BTreeSet<VarName>,
        path: impl AsRef<Path>,
    ) -> Self {
        Self {
            inner: Some(ManualOutputHandler::new(executor, var_names)),
            path: path.as_ref().to_owned(),
        }
    }
}

impl<D: CausalDomain> OutputHandler for CausalJsonlOutputHandler<D> {
    type Val = CausalValue<D>;

    fn provide_streams(
        &mut self,
        streams: std::collections::BTreeMap<VarName, OutputStream<Self::Val>>,
    ) {
        self.inner
            .as_mut()
            .expect("causal JSONL output streams were already consumed")
            .provide_streams(streams);
    }

    fn run(&mut self) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let mut inner = self
            .inner
            .take()
            .expect("causal JSONL output handler was already run");
        let output = inner.get_output();
        let executor = inner.executor.clone();
        let task = executor.spawn(inner.run()).fuse();
        let path = self.path.clone();

        Box::pin(async move {
            let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            else {
                let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
                return write_reports(&mut file, output, task).await;
            };
            std::fs::create_dir_all(parent)?;
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            write_reports(&mut file, output, task).await
        })
    }
}

async fn write_reports<D: CausalDomain>(
    file: &mut File,
    mut output: OutputStream<std::collections::BTreeMap<VarName, CausalValue<D>>>,
    task: futures::future::Fuse<smol::Task<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    while let Some(batch) = output.next().await {
        file.write_all(report_batch_json_line(batch)?.as_bytes())?;
    }
    task.await
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
    use crate::core::OutputHandler;
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
        let output: crate::OutputStream<CausalValue<CausalSet>> =
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
        let output: crate::OutputStream<CausalValue<CausalSet>> =
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
