//! Shared expansion, checking and execution fixtures for DSRV feature tests.

use crate::VarName;
use crate::core::{ExecutionPolicy, OutputInterface, RuntimeSpec, Semantics, Value};
use crate::dsrv_fixtures::WithoutWarnings;
use crate::lang::dsrv::ElaboratedDsrvSpecification;
use crate::lang::dsrv::ast::DsrvSpecification;
use crate::lang::dsrv::diagnostics::SemanticError;
use crate::lang::dsrv::expand::DsrvExpandError;
use crate::lang::dsrv::modules::{ModuleCollector, show_path};
use crate::lang::dsrv::path::ModuleName;
use crate::runtime::builder::GeneralRuntimeBuilder;
use futures::FutureExt;
use smol::LocalExecutor;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Expand a program whose root is `root` and whose modules are supplied
/// from `modules`.
pub(super) fn expand(
    root: &str,
    modules: &[(&str, &str)],
) -> Result<DsrvSpecification, DsrvExpandError> {
    let mut collector = ModuleCollector::new(root).expect("a parsable root");
    while let Some(module) = collector.next_request().map(<[ModuleName]>::to_vec) {
        let wanted = show_path(&module);
        let text = modules
            .iter()
            .find(|(name, _)| *name == wanted)
            .unwrap_or_else(|| panic!("no source for {wanted}"))
            .1;
        collector.supply(text).expect("a parsable module");
    }
    crate::lang::dsrv::expand::expand_program(collector.finish().expect("collected"), None)
}

pub(super) fn check_errors(spec: DsrvSpecification) -> Vec<SemanticError> {
    spec.check()
        .without_warnings()
        .err()
        .unwrap_or_else(|| panic!("expected checking to fail ({:?})", "checked"))
}

pub(super) fn elaborate(spec: DsrvSpecification) -> ElaboratedDsrvSpecification {
    spec.check_and_elaborate()
        .without_warnings()
        .unwrap_or_else(|errors| panic!("{errors:?}"))
}

pub(super) fn is_type_error(errors: &[SemanticError]) -> bool {
    errors
        .iter()
        .any(|error| matches!(error, SemanticError::TypeError(_)))
}

/// Run the elaborated `spec` on `runtime` over `inputs`, one value per tick
/// for each input, returning each tick's outputs and the failure that
/// stopped it, if any. A failure is an error from the runtime or a panic in
/// its evaluation, which is how both runtimes report a failed operation.
pub(super) fn run(
    runtime: RuntimeSpec,
    semantics: Semantics,
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> (Vec<BTreeMap<VarName, Value>>, Option<String>) {
    let executor = Rc::new(LocalExecutor::new());
    let (sender, mut receiver) = crate::io::channel::output(1024);
    let outputs = spec.output_vars().clone();
    let run = {
        let executor = Rc::clone(&executor);
        async move {
            let writer = crate::io::channel::open_output(
                sender,
                OutputInterface::outputs(outputs.iter().cloned()).expect("an interface"),
            )
            .await
            .expect("the channel opens");
            let monitor = GeneralRuntimeBuilder::new()
                .executor(Rc::clone(&executor))
                .model(spec)
                .input(crate::io::map::input_stream(inputs))
                .output_writer(writer)
                .runtime(runtime)
                .semantics(semantics)
                .build()
                .await
                .expect("the runtime builds");
            crate::core::Runtime::run(monitor).await
        }
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        smol::block_on(executor.run(std::panic::AssertUnwindSafe(run).catch_unwind()))
    }));
    let failure = match outcome {
        Ok(Ok(Ok(()))) => None,
        Ok(Ok(Err(error))) => Some(format!("{error:#}")),
        Ok(Err(panic)) | Err(panic) => Some(panic_message(panic)),
    };
    let mut rows = Vec::new();
    while let Ok(row) = receiver.try_recv() {
        rows.push(row);
    }
    (rows, failure)
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .unwrap_or_else(|| "a panic with no message".to_owned())
}

/// Run `spec` on dataflow and semi-sync, comparing outputs and failure presence.
///
/// On failure, the synchronous dataflow runtime has published every tick
/// before the failing one, which is what the returned rows are; semi-sync
/// publishes asynchronously and may not have written its last completed
/// tick when the failure stops it, so its rows need only be a prefix of
/// them. Which tick failed is established from the failure itself: callers
/// check that it names the value that arrived at that tick.
pub(super) fn run_both(
    spec: ElaboratedDsrvSpecification,
    inputs: BTreeMap<VarName, Vec<Value>>,
) -> (Vec<BTreeMap<VarName, Value>>, Option<String>) {
    let semantics = Semantics::TypedUntimed;
    let (dataflow_rows, dataflow_failure) = run(
        RuntimeSpec::Dataflow(ExecutionPolicy::Synchronous),
        semantics,
        spec.clone(),
        inputs.clone(),
    );
    let (semi_rows, semi_failure) = run(RuntimeSpec::SemiSync, semantics, spec, inputs);
    assert_eq!(
        dataflow_failure.is_some(),
        semi_failure.is_some(),
        "only one runtime failed: dataflow {dataflow_failure:?}, semi-sync {semi_failure:?}"
    );
    match (&dataflow_failure, &semi_failure) {
        (Some(_), Some(_)) => {
            assert!(
                dataflow_rows.starts_with(&semi_rows),
                "semi-sync published {semi_rows:?}, which dataflow did not: {dataflow_rows:?}"
            );
        }
        _ => assert_eq!(dataflow_rows, semi_rows, "the runtimes disagree on outputs"),
    }
    (dataflow_rows, dataflow_failure)
}

pub(super) fn inputs<const N: usize>(
    values: [(&str, Vec<Value>); N],
) -> BTreeMap<VarName, Vec<Value>> {
    values
        .into_iter()
        .map(|(name, values)| (VarName::from(name), values))
        .collect()
}
