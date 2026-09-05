use std::rc::Rc;

use smol::LocalExecutor;

use crate::causal::{
    CausalDomain, CausalSet, CausalValue, RoleCausalAntichain, RoleCausalDomain, RoleCausalSet,
};
use crate::core::OutputWriter;
use crate::lang::dsrv::ast::{CheckedDsrvSpecification, DsrvSpecification};
use crate::runtime::RuntimeBuilder;
use crate::runtime::semi_sync::{SemiSyncRuntime, SemiSyncRuntimeBuilder};
use crate::{Value, io::OpenedInput};

use super::{
    CausalCheckedSemiSyncConfig, CausalDsrvSemantics, CausalSemiSyncConfig, RoleCausalDsrvSemantics,
};

/// A model-owned causal semi-synchronous runtime builder.
///
/// Unlike [`SemiSyncRuntimeBuilder`], this builder accepts an ordinary
/// `InputStream<Value>`. It derives the model input set and annotates each
/// logical tick internally before constructing the causal runtime.
pub struct CausalRuntimeBuilder<D: CausalDomain = CausalSet> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<DsrvSpecification>,
    input: Option<OpenedInput<Value>>,
    output_writer: Option<OutputWriter<CausalValue<D>>>,
}

impl<D: CausalDomain> CausalRuntimeBuilder<D> {
    pub fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn model(mut self, model: DsrvSpecification) -> Self {
        self.model = Some(model);
        self
    }

    pub fn input(mut self, input: impl Into<OpenedInput<Value>>) -> Self {
        self.input = Some(input.into());
        self
    }

    pub fn output_writer(mut self, output_writer: OutputWriter<CausalValue<D>>) -> Self {
        self.output_writer = Some(output_writer);
        self
    }
}

impl CausalRuntimeBuilder<CausalSet> {
    pub fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output_writer: None,
        }
    }

    /// Construct the default reference causal runtime.
    pub async fn build(
        self,
    ) -> anyhow::Result<SemiSyncRuntime<CausalSemiSyncConfig<CausalSet>, CausalDsrvSemantics>> {
        build_unchecked::<CausalSet, CausalDsrvSemantics>(self).await
    }
}

impl Default for CausalRuntimeBuilder<CausalSet> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: RoleCausalDomain> CausalRuntimeBuilder<D> {
    pub fn role_new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output_writer: None,
        }
    }

    /// Construct the causal runtime, annotating input from the supplied model.
    pub async fn build(
        self,
    ) -> anyhow::Result<SemiSyncRuntime<CausalSemiSyncConfig<D>, RoleCausalDsrvSemantics<D>>> {
        build_unchecked::<D, RoleCausalDsrvSemantics<D>>(self).await
    }
}

impl<D: RoleCausalDomain> Default for CausalRuntimeBuilder<D> {
    fn default() -> Self {
        Self::role_new()
    }
}

async fn build_unchecked<D, MS>(
    builder: CausalRuntimeBuilder<D>,
) -> anyhow::Result<SemiSyncRuntime<CausalSemiSyncConfig<D>, MS>>
where
    D: CausalDomain,
    MS: crate::semantics::MonitoringSemantics<CausalSemiSyncConfig<D>>,
{
    let CausalRuntimeBuilder {
        executor,
        model,
        input,
        output_writer,
    } = builder;
    let executor =
        executor.ok_or_else(|| anyhow::anyhow!("causal runtime executor was not configured"))?;
    let model = model.ok_or_else(|| anyhow::anyhow!("causal runtime model was not configured"))?;
    let input = input.ok_or_else(|| anyhow::anyhow!("causal runtime input was not configured"))?;
    let output_writer = output_writer
        .ok_or_else(|| anyhow::anyhow!("causal runtime output writer was not configured"))?;
    let declared_inputs = model.input_vars().clone();
    let input = input.map_values(|input| super::annotate_input::<D>(input, declared_inputs));

    Ok(SemiSyncRuntimeBuilder::<CausalSemiSyncConfig<D>, MS>::new()
        .executor(executor)
        .model(model)
        .input(input)
        .output_writer(output_writer)
        .build()
        .await)
}

/// Checked model-owned causal runtime builder.
pub struct CheckedCausalRuntimeBuilder<D: CausalDomain = CausalSet> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<CheckedDsrvSpecification>,
    input: Option<OpenedInput<Value>>,
    output_writer: Option<OutputWriter<CausalValue<D>>>,
}

impl<D: CausalDomain> CheckedCausalRuntimeBuilder<D> {
    pub fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn model(mut self, model: CheckedDsrvSpecification) -> Self {
        self.model = Some(model);
        self
    }

    pub fn input(mut self, input: impl Into<OpenedInput<Value>>) -> Self {
        self.input = Some(input.into());
        self
    }

    pub fn output_writer(mut self, output_writer: OutputWriter<CausalValue<D>>) -> Self {
        self.output_writer = Some(output_writer);
        self
    }
}

impl CheckedCausalRuntimeBuilder<CausalSet> {
    pub fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output_writer: None,
        }
    }

    pub async fn build(
        self,
    ) -> anyhow::Result<SemiSyncRuntime<CausalCheckedSemiSyncConfig<CausalSet>, CausalDsrvSemantics>>
    {
        build_checked::<CausalSet, CausalDsrvSemantics>(self).await
    }
}

impl Default for CheckedCausalRuntimeBuilder<CausalSet> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: RoleCausalDomain> CheckedCausalRuntimeBuilder<D> {
    pub fn role_new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output_writer: None,
        }
    }

    pub async fn build(
        self,
    ) -> anyhow::Result<SemiSyncRuntime<CausalCheckedSemiSyncConfig<D>, RoleCausalDsrvSemantics<D>>>
    {
        build_checked::<D, RoleCausalDsrvSemantics<D>>(self).await
    }
}

impl Default for CheckedCausalRuntimeBuilder<RoleCausalSet> {
    fn default() -> Self {
        Self::role_new()
    }
}

impl Default for CheckedCausalRuntimeBuilder<RoleCausalAntichain> {
    fn default() -> Self {
        Self::role_new()
    }
}

async fn build_checked<D, MS>(
    builder: CheckedCausalRuntimeBuilder<D>,
) -> anyhow::Result<SemiSyncRuntime<CausalCheckedSemiSyncConfig<D>, MS>>
where
    D: CausalDomain,
    MS: crate::semantics::MonitoringSemantics<CausalCheckedSemiSyncConfig<D>>,
{
    let CheckedCausalRuntimeBuilder {
        executor,
        model,
        input,
        output_writer,
    } = builder;
    let executor = executor
        .ok_or_else(|| anyhow::anyhow!("checked causal runtime executor was not configured"))?;
    let model =
        model.ok_or_else(|| anyhow::anyhow!("checked causal runtime model was not configured"))?;
    let input =
        input.ok_or_else(|| anyhow::anyhow!("checked causal runtime input was not configured"))?;
    let output_writer = output_writer.ok_or_else(|| {
        anyhow::anyhow!("checked causal runtime output writer was not configured")
    })?;
    let declared_inputs = model.input_vars().clone();
    let input = input.map_values(|input| super::annotate_input::<D>(input, declared_inputs));

    Ok(
        SemiSyncRuntimeBuilder::<CausalCheckedSemiSyncConfig<D>, MS>::new()
            .executor(executor)
            .model(model)
            .input(input)
            .output_writer(output_writer)
            .build()
            .await,
    )
}
