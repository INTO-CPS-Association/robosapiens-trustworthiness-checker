//! Test input helpers and resource-free output backend constructors.
mod manual_input;
pub(crate) use manual_input::from_streams;

#[cfg(test)]
pub(crate) async fn manual_output<V: crate::core::StreamData>(
    variables: std::collections::BTreeSet<crate::VarName>,
) -> (
    crate::core::OutputWriter<V>,
    crate::core::OutputStream<std::collections::BTreeMap<crate::VarName, V>>,
) {
    use crate::core::{OutputBackend, OutputInterface};
    let (backend, receiver) = crate::io::output::ManualOutputBackend::<V>::channel(1024);
    let writer = backend
        .open(OutputInterface::outputs(variables).expect("test output interface is valid"))
        .await
        .expect("manual output backend opens");
    let stream = Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|row| (row, receiver)) },
    ));
    (writer, stream)
}

#[cfg(test)]
pub(crate) async fn null_output<V: crate::core::StreamData>(
    variables: std::collections::BTreeSet<crate::VarName>,
) -> crate::core::OutputWriter<V> {
    use crate::core::{OutputBackend, OutputInterface};
    crate::io::output::NullOutputBackend::<V>::new()
        .open(OutputInterface::outputs(variables).expect("test output interface is valid"))
        .await
        .expect("null output backend opens")
}

#[cfg(test)]
pub(crate) async fn limited_null_output<V: crate::core::StreamData>(
    variables: std::collections::BTreeSet<crate::VarName>,
    limit: usize,
) -> crate::core::OutputWriter<V> {
    use crate::core::{OutputBackend, OutputInterface};
    crate::io::output::LimitedNullOutputBackend::<V>::new(limit)
        .open(OutputInterface::outputs(variables).expect("test output interface is valid"))
        .await
        .expect("limited null output backend opens")
}
pub use manual_input::{ManualInputController, channel};

/// Construct a reusable manual step source for tests and benchmarks.
pub fn input_source(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::manual(fanouts)
}

/// Construct a manual step source with a separate control-plane fanout.
pub fn input_source_with_control(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
    control: std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::manual_with_control(fanouts, Some(control))
}
