//! Test input helpers and resource-free output backend constructors.

#[cfg(test)]
pub(crate) async fn channel_output<V: crate::core::StreamData>(
    variables: std::collections::BTreeSet<crate::VarName>,
) -> (
    crate::core::OutputWriter<V>,
    crate::core::LocalStream<std::collections::BTreeMap<crate::VarName, V>>,
) {
    use crate::core::OutputInterface;
    let (sender, receiver) = crate::io::channel::output(1024);
    let writer = crate::io::channel::open_output(
        sender,
        OutputInterface::outputs(variables).expect("test output interface is valid"),
    )
    .await
    .expect("channel output backend opens");
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
    use crate::core::OutputInterface;
    crate::io::output::open_null::<V>(
        OutputInterface::outputs(variables).expect("test output interface is valid"),
    )
    .await
    .expect("null output backend opens")
}

#[cfg(test)]
pub(crate) async fn limited_null_output<V: crate::core::StreamData>(
    variables: std::collections::BTreeSet<crate::VarName>,
    limit: usize,
) -> crate::core::OutputWriter<V> {
    use crate::core::OutputInterface;
    crate::io::output::open_limited_null::<V>(
        limit,
        OutputInterface::outputs(variables).expect("test output interface is valid"),
    )
    .await
    .expect("limited null output backend opens")
}
/// Construct a reusable channel step source for tests and benchmarks.
pub fn input_source(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::channel(fanouts)
}

/// Construct a channel step source with a separate control-plane fanout.
pub fn input_source_with_control(
    fanouts: std::collections::BTreeMap<
        crate::VarName,
        std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
    >,
    control: std::rc::Rc<crate::stream_utils::Fanout<crate::Value>>,
) -> crate::io::InputSource {
    crate::io::InputSource::<crate::Value>::channel_with_control(fanouts, Some(control))
}
