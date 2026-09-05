use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    task::Poll,
};

use async_unsync::bounded;
use futures::{FutureExt, StreamExt, future::join_all};

use crate::core::{OutputBatch, OutputError, OutputInterface, OutputWriter, StreamData};
use crate::io::ReconfigurationRequest;
use crate::io::output::InterfaceSink;
use crate::io::reconfigurable_input::{ReconfigurableInputItem, ReconfigurableInputStream};
use crate::stream_utils::Fanout;
use crate::{InputBatch, InputStream, InputUpdate, LocalStream, Value, VarName};

const CHANNEL_SIZE: usize = 10;

pub struct ChannelInputController<V> {
    sender: bounded::Sender<InputBatch<V>>,
}

impl<V> ChannelInputController<V> {
    /// Send one validated simultaneous input tick.
    pub async fn send_tick(&mut self, updates: Vec<InputUpdate<V>>) -> anyhow::Result<()> {
        let tick = InputBatch::tick(updates)?;
        self.sender
            .send(tick)
            .await
            .map_err(|_| anyhow::anyhow!("channel input stopped"))
    }
}

/// Create a channel driven input stream and its controller.
pub fn channel<V: 'static>() -> (crate::io::OpenedInput<V>, ChannelInputController<V>) {
    let (sender, receiver) = bounded::channel(CHANNEL_SIZE).into_split();
    let (stop, stopped) = async_channel::bounded::<()>(1);
    let mut receiver = receiver;
    let stream: InputStream<V> = Box::pin(async_stream::stream! {
        loop {
            futures::select_biased! {
                _ = stopped.recv().fuse() => break,
                batch = receiver.recv().fuse() => match batch {
                    Some(batch) => yield Ok(batch),
                    None => return,
                },
            }
        }
        receiver.close();
        loop {
            match receiver.recv().await {
                Some(batch) => yield Ok(batch),
                None => return,
            }
        }
    });
    (
        crate::io::OpenedInput::with_stop(stream, move || {
            let _ = stop.try_send(());
        }),
        ChannelInputController { sender },
    )
}

pub type ChannelOutputSender<V> = bounded::Sender<BTreeMap<VarName, V>>;
pub type ChannelOutputReceiver<V> = bounded::Receiver<BTreeMap<VarName, V>>;

/// Create a bounded output channel and return its sender and receiver.
pub fn output<V>(capacity: usize) -> (ChannelOutputSender<V>, ChannelOutputReceiver<V>) {
    bounded::channel(capacity).into_split()
}

/// Open an output writer that emits one row per logical tick to a bounded channel.
pub async fn open_output<V: StreamData>(
    sender: ChannelOutputSender<V>,
    interface: OutputInterface,
) -> Result<OutputWriter<V>, OutputError> {
    let sender = sender.clone();
    Ok(OutputWriter::from_output_sink(InterfaceSink::new(
        interface,
        move |interface, batch: OutputBatch<V>| {
            let sender = sender.clone();
            async move {
                interface.validate_batch(&batch)?;
                for tick in batch.ticks() {
                    let row = tick
                        .updates()
                        .map(|update| (update.variable.clone(), update.value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    sender.send(row).await.map_err(|_| OutputError::closed())?;
                }
                Ok(())
            }
        },
    )))
}

pub(crate) fn from_streams<V: 'static>(
    streams: BTreeMap<VarName, LocalStream<V>>,
) -> InputStream<V> {
    let mut streams = streams.into_iter().collect::<Vec<_>>();
    Box::pin(async_stream::try_stream! {
        loop {
            let values = join_all(streams.iter_mut().map(|(_, stream)| stream.next())).await;
            let updates = streams
                .iter()
                .map(|(var, _)| var.clone())
                .zip(values)
                .filter_map(|(var, value)| value.map(|value| InputUpdate::new(var, value)))
                .collect::<Vec<_>>();
            if updates.is_empty() {
                return;
            }
            yield InputBatch::tick(updates)?;
        }
    })
}

pub(crate) enum ReconfigurableChannelCommand {
    Pause(u64),
    Rebind(BTreeSet<VarName>),
    Resume,
    Stop,
}

pub(crate) struct ReconfigurableChannelControl {
    commands: bounded::Sender<ReconfigurableChannelCommand>,
}

impl ReconfigurableChannelControl {
    pub(crate) async fn pause(&self, boundary: u64) -> anyhow::Result<()> {
        self.commands
            .send(ReconfigurableChannelCommand::Pause(boundary))
            .await
            .map_err(|_| anyhow::anyhow!("channel input stopped before pause"))
    }

    pub(crate) async fn rebind(&self, variables: BTreeSet<VarName>) -> anyhow::Result<()> {
        self.commands
            .send(ReconfigurableChannelCommand::Rebind(variables))
            .await
            .map_err(|_| anyhow::anyhow!("channel input stopped during rebind"))
    }

    pub(crate) fn resume(&self) -> anyhow::Result<()> {
        match self.commands.try_send(ReconfigurableChannelCommand::Resume) {
            Ok(()) => Ok(()),
            Err(_) if !self.commands.is_closed() => Ok(()),
            Err(_) => Err(anyhow::anyhow!("channel input stopped before resume")),
        }
    }

    pub(crate) async fn shutdown(&self) {
        let _ = self.commands.send(ReconfigurableChannelCommand::Stop).await;
    }
}

pub(crate) fn reconfigurable_stream<V: Clone + 'static>(
    fanouts: BTreeMap<VarName, Rc<Fanout<V>>>,
    active: BTreeSet<VarName>,
    control: Rc<Fanout<Value>>,
) -> (ReconfigurableInputStream<V>, ReconfigurableChannelControl) {
    let (commands, mut command_receiver) = bounded::channel(1).into_split();
    let mut control_receiver = Some(control.subscribe());
    let mut receivers = active
        .into_iter()
        .filter_map(|variable| {
            fanouts
                .get(&variable)
                .map(|fanout| (variable, fanout.subscribe()))
        })
        .collect::<BTreeMap<_, _>>();
    let stream = Box::pin(async_stream::try_stream! {
        let mut pending = BTreeMap::<VarName, V>::new();
        let mut paused = false;
        loop {
            enum Next<V> {
                Command(Option<ReconfigurableChannelCommand>),
                Control(Option<Value>),
                Row(Vec<InputUpdate<V>>),
                Complete,
            }
            let next = if paused {
                Next::Command(command_receiver.recv().await)
            } else {
                futures::future::poll_fn(|cx| {
                    if let Poll::Ready(command) = command_receiver.poll_recv(cx) {
                        return Poll::Ready(Next::Command(command));
                    }
                    if let Some(control) = control_receiver.as_mut() {
                        if let Poll::Ready(value) = control.poll_recv(cx) {
                            return Poll::Ready(Next::Control(value));
                        }
                    }
                    let mut ended = Vec::new();
                    for (variable, receiver) in &mut receivers {
                        if pending.contains_key(variable) { continue; }
                        match receiver.poll_recv(cx) {
                            Poll::Ready(Some(value)) => { pending.insert(variable.clone(), value); }
                            Poll::Ready(None) => ended.push(variable.clone()),
                            Poll::Pending => {}
                        }
                    }
                    for variable in ended { receivers.remove(&variable); pending.remove(&variable); }
                    if !receivers.is_empty() && receivers.keys().all(|variable| pending.contains_key(variable)) {
                        return Poll::Ready(Next::Row(receivers.keys().map(|variable| InputUpdate::new(variable.clone(), pending.remove(variable).expect("complete channel row"))).collect()));
                    }
                    if receivers.is_empty() && control_receiver.is_none() { Poll::Ready(Next::Complete) } else { Poll::Pending }
                }).await
            };
            match next {
                Next::Command(Some(ReconfigurableChannelCommand::Pause(id))) => {
                    paused = true;
                    let mut snapshots = receivers.iter().map(|(variable, receiver)| (variable.clone(), receiver.len())).collect::<BTreeMap<_, _>>();
                    let mut ready_rows = Vec::new();
                    loop {
                        for (variable, receiver) in &mut receivers {
                            if pending.contains_key(variable) { continue; }
                            let remaining = snapshots.get_mut(variable).expect("snapshot for active channel receiver");
                            if *remaining == 0 { continue; }
                            *remaining -= 1;
                            if let Ok(value) = receiver.try_recv() { pending.insert(variable.clone(), value); }
                        }
                        if receivers.is_empty() || !receivers.keys().all(|variable| pending.contains_key(variable)) { break; }
                        ready_rows.push(receivers.keys().map(|variable| InputUpdate::new(variable.clone(), pending.remove(variable).expect("complete queued channel row"))).collect());
                    }
                    for updates in ready_rows {
                        yield ReconfigurableInputItem::Data(InputBatch::tick(updates)?);
                    }
                    yield ReconfigurableInputItem::Boundary(id);
                }
                Next::Command(Some(ReconfigurableChannelCommand::Rebind(next))) => {
                    receivers.retain(|variable, _| next.contains(variable));
                    pending.retain(|variable, _| next.contains(variable));
                    for variable in next { if !receivers.contains_key(&variable) { let fanout = fanouts.get(&variable).ok_or_else(|| anyhow::anyhow!("channel input has no stream for `{variable}`"))?; receivers.insert(variable, fanout.subscribe()); } }
                    paused = false;
                }
                Next::Command(Some(ReconfigurableChannelCommand::Resume)) => paused = false,
                Next::Command(Some(ReconfigurableChannelCommand::Stop)) | Next::Command(None) | Next::Complete => return,
                Next::Control(Some(Value::NoVal)) => {}
                Next::Control(Some(Value::Str(payload))) => yield ReconfigurableInputItem::Reconfigure(ReconfigurationRequest::from_json(payload.as_str())?),
                Next::Control(Some(other)) => Err(anyhow::anyhow!("channel reconfiguration payload must be a string, got {other:?}"))?,
                Next::Control(None) => control_receiver = None,
                Next::Row(updates) => yield ReconfigurableInputItem::Data(InputBatch::tick(updates)?),
            }
        }
    });
    (stream, ReconfigurableChannelControl { commands })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_input_delivers_simultaneous_steps() {
        smol::block_on(async {
            // The controller channel carries typed steps, so the stream is a
            // `InputStream` of one simultaneous tick per sent step.
            let (mut stream, mut controller) = channel::<i32>();
            controller
                .send_tick(vec![
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("y".into(), 2),
                ])
                .await
                .unwrap();

            let batch = stream.next().await.unwrap().unwrap();
            assert_eq!(batch.tick_count(), 1);
            assert_eq!(
                batch.ticks().next().unwrap().to_updates(),
                [
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("y".into(), 2),
                ]
            );
        });
    }

    #[test]
    fn shutdown_closes_admission_and_drains_queued_batches() {
        smol::block_on(async {
            let (input, mut controller) = channel::<i32>();
            controller
                .send_tick(vec![InputUpdate::new("x".into(), 1)])
                .await
                .unwrap();
            controller
                .send_tick(vec![InputUpdate::new("x".into(), 2)])
                .await
                .unwrap();

            let batches = input
                .into_drain()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
                .await;
            assert_eq!(batches.len(), 2);
            assert!(
                controller
                    .send_tick(vec![InputUpdate::new("x".into(), 3)])
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn send_tick_rejects_duplicate_variables() {
        smol::block_on(async {
            let (_stream, mut controller) = channel();
            let error = controller
                .send_tick(vec![
                    InputUpdate::new("x".into(), 1),
                    InputUpdate::new("x".into(), 2),
                ])
                .await
                .unwrap_err();

            assert!(error.to_string().contains("duplicate variable `x`"));
        });
    }

    #[test]
    fn send_tick_rejects_empty_ticks() {
        smol::block_on(async {
            let (_stream, mut controller) = channel::<()>();
            let error = controller.send_tick(Vec::new()).await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "input tick must contain at least one update"
            );
        });
    }

    #[test]
    fn reconfigurable_data_survives_control_eof() {
        smol::block_on(async {
            let (data_sender, data) = Fanout::<Value>::new();
            let (control_sender, control) = Fanout::<Value>::new();
            let (mut stream, _owner) = reconfigurable_stream(
                BTreeMap::from([(VarName::new("x"), data)]),
                BTreeSet::from([VarName::new("x")]),
                control,
            );

            data_sender.send(Value::Int(4)).await;
            drop(control_sender);
            drop(data_sender);

            let ReconfigurableInputItem::Data(batch) = stream.next().await.unwrap().unwrap() else {
                panic!("data queued before control EOF must remain live")
            };
            assert_eq!(*batch.updates().next().unwrap().value, Value::Int(4));
            assert!(stream.next().await.is_none());
        });
    }

    #[test]
    fn repeated_pause_does_not_move_asymmetric_backlog_out_of_bounded_receiver() {
        smol::block_on(async {
            let (x_sender, x) = Fanout::<Value>::new();
            let (_y_sender, y) = Fanout::<Value>::new();
            let (_control_sender, control) = Fanout::<Value>::new();
            let variables = BTreeSet::from([VarName::new("x"), VarName::new("y")]);
            let (mut stream, owner) = reconfigurable_stream(
                BTreeMap::from([(VarName::new("x"), x), (VarName::new("y"), y)]),
                variables.clone(),
                control,
            );

            for value in 0..1024 {
                x_sender.send(Value::Int(value)).await;
            }
            owner.pause(0).await.unwrap();
            assert!(matches!(
                stream.next().await.unwrap().unwrap(),
                ReconfigurableInputItem::Boundary(0)
            ));
            for boundary in 1..8 {
                owner.rebind(variables.clone()).await.unwrap();
                let (item, pause) =
                    futures::future::join(stream.next(), owner.pause(boundary)).await;
                pause.unwrap();
                assert!(matches!(
                    item.unwrap().unwrap(),
                    ReconfigurableInputItem::Boundary(id) if id == boundary
                ));
            }

            // One value occupies the single pending x slot and the rest stay
            // in the bounded fanout receiver. Only one newly freed receiver
            // slot is available; a second send must remain backpressured.
            x_sender.send(Value::Int(1024)).await;
            let blocked = Box::pin(x_sender.send(Value::Int(1025)));
            let timeout = Box::pin(smol::Timer::after(std::time::Duration::from_millis(10)));
            assert!(matches!(
                futures::future::select(blocked, timeout).await,
                futures::future::Either::Right(_)
            ));
        });
    }
}
