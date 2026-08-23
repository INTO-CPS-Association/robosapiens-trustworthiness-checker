use std::collections::BTreeMap;

use futures::{StreamExt, stream::FuturesUnordered};

use crate::{OutputBatch, OutputStream, OutputUpdate, OutputWriter, VarName};

pub(crate) type NamedOutputStreams<V> = BTreeMap<VarName, OutputStream<V>>;

/// Drive independent named streams as singleton logical output ticks.
///
/// This is the native representation for asynchronous and distributed
/// monitors: whichever producer yields next becomes one width-one tick. The
/// sink remains the only output boundary.
pub(crate) async fn drive_singleton_streams<V: 'static>(
    streams: NamedOutputStreams<V>,
    writer: &mut OutputWriter<V>,
) -> anyhow::Result<()> {
    let streams =
        streams
            .into_iter()
            .map(|(variable, stream)| {
                Box::pin(stream.map(move |value| {
                    OutputBatch::from(OutputUpdate::new(variable.clone(), value))
                })) as OutputStream<OutputBatch<V>>
            })
            .collect::<Vec<_>>();
    let mut streams = futures::stream::select_all(streams);

    while let Some(batch) = streams.next().await {
        match writer.send(batch).await {
            Ok(()) => {
                // Independent streams can be permanently ready. Yield so one
                // constant stream cannot starve input driving or sibling outputs.
                smol::future::yield_now().await;
            }
            Err(error) if error.is_closed() => break,
            Err(_error) => {
                // OutputWriter retains non-closed operation errors. The
                // explicit finalizer below preserves that error and still
                // drives close after the failed send.
                break;
            }
        }
    }

    finish_writer(writer).await
}

/// Drive one value from every named stream as one simultaneous logical tick.
///
/// Semi-sync output subscriptions advance together, so the driver preserves
/// that row boundary with `OutputBatch::tick` rather than flattening values
/// into independent updates.
pub(crate) async fn drive_row_streams<V: 'static>(
    streams: NamedOutputStreams<V>,
    writer: &mut OutputWriter<V>,
) -> anyhow::Result<()> {
    let (variables, mut streams): (Vec<_>, Vec<_>) = streams.into_iter().unzip();
    if streams.is_empty() {
        return finish_writer(writer).await;
    }

    loop {
        // Poll all members of this logical row concurrently, but observe
        // completions individually. If one member ends, dropping `nexts`
        // cancels the still-pending next futures before writer cleanup.
        let row = {
            let stream_count = streams.len();
            let mut nexts = streams
                .iter_mut()
                .enumerate()
                .map(|(index, stream)| async move { (index, stream.next().await) })
                .collect::<FuturesUnordered<_>>();
            let mut values = (0..stream_count).map(|_| None).collect::<Vec<Option<V>>>();
            let mut complete = true;

            while let Some((index, value)) = nexts.next().await {
                match value {
                    Some(value) => values[index] = Some(value),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }

            complete.then_some(values)
        };
        let Some(values) = row else {
            break;
        };

        let updates = variables
            .iter()
            .zip(values.into_iter())
            .map(|(variable, value)| {
                OutputUpdate::new(
                    variable.clone(),
                    value.expect("output value exists after complete row collection"),
                )
            })
            .collect();
        let batch = match OutputBatch::tick(updates) {
            Ok(batch) => batch,
            Err(error) => return finish_writer_with_primary(writer, error.into()).await,
        };
        match writer.send(batch).await {
            Ok(()) => {}
            Err(error) if error.is_closed() => break,
            Err(_error) => {
                // OutputWriter retains non-closed operation errors. The
                // explicit finalizer below preserves that error and still
                // drives close after the failed send.
                break;
            }
        }
    }

    finish_writer(writer).await
}

pub(crate) async fn finish_writer<V: 'static>(writer: &mut OutputWriter<V>) -> anyhow::Result<()> {
    let flush_result = writer.flush().await;
    let close_result = writer.close().await;

    // OutputWriter's close result includes any retained operation error and a
    // distinct close error. Prefer it over the flush result, while still
    // falling back to a non-closed flush failure if close was already closed.
    let error = match close_result {
        Err(error) if !error.is_closed() => Some(error),
        _ => match flush_result {
            Err(error) if !error.is_closed() => Some(error),
            _ => None,
        },
    };
    error.map_or(Ok(()), |error| Err(error.into()))
}

async fn finish_writer_with_primary<V: 'static>(
    writer: &mut OutputWriter<V>,
    primary: anyhow::Error,
) -> anyhow::Result<()> {
    match finish_writer(writer).await {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(combine_errors(primary, cleanup)),
    }
}

fn combine_errors(primary: anyhow::Error, additional: anyhow::Error) -> anyhow::Error {
    let primary_message = primary.to_string();
    let additional_message = additional.to_string();
    if primary_message == additional_message {
        primary
    } else {
        anyhow::anyhow!("{primary_message}; additionally: {additional_message}")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
        time::Duration,
    };

    use futures::{Sink, stream};

    use crate::OutputError;

    use super::*;

    struct RecordingSink {
        batches: Rc<RefCell<Vec<OutputBatch<i32>>>>,
        flushes: Rc<Cell<usize>>,
        closes: Rc<Cell<usize>>,
        ready: bool,
        closed: bool,
        send_error: Option<OutputError>,
        ready_error: Option<OutputError>,
        flush_error: Option<OutputError>,
        close_error: Option<OutputError>,
    }

    impl Sink<OutputBatch<i32>> for RecordingSink {
        type Error = OutputError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            if this.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            if let Some(error) = this.ready_error.take() {
                return Poll::Ready(Err(error));
            }
            this.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, batch: OutputBatch<i32>) -> Result<(), Self::Error> {
            let this = self.get_mut();
            if this.closed {
                return Err(OutputError::Closed);
            }
            if !this.ready {
                return Err(OutputError::backend("recording sink was not ready"));
            }
            this.ready = false;
            if let Some(error) = this.send_error.take() {
                return Err(error);
            }
            this.batches.borrow_mut().push(batch);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.flushes.set(this.flushes.get() + 1);
            if this.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            match this.flush_error.take() {
                Some(error) => Poll::Ready(Err(error)),
                None => {
                    this.ready = true;
                    Poll::Ready(Ok(()))
                }
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.closes.set(this.closes.get() + 1);
            if this.closed {
                return Poll::Ready(Err(OutputError::Closed));
            }
            this.closed = true;
            Poll::Ready(match this.close_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            })
        }
    }

    fn recording_writer(
        send_error: Option<OutputError>,
        ready_error: Option<OutputError>,
        flush_error: Option<OutputError>,
        close_error: Option<OutputError>,
    ) -> (
        OutputWriter<i32>,
        Rc<RefCell<Vec<OutputBatch<i32>>>>,
        Rc<Cell<usize>>,
        Rc<Cell<usize>>,
    ) {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let flushes = Rc::new(Cell::new(0));
        let closes = Rc::new(Cell::new(0));
        let sink = RecordingSink {
            batches: Rc::clone(&batches),
            flushes: Rc::clone(&flushes),
            closes: Rc::clone(&closes),
            ready: false,
            closed: false,
            send_error,
            ready_error,
            flush_error,
            close_error,
        };
        (OutputWriter::from_sink(sink), batches, flushes, closes)
    }

    fn singleton_streams<I>(values: I) -> NamedOutputStreams<i32>
    where
        I: IntoIterator<Item = i32>,
        I::IntoIter: 'static,
    {
        BTreeMap::from([(
            VarName::new("x"),
            Box::pin(stream::iter(values)) as OutputStream<i32>,
        )])
    }

    #[test]
    fn singleton_driver_flushes_and_closes_after_normal_exhaustion() {
        let (mut writer, _, flushes, closes) = recording_writer(None, None, None, None);

        let result = smol::block_on(drive_singleton_streams(
            singleton_streams([1, 2]),
            &mut writer,
        ));

        assert!(result.is_ok());
        assert_eq!(flushes.get(), 1);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn singleton_driver_closes_after_send_failure_and_preserves_close_context() {
        let (mut writer, _, flushes, closes) = recording_writer(
            Some(OutputError::backend("send failed")),
            None,
            None,
            Some(OutputError::backend("close failed")),
        );

        let error = smol::block_on(drive_singleton_streams(singleton_streams([1]), &mut writer))
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("send failed"));
        assert!(message.contains("close failed"));
        assert_eq!(flushes.get(), 0);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn singleton_driver_closes_after_early_downstream_closure() {
        let (mut writer, _, flushes, closes) =
            recording_writer(None, Some(OutputError::Closed), None, None);

        let result = smol::block_on(drive_singleton_streams(singleton_streams([1]), &mut writer));

        assert!(result.is_ok());
        assert_eq!(flushes.get(), 0);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn row_driver_finalizes_empty_stream_map_without_emitting_a_batch() {
        let (mut writer, batches, flushes, closes) = recording_writer(None, None, None, None);

        let result = smol::block_on(drive_row_streams(BTreeMap::new(), &mut writer));

        assert!(result.is_ok());
        assert!(batches.borrow().is_empty());
        assert_eq!(flushes.get(), 1);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn row_driver_keeps_equal_rows_as_one_batch() {
        let batches = Rc::new(RefCell::new(Vec::new()));
        let flushes = Rc::new(Cell::new(0));
        let closes = Rc::new(Cell::new(0));
        let writer_sink = RecordingSink {
            batches: Rc::clone(&batches),
            flushes: Rc::clone(&flushes),
            closes: Rc::clone(&closes),
            ready: false,
            closed: false,
            send_error: None,
            ready_error: None,
            flush_error: None,
            close_error: None,
        };
        let mut writer = OutputWriter::from_sink(writer_sink);
        let streams = BTreeMap::from([
            (
                VarName::new("x"),
                Box::pin(stream::iter([1, 2])) as OutputStream<i32>,
            ),
            (
                VarName::new("y"),
                Box::pin(stream::iter([10, 20])) as OutputStream<i32>,
            ),
        ]);

        let result = smol::block_on(drive_row_streams(streams, &mut writer));

        assert!(result.is_ok());
        let recorded = batches.borrow();
        assert_eq!(recorded.len(), 2);
        assert!(
            recorded
                .iter()
                .all(|batch| { batch.tick_count() == 1 && batch.update_count() == 2 })
        );
        let rows = recorded
            .iter()
            .map(|batch| {
                batch
                    .ticks()
                    .flat_map(|tick| {
                        tick.to_updates()
                            .into_iter()
                            .map(|update| (update.variable, update.value))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec![(VarName::new("x"), 1), (VarName::new("y"), 10)],
                vec![(VarName::new("x"), 2), (VarName::new("y"), 20)],
            ]
        );
        assert_eq!(flushes.get(), 1);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn row_driver_stops_at_early_eof_and_closes_with_pending_sibling() {
        let (mut writer, _, flushes, closes) = recording_writer(None, None, None, None);
        let streams = BTreeMap::from([
            (
                VarName::new("ended"),
                Box::pin(stream::empty()) as OutputStream<i32>,
            ),
            (
                VarName::new("pending"),
                Box::pin(stream::pending()) as OutputStream<i32>,
            ),
        ]);

        let result = smol::block_on(async {
            match futures::future::select(
                Box::pin(drive_row_streams(streams, &mut writer)),
                Box::pin(async {
                    smol::Timer::after(Duration::from_secs(1)).await;
                }),
            )
            .await
            {
                futures::future::Either::Left((result, _)) => result,
                futures::future::Either::Right((_, _)) => {
                    panic!("row driver did not stop at early EOF")
                }
            }
        });

        assert!(result.is_ok());
        assert_eq!(flushes.get(), 1);
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn row_driver_surfaces_close_failure_after_flush() {
        let (mut writer, _, flushes, closes) =
            recording_writer(None, None, None, Some(OutputError::backend("close failed")));

        let error =
            smol::block_on(drive_row_streams(singleton_streams([1]), &mut writer)).unwrap_err();

        assert!(error.to_string().contains("close failed"));
        assert_eq!(flushes.get(), 1);
        assert_eq!(closes.get(), 1);
    }
}
