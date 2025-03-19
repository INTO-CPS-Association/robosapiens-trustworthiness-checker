use std::rc::Rc;

use async_stream::stream;
use futures::{
    FutureExt, StreamExt,
    stream::{self, LocalBoxStream},
};
// use tokio::sync::oneshot;
use async_unsync::oneshot;
use tokio_util::sync::DropGuard;

/* Converts a `oneshot::Receiver` of an `OutputStream` into an `OutputStream`.
 * Is done by first waiting for the oneshot to resolve to an OutputStream and
 * then continuously yielding the values from the stream. This is implemented
 * using the `flatten_stream` combinator from the `futures` crate, which
 * is essentially a general version of this function (except for handling the
 * case where the oneshot resolves to an error due to the sender going away).
 */
pub fn oneshot_to_stream<T: 'static>(
    receiver: oneshot::Receiver<LocalBoxStream<'static, T>>,
) -> LocalBoxStream<'static, T> {
    let empty_stream = Box::pin(stream::empty());
    Box::pin(
        receiver
            .map(|res| res.unwrap_or(empty_stream))
            .flatten_stream(),
    )
}

/* Wrap a stream in a drop guard to ensure that the associated cancellation
 * token is not dropped before the stream has completed or been dropped.
 * This is used for automatic cleanup of background tasks when all consumers
 * of an output stream have gone away. */
pub fn drop_guard_stream<T: 'static>(
    stream: LocalBoxStream<'static, T>,
    drop_guard: Rc<DropGuard>,
) -> LocalBoxStream<'static, T> {
    Box::pin(stream! {
        // Keep the shared reference to drop_guard alive until the stream
        // is done
        let _drop_guard = drop_guard.clone();
        let mut stream = stream;
        while let Some(val) = stream.next().await {
            yield val;
        }
    })
}
