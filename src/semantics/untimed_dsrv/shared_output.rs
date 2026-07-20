use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::{Rc, Weak},
    task::{Poll, Waker},
};

use crate::core::OutputStream;

/// A lazily driven stream with independently advancing subscribers.
///
/// Values are retained only until every live subscriber has consumed them.
#[derive(Clone)]
pub(super) struct SharedOutput<T> {
    state: Rc<SharedOutputState<T>>,
}

struct SharedOutputState<T> {
    source: RefCell<OutputStream<T>>,
    values: RefCell<VecDeque<T>>,
    base: Cell<usize>,
    cursors: RefCell<Vec<Weak<SharedCursor>>>,
    ended: Cell<bool>,
}

struct SharedCursor {
    position: Cell<usize>,
    waker: RefCell<Option<Waker>>,
}

impl<T: Clone + 'static> SharedOutput<T> {
    pub(super) fn new(source: OutputStream<T>) -> Self {
        Self {
            state: Rc::new(SharedOutputState {
                source: RefCell::new(source),
                values: RefCell::new(VecDeque::new()),
                base: Cell::new(0),
                cursors: RefCell::new(Vec::new()),
                ended: Cell::new(false),
            }),
        }
    }

    pub(super) fn subscribe(&self) -> OutputStream<T> {
        let position = self.state.base.get() + self.state.values.borrow().len();
        let cursor = Rc::new(SharedCursor {
            position: Cell::new(position),
            waker: RefCell::new(None),
        });
        self.state.cursors.borrow_mut().push(Rc::downgrade(&cursor));
        let state = Rc::clone(&self.state);
        Box::pin(futures::stream::poll_fn(move |cx| {
            *cursor.waker.borrow_mut() = Some(cx.waker().clone());
            let position = cursor.position.get();
            let base = state.base.get();
            if let Some(value) = position
                .checked_sub(base)
                .and_then(|index| state.values.borrow().get(index).cloned())
            {
                cursor.position.set(position + 1);
                state.discard_consumed();
                return Poll::Ready(Some(value));
            }
            if state.ended.get() {
                return Poll::Ready(None);
            }
            match state.source.borrow_mut().as_mut().poll_next(cx) {
                Poll::Ready(Some(value)) => {
                    state.values.borrow_mut().push_back(value.clone());
                    cursor.position.set(position + 1);
                    state.wake_subscribers();
                    state.discard_consumed();
                    Poll::Ready(Some(value))
                }
                Poll::Ready(None) => {
                    state.ended.set(true);
                    state.wake_subscribers();
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        }))
    }
}

impl<T> SharedOutputState<T> {
    fn wake_subscribers(&self) {
        self.cursors.borrow_mut().retain(|cursor| {
            let Some(cursor) = cursor.upgrade() else {
                return false;
            };
            if let Some(waker) = cursor.waker.borrow_mut().take() {
                waker.wake();
            }
            true
        });
    }

    fn discard_consumed(&self) {
        let mut cursors = self.cursors.borrow_mut();
        cursors.retain(|cursor| cursor.strong_count() != 0);
        let consumed_to = cursors
            .iter()
            .filter_map(Weak::upgrade)
            .map(|cursor| cursor.position.get())
            .min()
            .unwrap_or_else(|| self.base.get() + self.values.borrow().len());
        let count = consumed_to.saturating_sub(self.base.get());
        self.values.borrow_mut().drain(..count);
        self.base.set(consumed_to);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, executor::block_on, stream};

    #[test]
    fn subscribers_read_each_value_independently() {
        block_on(async {
            let shared = SharedOutput::new(Box::pin(stream::iter([1, 2, 3])));
            let mut first = shared.subscribe();
            let mut second = shared.subscribe();

            assert_eq!(first.next().await, Some(1));
            assert_eq!(second.next().await, Some(1));
            assert_eq!(first.next().await, Some(2));
            assert_eq!(second.next().await, Some(2));
            drop(second);
            assert_eq!(first.next().await, Some(3));
            assert_eq!(first.next().await, None);
        });
    }

    #[test]
    fn late_subscribers_start_at_the_current_frontier() {
        block_on(async {
            let shared = SharedOutput::new(Box::pin(stream::iter([1, 2])));
            let mut first = shared.subscribe();
            assert_eq!(first.next().await, Some(1));

            let mut second = shared.subscribe();
            assert_eq!(first.next().await, Some(2));
            assert_eq!(second.next().await, Some(2));
        });
    }
}
