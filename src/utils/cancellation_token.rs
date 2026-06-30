use std::{cell::Cell, rc::Rc};

use event_listener::Event;
use futures::future::LocalBoxFuture;

#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Rc<Cell<bool>>,
    event: Rc<Event>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Rc::new(Cell::new(false)),
            event: Rc::new(Event::new()),
        }
    }

    pub async fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }

    pub fn cancel(&self) {
        if !self.cancelled.replace(true) {
            self.event.notify(usize::MAX);
        }
    }

    pub fn drop_guard(&self) -> DropGuard {
        DropGuard {
            cancelled: self.cancelled.clone(),
            event: self.event.clone(),
        }
    }

    pub fn cancelled(&self) -> LocalBoxFuture<'static, ()> {
        let cancelled = self.cancelled.clone();
        let event = self.event.clone();
        Box::pin(async move {
            loop {
                if cancelled.get() {
                    return;
                }
                let listener = event.listen();
                if cancelled.get() {
                    return;
                }
                listener.await;
            }
        })
    }
}

pub struct DropGuard {
    cancelled: Rc<Cell<bool>>,
    event: Rc<Event>,
}

impl DropGuard {
    /// Conceptually, a weak reference to the DropGuard.
    /// Allows using the token but does not extend its lifetime.
    pub fn clone_tok(&self) -> CancellationToken {
        CancellationToken {
            cancelled: self.cancelled.clone(),
            event: self.event.clone(),
        }
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        if !self.cancelled.replace(true) {
            self.event.notify(usize::MAX);
        }
    }
}
