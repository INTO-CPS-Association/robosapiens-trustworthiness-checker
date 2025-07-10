use std::rc::Rc;

use async_cell::unsync::AsyncCell;
use futures::future::LocalBoxFuture;

#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Rc<AsyncCell<bool>>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Rc::new(AsyncCell::new_with(false)),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.try_get().unwrap_or(false)
    }

    pub fn cancel(&self) {
        self.cancelled.set(true);
    }

    pub fn drop_guard(&self) -> DropGuard {
        DropGuard {
            cancelled: self.cancelled.clone(),
        }
    }

    pub fn cancelled(&self) -> LocalBoxFuture<'static, ()> {
        let cancelled = self.cancelled.clone();
        Box::pin(async move {
            while !cancelled.get().await {
                smol::future::yield_now().await;
            }
        })
    }
}

pub struct DropGuard {
    cancelled: Rc<AsyncCell<bool>>,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.cancelled.set(true);
    }
}
