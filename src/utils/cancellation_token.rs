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

    pub async fn is_cancelled(&self) -> bool {
        self.cancelled.get().await
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

impl DropGuard {
    /// Conceptually, a weak reference to the DropGuard.
    /// Allows using the token but does not extend its lifetime.
    pub fn clone_tok(&self) -> CancellationToken {
        CancellationToken {
            cancelled: self.cancelled.clone(),
        }
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.cancelled.set(true);
    }
}
