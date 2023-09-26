use anyhow::{anyhow, Result};
use core::future::Future;
use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt};
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::{Context, Poll};

type OnceFutPending<T> = Shared<BoxFuture<'static, SharedResult<Arc<T>>>>;
pub type SharedResult<T> = core::result::Result<T, Arc<anyhow::Error>>;

enum OnceFutState<T> {
    Pending(OnceFutPending<T>),
    Ready(SharedResult<Arc<T>>),
}

impl<T> Clone for OnceFutState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending(p) => Self::Pending(p.clone()),
            Self::Ready(r) => Self::Ready(r.clone()),
        }
    }
}

pub struct OnceFut<T> {
    state: OnceFutState<T>,
}

impl<T> Clone for OnceFut<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T: 'static> OnceFut<T> {
    /// convert a future into a OnceFut(i.e,shared future)
    pub fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let shared_future = fut
            .map(|res| res.map(Arc::new).map_err(Arc::new))
            .boxed()
            .shared();
        Self {
            state: OnceFutState::Pending(shared_future),
        }
    }

    /// get the computation result of shared future
    pub fn get(&mut self, cx: &mut Context<'_>) -> Poll<Result<&T>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        };
        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.as_ref()
                    .map(|r| r.as_ref())
                    .map_err(|e| anyhow!(format!("error fetching poll result:{:?}", e))),
            ),
        }
    }
}

pub struct OnceAsync<T> {
    fut: Mutex<Option<OnceFut<T>>>,
}

impl<T> Default for OnceAsync<T> {
    fn default() -> Self {
        Self {
            fut: Mutex::new(None),
        }
    }
}

impl<T> std::fmt::Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnceAsync")
    }
}
impl<T: 'static> OnceAsync<T> {
    pub fn once<F, Fut>(&self, f: F) -> OnceFut<T>
    where   F: FnOnce()->Fut,
            Fut: Future<Output=Result<T>> + Send + 'static {
        self.fut.lock().get_or_insert_with(||OnceFut::new(f())).clone()
    }
}
