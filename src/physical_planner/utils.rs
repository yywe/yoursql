use core::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Result};
use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt, StreamExt};
use parking_lot::Mutex;

use super::ExecutionPlan;
use crate::common::record_batch::RecordBatch;
use crate::common::types::DataValue;
use crate::session::SessionState;

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
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fut
            .lock()
            .get_or_insert_with(|| OnceFut::new(f()))
            .clone()
    }
}

pub async fn collect_batch_stream(
    plan: Arc<dyn ExecutionPlan>,
    state: SessionState,
) -> Result<RecordBatch> {
    let stream = plan.execute(&state)?;
    let batches: Vec<Result<RecordBatch>> = stream.collect().await;
    let mut merged_rows = Vec::new();
    for item in batches {
        merged_rows.extend(item?.rows);
    }
    Ok(RecordBatch {
        schema: plan.schema(),
        rows: merged_rows,
    })
}

pub fn transpose_matrix(m: Vec<Vec<DataValue>>) -> Result<Vec<Vec<DataValue>>> {
    let n_col = m.len();
    if n_col <= 0 {
        return Err(anyhow!("invalid number of columns {}", n_col));
    }
    let n_row = m[0].len();
    if n_row <= 0 {
        return Err(anyhow!("invalid number of rows {}", n_row));
    }
    Ok((0..n_row)
        .map(|i| (0..n_col).map(|c| m[c][i].clone()).collect())
        .collect())
}
