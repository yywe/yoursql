use crate::{
    common::{
        record_batch::RecordBatch,
        schema::{Schema, SchemaRef},
    },
    physical_planner::utils::{collect_batch_stream, OnceAsync, OnceFut},
    session::SessionState,
};
use anyhow::Result;
use async_trait::async_trait;
use futures::{ready, Stream, StreamExt};
use std::{sync::Arc, task::Poll};

use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};

#[derive(Debug)]
pub struct CrossJoinExec {
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    left_fut: OnceAsync<RecordBatch>,
}

impl CrossJoinExec {
    pub fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let schema = left.schema().join(right.schema().as_ref())?;
        Ok(CrossJoinExec {
            left: left,
            right: right,
            schema: Arc::new(schema),
            left_fut: Default::default(),
        })
    }
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

struct CrossJoinStream {
    schema: Arc<Schema>,
    left_fut: OnceFut<RecordBatch>,
    right: SendableRecordBatchStream,
    left_index: usize,
    right_batch: Arc<parking_lot::Mutex<Option<RecordBatch>>>,
}

impl RecordBatchStream for CrossJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for CrossJoinStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

fn build_batch(
    left_index: usize,
    batch: &RecordBatch,
    left_data: &RecordBatch,
    schema: &Schema,
) -> Result<RecordBatch> {
    let left_row = left_data.rows[left_index].clone();
    let joined_rows: Vec<_> = batch
        .rows
        .iter()
        .map(|row| {
            let mut new_row = left_row.clone();
            new_row.extend(row.clone());
            new_row
        })
        .collect();
    Ok(RecordBatch {
        schema: Arc::new(schema.clone()),
        rows: joined_rows,
    })
}

impl CrossJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // first get all left data
        let left_data = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        if left_data.rows.len() == 0 {
            return Poll::Ready(None);
        }
        // got right batch, continue loop every record in left batch
        // when done, left_index = left_rows and if will break
        if self.left_index > 0 && self.left_index < left_data.rows.len() {
            let right_batch = {
                let right_batch = self.right_batch.lock();
                right_batch.clone().unwrap()
            };
            let result = build_batch(self.left_index, &right_batch, left_data, &self.schema);
            self.left_index += 1;
            return Poll::Ready(Some(result));
        } else {
            self.left_index = 0;
            self.right
                .poll_next_unpin(cx)
                .map(|maybe_batch| match maybe_batch {
                    Some(Ok(batch)) => {
                        let result = build_batch(self.left_index, &batch, left_data, &self.schema);
                        self.left_index = 1;
                        // save the right batch for loop of left index
                        let mut right_batch = self.right_batch.lock();
                        *right_batch = Some(batch);
                        Some(result)
                    }
                    other => other,
                })
        }
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CrossJoinExec::new(
            children[0].clone(),
            children[1].clone(),
        )?))
    }
    fn execute(&self, state: &SessionState) -> Result<SendableRecordBatchStream> {
        let stream = self.right.execute(state)?;
        let state_cloned = state.clone();
        let left_fut = self
            .left_fut
            .once(|| collect_batch_stream(self.left.clone(), state_cloned));
        Ok(Box::pin(CrossJoinStream {
            schema: self.schema.clone(),
            left_fut,
            right: stream,
            right_batch: Arc::new(parking_lot::Mutex::new(None)),
            left_index: 0,
        }))
    }
}
