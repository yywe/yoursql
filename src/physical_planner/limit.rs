use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::{
    common::{record_batch::RecordBatch, schema::SchemaRef},
    physical_planner::ExecutionPlan,
    session::SessionState,
};
use anyhow::Result;
use futures::{Stream, StreamExt};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
}

impl LimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, skip: usize, fetch: Option<usize>) -> Self {
        Self { input, skip, fetch }
    }
}

struct LimitStream {
    skip: usize,
    fetch: usize,
    input: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
}

impl LimitStream {
    fn new(input: SendableRecordBatchStream, skip: usize, fetch: Option<usize>) -> Self {
        let schema = input.schema();
        Self {
            skip,
            fetch: fetch.unwrap_or(usize::MAX),
            input: Some(input),
            schema,
        }
    }
    //loop to get from the stream until reach skip and get the first batch of data
    fn poll_and_skip(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let input = self.input.as_mut().unwrap();
        loop {
            let poll = input.poll_next_unpin(cx);
            let poll = poll.map_ok(|batch| {
                if batch.rows.len() <= self.skip {
                    self.skip -= batch.rows.len();
                    // create an empty batch if this batch should be skipped
                    RecordBatch {
                        rows: vec![],
                        schema: input.schema().clone(),
                    }
                } else {
                    // a part of this batch (from skip to batch row len - skip) should be returned
                    let new_batch = RecordBatch {
                        rows: batch.rows[self.skip..].to_vec(),
                        schema: input.schema().clone(),
                    };
                    self.skip = 0; // set skip = 0 cause we started get data
                    new_batch
                }
            });
            match &poll {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.rows.len() > 0 && self.skip == 0 {
                        break poll; // mean we already get some data
                    } else {
                        //continue poll the stream until get some data
                    }
                }
                Poll::Ready(Some(Err(_e))) => break poll,
                Poll::Ready(None) => break poll,
                Poll::Pending => break poll,
            }
        }
    }

    /// get data from the stream until hit limit
    fn stream_limit(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        if self.fetch == 0 {
            self.input = None;
            None
        } else if batch.rows.len() < self.fetch {
            //the whole batch is needed
            self.fetch -= batch.rows.len();
            Some(batch)
        } else {
            //the batch only has a subset of rows needed. from 0..fetch
            let new_batch = RecordBatch {
                rows: batch.rows[..self.fetch].to_vec(),
                schema: batch.schema.clone(),
            };
            self.fetch = 0;
            self.input = None;
            Some(new_batch)
        }
    }
}

impl ExecutionPlan for LimitExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LimitExec::new(
            children[0].clone(),
            self.skip,
            self.fetch,
        )))
    }
    fn execute(&self, state: &SessionState) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(state)?;
        Ok(Box::pin(LimitStream::new(
            input_stream,
            self.skip,
            self.fetch,
        )))
    }
}

impl Stream for LimitStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let fetch_started = self.skip == 0;
        let poll = match &mut self.input {
            Some(input) => {
                // get the first poll with data
                let poll = if fetch_started {
                    input.poll_next_unpin(cx)
                } else {
                    self.poll_and_skip(cx) // here will loop until get first batch of data
                };
                poll.map(|x| match x {
                    Some(Ok(batch)) => Ok(self.stream_limit(batch)).transpose(), // limit the batch through map
                    other => other,
                })
            }
            None => Poll::Ready(None), //input has been cleared, done
        };
        poll
    }
}

impl RecordBatchStream for LimitStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
