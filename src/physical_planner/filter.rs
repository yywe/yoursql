use crate::{
    common::{
        record_batch::RecordBatch,
        schema::SchemaRef,
        types::{DataType, DataValue},
    },
    physical_expr::PhysicalExpr,
    session::SessionState,
};
use anyhow::{anyhow, Result};
use futures::{Stream, StreamExt};
use std::sync::Arc;

use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};

#[derive(Debug)]
pub struct FilterExec {
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
}

impl FilterExec {
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => Ok(Self {
                predicate,
                input: input.clone(),
            }),
            other => Err(anyhow!(format!(
                "filter predicate must return bool not {other:?}"
            ))),
        }
    }
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

/// filter a single batch based on the predicate
pub fn batch_filter(batch: &RecordBatch, predicate: &Arc<dyn PhysicalExpr>) -> Result<RecordBatch> {
    let bool_vec = predicate.evaluate(batch)?;
    let filtered_rows = batch
        .rows
        .iter()
        .enumerate()
        .filter_map(|(i, ref ref_row)| match bool_vec[i] {
            DataValue::Boolean(Some(true)) => Some((*ref_row).clone()),
            _ => None,
        })
        .collect();
    Ok(RecordBatch {
        rows: filtered_rows,
        schema: batch.schema.clone(),
    })
}

/// stream implementation
struct FilterExecStream {
    schema: SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    input: SendableRecordBatchStream,
}

impl Stream for FilterExecStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll;
        // here we need a loop cause it is possible the current batch will be filtered empty
        loop {
            match self.input.poll_next_unpin(cx) {
                std::task::Poll::Ready(value) => match value {
                    // got some data here, let's do filter
                    Some(Ok(batch)) => {
                        let filtered_batch = batch_filter(&batch, &self.predicate)?;
                        // if no data for this batch after filter, continue for next loop wait
                        if filtered_batch.rows.len() == 0 {
                            continue; // continue loop
                        } else {
                            poll = std::task::Poll::Ready(Some(Ok(filtered_batch)));
                            break; // break here since got some data
                        }
                    }
                    // either None (no data) or error? then just return whatever value obtained
                    _ => {
                        poll = std::task::Poll::Ready(value);
                        break; // remember break here, return whatever value
                    }
                },
                // no data available yet, keep pending and break loop
                std::task::Poll::Pending => {
                    poll = std::task::Poll::Pending;
                    break;
                }
            }
        }
        poll
    }
}

impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ExecutionPlan for FilterExec {
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
        Ok(Arc::new(FilterExec::try_new(
            self.predicate.clone(),
            children[0].clone(),
        )?))
    }
    fn execute(&self, session_state: &SessionState) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(FilterExecStream {
            schema: self.input.schema(),
            predicate: self.predicate.clone(),
            input: self.input.execute(session_state)?,
        }))
    }
}
