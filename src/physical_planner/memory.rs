use super::RecordBatchStream;
use crate::common::record_batch::RecordBatch;
use crate::common::schema::SchemaRef;
use crate::physical_planner::ExecutionPlan;
use crate::physical_planner::SendableRecordBatchStream;
use crate::session::SessionState;
use anyhow::{anyhow, Result};
use futures::Stream;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[allow(dead_code)]
#[derive(Debug)]
pub struct MemoryExec {
    table: SchemaRef,
    batches: Vec<RecordBatch>,

    // note here the header is projected, while the data/header stored in RecordBatch is not yet
    // the data will be projected when do the execution
    projected_table: SchemaRef,
    projection_indices: Option<Vec<usize>>,
}

impl MemoryExec {
    pub fn try_new(
        table: SchemaRef,
        data: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_table = match projection.clone() {
            Some(indices) => Arc::new(table.project(&indices)?),
            None => table.clone(),
        };
        Ok(Self {
            table: table,
            batches: data,
            projected_table: projected_table,
            projection_indices: projection,
        })
    }
}
impl ExecutionPlan for MemoryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_table.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(anyhow!("children cannot be replaced for MemoryExec"))
    }
    fn execute(&self, _state: &SessionState) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.batches.clone(),
            self.projected_table.clone(),
            self.projection_indices.clone(),
        )?))
    }
}

/// again in MemoryStream, the header is the final/projected header, while data is not projected yet
pub struct MemoryStream {
    data: Vec<RecordBatch>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    index: usize,
}

impl MemoryStream {
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            let batch = match self.projection.as_ref() {
                Some(indices) => batch.project(indices)?,
                None => batch.clone(),
            };
            Some(Ok(batch))
        } else {
            None
        })
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
