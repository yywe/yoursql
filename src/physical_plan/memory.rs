use super::RecordBatchStream;
use crate::common::types::SchemaRef;
use crate::common::{record_batch::RecordBatch, types::Fields};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::SendableRecordBatchStream;
use anyhow::Result;
use futures::Stream;
use std::any::Any;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct MemoryExec {
    table: SchemaRef,
    batches: Vec<RecordBatch>,

    // note here the header is projected, while the data/header stored in RecordBatch is not yet
    // the data will be projected when do the execution
    projected_header: Fields,
    projection_indices: Option<Vec<usize>>,
}

impl MemoryExec {
    pub fn try_new(
        table: SchemaRef,
        data: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_header = match projection.clone() {
            Some(indices) => table.fields.project(&indices)?,
            None => table.fields.clone(),
        };
        Ok(Self {
            table: table,
            batches: data,
            projected_header: projected_header,
            projection_indices: projection,
        })
    }
}
impl ExecutionPlan for MemoryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn header(&self) -> Fields {
        self.projected_header.clone()
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.batches.clone(),
            self.header(),
            self.projection_indices.clone(),
        )?))
    }
}

/// again in MemoryStream, the header is the final/projected header, while data is not projected yet
pub struct MemoryStream {
    data: Vec<RecordBatch>,
    header: Fields,
    projection: Option<Vec<usize>>,
    index: usize,
}

impl MemoryStream {
    pub fn try_new(
        data: Vec<RecordBatch>,
        header: Fields,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            header,
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
    fn header(&self) -> Fields {
        self.header.clone()
    }
}
