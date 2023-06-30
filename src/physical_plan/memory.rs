use crate::common::{record_batch::RecordBatch, types::Fields};
use anyhow::Result;
use futures::Stream;
use std::task::{Context, Poll};
use std::pin::Pin;

use super::RecordBatchStream;
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
    fn poll_next(
        mut self: Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(
            if self.index < self.data.len() {
                self.index +=1;
                let batch = &self.data[self.index - 1];
                let batch = match self.projection.as_ref(){
                    Some(indices)=>batch.project(indices)?,
                    None=>batch.clone(),
                };
                Some(Ok(batch))
            }else{
                None
            }
        )
    }
}

impl RecordBatchStream for MemoryStream {
    fn header(&self) -> Fields {
        self.header.clone()
    }
}