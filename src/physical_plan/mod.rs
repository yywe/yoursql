
pub mod empty;
pub mod memory;

use crate::common::{record_batch::RecordBatch, types::Fields};
use futures::Stream;
use anyhow::Result;
use std::pin::Pin;
use std::fmt::Debug;
use std::any::Any;
pub trait RecordBatchStream: Stream<Item=Result<RecordBatch>>{
    fn header(&self) -> Fields;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub trait ExecutionPlan: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn header(&self) -> Fields;
    fn execute(&self) -> Result<SendableRecordBatchStream>;
}