pub mod empty;
pub mod memory;

use crate::common::schema::SchemaRef;
use crate::common::{record_batch::RecordBatch};
use futures::Stream;
use anyhow::Result;
use std::pin::Pin;
use std::fmt::Debug;
use std::any::Any;
use futures::StreamExt;

/// note the item is Result of RecordBatch
pub trait RecordBatchStream: Stream<Item=Result<RecordBatch>>{
    fn schema(&self) -> SchemaRef;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub trait ExecutionPlan: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<SendableRecordBatchStream>;
}

pub async fn print_batch_stream(mut rs: Pin<Box<dyn RecordBatchStream + Send>>) -> Result<()> {
    let header = rs.schema().all_fields().iter().map(|f|(*f).clone()).collect::<Vec<_>>();
    println!("{}", header.iter().map(|f|f.name().as_str()).collect::<Vec<_>>().join("|"));
    while let Some(result) = rs.next().await {
        if let Ok(batch) = result{
            for row in batch.rows {
                println!("{}", row.iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"))
            }
        }else{
            println!("error occured while fetching next result")
        }
    }
    Ok(())
}