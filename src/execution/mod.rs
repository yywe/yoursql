
mod scan;
use crate::storage::Batch;
use crate::storage::Row;
use crate::storage::Storage;
use crate::error::ExecuteError;
use std::sync::Arc;
use anyhow::Error;
use futures_async_stream::try_stream;



// column def in result is very simple, even column name is optional
#[derive(Debug)]
pub struct Column {
    pub name: Option<String>,
}
pub type Columns = Vec<Column>;

pub enum ResultBatch {
    Query {
        columns: Columns,
        rows: Vec<Row>,
    }
}


pub trait Executor<T: Storage> {
    #[try_stream(boxed, ok=ResultBatch, error = Box<dyn std::error::Error>)]
    async fn execute(&self, store: Arc<T>);
}
