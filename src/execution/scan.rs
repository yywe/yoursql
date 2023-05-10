use crate::{plan::Expression, storage::Storage};
use crate::execution::Executor;
use super::ResultBatch;
use super::Column;
use crate::error::ExecuteError;
use anyhow::Error;
use futures_async_stream::try_stream;
use std::any::Any;
use std::sync::Arc;
use crate::execution::Batch;

pub struct Scan {
    table: String,
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self {table, filter})
    }
}

impl<T: Storage+'static> Executor<T> for Scan {
    #[try_stream(boxed, ok=ResultBatch, error = Box<dyn std::error::Error>)]
    async fn execute(&self, store: Arc<T>){
        let tbl = store.get_table_def(self.table.as_str()).await?;
        let mut dbscan = store.scan_table(self.table.as_str()).await?;
        while let Some(data) = dbscan.next(){
            let rows = data?;
            yield ResultBatch::Query{
                columns: tbl.columns.iter().map(|c|Column{name: Some(c.name.clone())}).collect(),
                rows: rows,
            }
        }   
    }
}