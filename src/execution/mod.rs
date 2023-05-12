mod source;
mod query;
use crate::storage::Row;
use crate::storage::Storage;
use std::sync::Arc;
use anyhow::Error;
use futures_async_stream::try_stream;


const MAX_BATCH_SIZE: usize = 2;

// column def in result is very simple, even column name is optional
#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
}
pub type Columns = Vec<Column>;

#[derive(Debug)]
pub enum ResultBatch {
    Query {
        columns: Columns,
        rows: Vec<Row>,
    }
}


pub trait Executor<T: Storage> {
    // try_stream here only allow a single life time bound, if use &self will 
    // introduce another bound beside &mut T
    // here use Box<Self> will sovle it, or make store as Arc<T>
    // that is why later for each operator its new method return a Boxed object
    #[try_stream(boxed, ok=ResultBatch, error = Error)]
    async fn execute(self: Box<Self>, store: &mut T);
}
