use crate::storage::Batch;
use crate::storage::Storage;
use anyhow::Result;


// column def in result is very simple, even column name is optional
#[derive(Debug)]
pub struct Column {
    pub name: Option<String>,
}
pub type Columns = Vec<Column>;

pub enum ResultSet {
    Query {
        columns: Columns,
        rows: Batch,
    }
}

pub trait Executor<T: Storage> {
    fn execute(&self, store: &T) -> Result<ResultSet>;
}