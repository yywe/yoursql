use crate::common::types::TableRef;
use async_trait::async_trait;

#[async_trait]
pub trait Table {
    fn get_table(&self) -> TableRef;
}