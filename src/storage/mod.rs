use arrow_schema::SchemaRef;
use async_trait::async_trait;

#[async_trait]
pub trait Table {
    fn schema(&self) -> SchemaRef;
}