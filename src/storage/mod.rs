use arrow_schema::Fields;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct SchemaDef {
    pub fields: Fields,
    pub metadata: HashMap<String, String>,
}

pub type SchemaRef = Arc<SchemaDef>;

#[async_trait]
pub trait Table {
    fn schema(&self) -> SchemaRef;
}