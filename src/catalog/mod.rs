use async_trait::async_trait;
use std::sync::Arc;
use crate::storage::Table;
use anyhow::Result;
use std::collections::HashMap;

#[async_trait]
pub trait Schema {
    fn table_names(&self) -> Vec<String>;
    async fn get_table(&self, name: &str) -> Option<Arc<dyn Table>>;
    fn table_exist(&self, name: &str) -> bool;
    fn register_table(&self, name: String, table: Arc<dyn Table>) -> Result<Option<Arc<dyn Table>>>;
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>>;

}

pub trait Catalog {
    fn schema_names(&self) -> Vec<String>;
    fn get_schema(&self, name: &str) -> Option<Arc<dyn Schema>>;
    fn register_schema(&self, name: &str, schema: Arc<dyn Schema>) -> Result<Option<Arc<dyn Schema>>>;
    fn deregister_schema(&self, name: &str) -> Result<Option<Arc<dyn Schema>>>;
}

pub struct CatalogSet {
    pub catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogSet {
    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|(k,_)|k.clone()).collect()
    }
}