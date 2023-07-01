use async_trait::async_trait;
use std::sync::Arc;
use crate::storage::Table;
use anyhow::{Result, anyhow};
use dashmap::DashMap;

#[async_trait]
pub trait Schema {
    fn table_names(&self) -> Vec<String>;
    async fn get_table(&self, name: &str) -> Option<Arc<dyn Table>>;
    fn table_exist(&self, name: &str) -> bool;
    fn register_table(&self, name: String, table: Arc<dyn Table>) -> Result<Option<Arc<dyn Table>>>;
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>>;
}

pub struct MemorySchema {
    tables: DashMap<String, Arc<dyn Table>>,
}

impl MemorySchema {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }
}

impl Default for MemorySchema{
    fn default()->Self {
        Self::new()
    }
}

#[async_trait]
impl Schema for MemorySchema {
    fn table_names(&self) -> Vec<String>{
        self.tables.iter().map(|table|table.key().clone()).collect()
    }
    async fn get_table(&self, name: &str) -> Option<Arc<dyn Table>>{
        self.tables.get(name).map(|table|table.value().clone())
    }
    fn table_exist(&self, name: &str) -> bool{
        self.tables.contains_key(name)
    }
    fn register_table(&self, name: String, table: Arc<dyn Table>) -> Result<Option<Arc<dyn Table>>>{
        if self.table_exist(name.as_str()) {
            return Err(anyhow!(format!("The table {name} already exist")));
        }
        Ok(self.tables.insert(name, table))
    }
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>>{
        Ok(self.tables.remove(name).map(|(_, table)|table))
    }
}

pub trait Catalog {
    fn schema_names(&self) -> Vec<String>;
    fn get_schema(&self, name: &str) -> Option<Arc<dyn Schema>>;
    fn register_schema(&self, name: &str, schema: Arc<dyn Schema>) -> Result<Option<Arc<dyn Schema>>>;
    fn deregister_schema(&self, name: &str) -> Result<Option<Arc<dyn Schema>>>;
}

pub trait CatalogList {
    fn register_catalog(&self, name: String, catalog: Arc<dyn Catalog>) -> Option<Arc<dyn Catalog>>;
    fn catalog_names(&self) -> Vec<String>;
    fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>>;
}
