use async_trait::async_trait;
use std::sync::Arc;
use crate::storage::Table;
use anyhow::{Result, anyhow};
use dashmap::DashMap;

#[async_trait]
pub trait DB: Send + Sync {
    fn table_names(&self) -> Vec<String>;
    async fn get_table(&self, name: &str) -> Option<Arc<dyn Table>>;
    fn table_exist(&self, name: &str) -> bool;
    fn register_table(&self, name: String, table: Arc<dyn Table>) -> Result<Option<Arc<dyn Table>>>;
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn Table>>>;
}

pub struct MemoryDB {
    tables: DashMap<String, Arc<dyn Table>>,
}

impl MemoryDB {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }
}

impl Default for MemoryDB{
    fn default()->Self {
        Self::new()
    }
}

#[async_trait]
impl DB for MemoryDB {
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


pub trait DBList: Send + Sync {
    fn register_database(&self, name: String, database: Arc<dyn DB>) -> Option<Arc<dyn DB>>;
    fn database_names(&self) -> Vec<String>;
    fn database(&self, name: &str) -> Option<Arc<dyn DB>>;
}

pub struct MemoryDBList {
    pub databases: DashMap<String, Arc<dyn DB>>,
}

impl MemoryDBList {
    pub fn new() -> Self {
        Self {
            databases: DashMap::new(),
        }
    }
}

impl Default for MemoryDBList {
    fn default() -> Self {
        Self::new()
    }
}

impl DBList for MemoryDBList {
    fn register_database(&self, name: String, database: Arc<dyn DB>) -> Option<Arc<dyn DB>>{
        self.databases.insert(name, database)
    }
    fn database_names(&self) -> Vec<String>{
        self.databases.iter().map(|c|c.key().clone()).collect()
    }
    fn database(&self, name: &str) -> Option<Arc<dyn DB>>{
        self.databases.get(name).map(|c|c.value().clone())
    }
}