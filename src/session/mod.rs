use crate::catalog::Catalog;
use crate::catalog::CatalogList;
use crate::catalog::MemoryCatalog;
use crate::catalog::MemoryCatalogList;
use crate::catalog::MemorySchema;
use crate::common::config::ConfigOptions;
use chrono::{DateTime, Utc};
use sled::Config;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub struct SessionContext {
    session_id: String,
    session_start_time: DateTime<Utc>,
    state: Arc<RwLock<SessionState>>,
}

#[derive(Clone)]
pub struct SessionState {
    session_id: String,
    catalogs: Arc<dyn CatalogList>,
    config: ConfigOptions,
}

impl SessionState {
    pub fn new(config: ConfigOptions, catalogs: Arc<dyn CatalogList>) -> Self {
        let session_id = Uuid::new_v4().to_string();
        Self {
            session_id,
            catalogs,
            config,
        }
    }
    pub fn catalogs(&self) -> Arc<dyn CatalogList> {
        self.catalogs.clone()
    }
    pub fn config(&self) -> &ConfigOptions {
        &self.config
    }
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    
}

impl SessionContext {
    pub fn new_inmemory_ctx() -> Self {
        let config = ConfigOptions::new();
        let catalogs = MemoryCatalogList::new();
        if config.catalog.create_default_catalog_and_schema {
            let default_catalog = MemoryCatalog::new();
            default_catalog
                .register_schema(
                    &config.catalog.default_schema,
                    Arc::new(MemorySchema::new()),
                )
                .expect("failed to register schema");
            catalogs.register_catalog(
                config.catalog.default_catalog.clone(),
                Arc::new(default_catalog),
            );
        }
        let state = SessionState::new(config, Arc::new(catalogs));
        Self {
            session_id: state.session_id.clone(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }
    
    pub fn catalog_names(&self) -> Vec<String> {
        self.state.read().catalogs.catalog_names()
    }
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        self.state.read().catalogs.catalog(name)
    }
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
    }

    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new_inmemory_ctx()
    }
}

// todo: test session module, and add memory table (include method to init data), then test basic table scan

#[cfg(test)]
mod test{
    use super::*;
    use anyhow::Result;
    #[test]
    fn test_session_init() -> Result<()> {
        let session = SessionContext::default();
        let session_id = session.state.read().session_id();
        //let catalogs: std::result::Result<std::sync::RwLockReadGuard<'_, SessionState>, std::sync::PoisonError<std::sync::RwLockReadGuard<'_, SessionState>>> = session.state.read();
        //println!("the catalog {:?}", catalogs);
        //let x = session.state.read().unwrap().catalogs.catalog("yoursql").unwrap();
        //println!("the schema {:?}?",x.schema_names())
        Ok(())
    }
}