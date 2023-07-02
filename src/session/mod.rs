use crate::catalog::Catalog;
use crate::catalog::CatalogList;
use crate::catalog::MemoryCatalog;
use crate::catalog::MemoryCatalogList;
use crate::catalog::MemorySchema;
use crate::common::config::ConfigOptions;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::RwLock;
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
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new_inmemory_ctx()
    }
}
