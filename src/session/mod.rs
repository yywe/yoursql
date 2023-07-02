use crate::catalog::CatalogList;
use crate::catalog::MemoryCatalogList;
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
        let state = SessionState::new(ConfigOptions::new(), Arc::new(MemoryCatalogList::new()));
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
