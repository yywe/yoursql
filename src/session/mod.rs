use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::RwLock;
use crate::catalog::CatalogList;
use crate::common::config::ConfigOptions;

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

