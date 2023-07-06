use crate::catalog::Catalog;
use crate::catalog::CatalogList;
use crate::catalog::MemoryCatalog;
use crate::catalog::MemoryCatalogList;
use crate::catalog::MemorySchema;
use crate::catalog::Schema;
use crate::common::config::ConfigOptions;
use crate::common::table_reference::ResolvedTableReference;
use crate::common::table_reference::TableReference;
use crate::storage::Table;
use anyhow::Context;
use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct SessionContext {
    pub session_id: String,
    pub session_start_time: DateTime<Utc>,
    pub state: Arc<RwLock<SessionState>>,
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

    pub fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        let catalog = &self.catalogs;
        table_ref.into().resolve(
            &self.config.catalog.default_catalog,
            &self.config.catalog.default_schema,
        )
    }

    pub fn schema_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn Schema>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        self.catalogs
            .catalog(&resolved_ref.catalog)
            .context(format!(
                "failed to resolve catalog: {}",
                resolved_ref.catalog
            ))?
            .get_schema(&resolved_ref.schema)
            .context(format!("failed to resolve schema:{}", resolved_ref.schema))
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

    pub fn register_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
        table: Arc<dyn Table>,
    ) -> Result<Option<Arc<dyn Table>>> {
        let table_ref = table_ref.into();
        let table_name = table_ref.table_name().to_owned();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table_name, table)
    }

    pub fn state(&self) -> SessionState {
        self.state.read().clone()
    }
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new_inmemory_ctx()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::record_batch::RecordBatch;
    use crate::common::types::DataType;
    use crate::common::types::DataValue;
    use crate::common::types::Field;
    use crate::common::types::TableDef;
    use crate::storage::empty::EmptyTable;
    use crate::storage::memory::MemTable;
    use anyhow::Result;
    use futures::StreamExt;
    use std::collections::HashMap;
    #[test]
    fn test_session_init() -> Result<()> {
        let session = SessionContext::default();
        let empty_table = EmptyTable::new(Arc::new(TableDef::new(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Boolean, false),
            ],
            HashMap::new(),
        )));
        let table_referene = TableReference::Bare {
            table: "testa".into(),
        };
        session.register_table(table_referene.clone(), Arc::new(empty_table))?;
        let schema = session.state.read().schema_for_ref(table_referene)?;
        assert_eq!(schema.table_exist("testa"), true);
        Ok(())
    }

    #[tokio::test]
    async fn test_memtable_scan() -> Result<()> {
        let session = SessionContext::default();
        let memtable_def = TableDef::new(
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Boolean, false),
            ],
            HashMap::new(),
        );
        let row_batch1 = vec![
            vec![DataValue::Int64(1), DataValue::Boolean(false)],
            vec![DataValue::Int64(2), DataValue::Boolean(false)],
        ];
        let row_batch2 = vec![
            vec![DataValue::Int64(3), DataValue::Boolean(true)],
            vec![DataValue::Int64(4), DataValue::Boolean(true)],
        ];
        let batch1 = RecordBatch {
            header: memtable_def.fields.clone(),
            rows: row_batch1.clone(),
        };
        let batch2 = RecordBatch {
            header: memtable_def.fields.clone(),
            rows: row_batch2.clone(),
        };
        let memtable = MemTable::try_new(Arc::new(memtable_def), vec![batch1, batch2])?;
        let table_referene = TableReference::Bare {
            table: "testa".into(),
        };
        let table_ref = Arc::new(memtable);
        session.register_table(table_referene.clone(), table_ref.clone())?;
        let schema = session.state.read().schema_for_ref(table_referene)?;
        assert_eq!(schema.table_exist("testa"), true);
        let exec = table_ref.scan(&session.state(), None, &[]).await?;
        let mut it = exec.execute()?;
        // note 1st unwrap is for option, 2nd the item of the stream is Result of RecordBatch, so use ?
        let fetch_batch1: RecordBatch = it.next().await.unwrap()?;
        assert_eq!(fetch_batch1.rows, row_batch1);
        let fetch_batch2: RecordBatch = it.next().await.unwrap()?;
        assert_eq!(fetch_batch2.rows, row_batch2);
        let done  = it.next().await;
        assert_eq!(true, done.is_none());
        Ok(())
    }
}
