use crate::common::{record_batch::RecordBatch, types::TableRef};
use crate::expr::expr::Expr;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::memory::MemoryExec;
use crate::session::SessionState;
use crate::storage::Table;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemTable {
    def: TableRef,
    batches: Vec<RecordBatch>,
}

impl MemTable {
    pub fn try_new(def: TableRef, batches: Vec<RecordBatch>) -> Result<Self> {
        // check if the header matches the table fields
        for batch in batches.iter() {
            if batch.header != def.fields {
                return Err(anyhow!(
                    "batch header {:?} does not match table definition {:?}",
                    batch.header,
                    def.fields
                ));
            }
        }
        Ok(Self {
            def: def,
            batches: batches,
        })
    }
}

#[async_trait]
impl Table for MemTable {
    fn get_table(&self) -> TableRef {
        self.def.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(self.def.clone(), self.batches.clone(), projection.cloned())?))
    }
}
