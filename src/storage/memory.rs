use crate::common::{record_batch::RecordBatch, types::SchemaRef};
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
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemTable {
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        // check if the header matches the table fields
        for batch in batches.iter() {
            if batch.schema != schema {
                return Err(anyhow!(
                    "batch schema {:?} does not match table definition {:?}",
                    batch.schema,
                    schema
                ));
            }
        }
        Ok(Self {
            schema: schema,
            batches: batches,
        })
    }
}

#[async_trait]
impl Table for MemTable {
    fn get_table(&self) -> SchemaRef {
        self.schema.clone()
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
        Ok(Arc::new(MemoryExec::try_new(self.schema.clone(), self.batches.clone(), projection.cloned())?))
    }
}
