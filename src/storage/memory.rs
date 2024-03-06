use crate::common::{record_batch::RecordBatch, schema::SchemaRef};
use crate::expr::expr::Expr;
use crate::physical_planner::memory::MemoryExec;
use crate::physical_planner::ExecutionPlan;
use crate::session::SessionState;
use crate::storage::Table;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    batches: Mutex<Vec<RecordBatch>>,
}

impl MemTable {
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
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
            batches: Mutex::new(batches),
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
        Ok(Arc::new(MemoryExec::try_new(
            self.schema.clone(),
            self.batches.lock().unwrap().clone(),
            projection.cloned(),
        )?))
    }
    fn insert(&self, batch: RecordBatch) -> Result<usize> {
        //TODO: the storage engine should be able to detect constraints (e.g, uniqueness, nullablity)
        //for now simply insert the batch into memory table
        //TODO: add null or default here or at upper layer?
        if !batch.schema.eq(&self.schema) {
            return Err(anyhow!(
                "The schema in data {:?} does not equal table schema {:?}",
                batch.schema,
                self.schema
            ));
        }
        let rowlen = batch.rows.len();
        self.batches.lock().unwrap().push(batch);
        Ok(rowlen)
    }
}
