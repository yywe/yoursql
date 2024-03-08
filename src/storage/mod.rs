use crate::{
    common::{record_batch::RecordBatch, schema::SchemaRef},
    expr::expr::Expr,
    physical_planner::ExecutionPlan,
    session::SessionState,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{any::Any, sync::Arc};

pub mod empty;
pub mod memory;

#[async_trait]
pub trait Table: Sync + Send {
    fn get_table(&self) -> SchemaRef;
    fn as_any(&self) -> &dyn Any;
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>>;
    async fn insert(&self, batch: RecordBatch) -> Result<usize>;
}

pub fn project_table(table: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let new_table = match projection {
        Some(indices) => Arc::new(table.project(indices)?),
        None => Arc::clone(table),
    };
    Ok(new_table)
}
