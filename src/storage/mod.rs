use crate::{common::schema::SchemaRef, session::SessionState, physical_plan::ExecutionPlan};
use std::any::Any;
use async_trait::async_trait;
use crate::expr::expr::Expr;
use anyhow::Result;
use std::sync::Arc;


pub mod empty;
pub mod memory;

#[async_trait]
pub trait Table: Sync + Send{
    fn get_table(&self) -> SchemaRef;
    fn as_any(&self) -> &dyn Any;
    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr])->Result<Arc<dyn ExecutionPlan>>;
}


pub fn project_table(table: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let new_table = match projection {
        Some(indices) => Arc::new(table.project(indices)?),
        None => Arc::clone(table),
    };
    Ok(new_table)
}