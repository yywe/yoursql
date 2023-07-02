use crate::{common::types::TableRef, session::SessionState, physical_plan::ExecutionPlan};
use std::any::Any;
use async_trait::async_trait;
use crate::expr::expr::Expr;
use anyhow::Result;
use std::sync::Arc;


mod empty;
mod memory;

#[async_trait]
pub trait Table: Sync + Send{
    fn get_table(&self) -> TableRef;
    fn as_any(&self) -> &dyn Any;
    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr])->Result<Arc<dyn ExecutionPlan>>;
}


pub fn project_table(table: &TableRef, projection: Option<&Vec<usize>>) -> Result<TableRef> {
    let new_table = match projection {
        Some(indices) => Arc::new(table.project(indices)?),
        None => Arc::clone(table),
    };
    Ok(new_table)
}