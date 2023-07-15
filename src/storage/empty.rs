use async_trait::async_trait;
use crate::storage::Table;
use crate::common::schema::SchemaRef;
use crate::session::SessionState;
use std::any::Any;
use anyhow::Result;
use std::sync::Arc;
use crate::expr::expr::Expr;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::empty::EmptyExec;

use super::project_table;

pub struct EmptyTable {
    schema: SchemaRef
}

impl EmptyTable {
    pub fn new(schema: SchemaRef) -> Self {
        Self{schema}
    }
}


#[async_trait]
impl Table for EmptyTable{
    fn get_table(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn as_any(&self) -> &dyn Any{
        self
    }
    async fn scan(&self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr])->Result<Arc<dyn ExecutionPlan>>{
        let projected = project_table(&self.schema, projection)?;
        Ok(Arc::new(
            EmptyExec::new(false, projected)
        ))
    }

}

