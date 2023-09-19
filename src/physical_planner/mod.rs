pub mod empty;
pub mod memory;

use crate::common::schema::SchemaRef;
use crate::common::record_batch::RecordBatch;
use futures::Stream;
use anyhow::Result;
use std::pin::Pin;
use std::fmt::Debug;
use std::any::Any;
use futures::StreamExt;
use async_trait::async_trait;
use crate::expr::logical_plan::LogicalPlan;
use crate::session::SessionState;
use std::sync::Arc;
use crate::expr::expr::Expr;
use crate::common::schema::Schema;
use crate::physical_expr::PhysicalExpr;

/// note the item is Result of RecordBatch
pub trait RecordBatchStream: Stream<Item=Result<RecordBatch>>{
    fn schema(&self) -> SchemaRef;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub trait ExecutionPlan: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<SendableRecordBatchStream>;
}

pub async fn print_batch_stream(mut rs: Pin<Box<dyn RecordBatchStream + Send>>) -> Result<()> {
    let header = rs.schema().all_fields().iter().map(|f|(*f).clone()).collect::<Vec<_>>();
    println!("{}", header.iter().map(|f|f.name().as_str()).collect::<Vec<_>>().join("|"));
    while let Some(result) = rs.next().await {
        if let Ok(batch) = result{
            for row in batch.rows {
                println!("{}", row.iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"))
            }
        }else{
            println!("error occured while fetching next result")
        }
    }
    Ok(())
}

#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    async fn create_physical_plan(&self, logical_plan: &LogicalPlan, session_state: &SessionState) -> Result<Arc<dyn ExecutionPlan>>;
    async fn create_physical_expr(&self, expr: &Expr, input_schema: &Schema, session_state: &SessionState) -> Result<Arc<dyn PhysicalExpr>>;
}

#[derive(Default)]
pub struct DefaultPhysicalPlanner {

}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    async fn create_physical_plan(&self, logical_plan: &LogicalPlan, session_state: &SessionState) -> Result<Arc<dyn ExecutionPlan>>{
        unimplemented!();
    }

    async fn create_physical_expr(&self, expr: &Expr, input_schema: &Schema, session_state: &SessionState) -> Result<Arc<dyn PhysicalExpr>>{
        unimplemented!();
    }
}

impl DefaultPhysicalPlanner {
    pub fn physical_optimize(&self, plan: Arc<dyn ExecutionPlan>, session_state: &SessionState) ->  Result<Arc<dyn ExecutionPlan>> {
        println!("todo: implement physical optimizer here");
        Ok(plan)
    }
}