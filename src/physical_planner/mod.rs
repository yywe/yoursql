pub mod aggregate;
pub mod create_table;
pub mod cross_join;
pub mod empty;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod memory;
pub mod nested_loop_join;
pub mod planner;
pub mod projection;
pub mod sort;
pub mod utils;
pub mod values;

use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::schema::SchemaRef;
use crate::expr::expr::Expr;
use crate::expr::logical_plan::LogicalPlan;
use crate::physical_expr::planner::create_physical_expr;
use crate::physical_expr::PhysicalExpr;
use crate::session::SessionState;
use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

pub enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

/// note the item is Result of RecordBatch
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

pub trait ExecutionPlan: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> SchemaRef;
    fn execute(&self, session_state: &SessionState) -> Result<SendableRecordBatchStream>;
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

pub async fn print_batch_stream(mut rs: Pin<Box<dyn RecordBatchStream + Send>>) -> Result<()> {
    let header = rs
        .schema()
        .all_fields()
        .iter()
        .map(|f| (*f).clone())
        .collect::<Vec<_>>();
    println!(
        "{}",
        header
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join("|")
    );
    while let Some(result) = rs.next().await {
        if let Ok(batch) = result {
            for row in batch.rows {
                println!(
                    "{}",
                    row.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join("|")
                )
            }
        } else {
            println!("error occured while fetching next result")
        }
    }
    Ok(())
}

#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        input_logischema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

#[derive(Default)]
pub struct DefaultPhysicalPlanner {}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .create_initial_plan(logical_plan, session_state)
            .await?;
        Ok(plan)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        input_logischema: &Schema,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(expr, input_schema, input_logischema)
    }
}

impl DefaultPhysicalPlanner {
    pub fn physical_optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("todo: implement physical optimizer here");
        Ok(plan)
    }
}
