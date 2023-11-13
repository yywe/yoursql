
use std::sync::Arc;
use crate::physical_planner::SchemaRef;
use crate::physical_expr::PhysicalExpr;
use crate::expr::logical_plan::JoinType;
use crate::physical_planner::utils::OnceAsync;
use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use crate::common::record_batch::RecordBatch;
#[derive(Debug)]
pub struct NestedLoopJoinExec {
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
    pub filter: Option<Arc<dyn PhysicalExpr>>,
    pub join_type: JoinType,
    schema: SchemaRef,
    inner_table: OnceAsync<RecordBatch>,
}