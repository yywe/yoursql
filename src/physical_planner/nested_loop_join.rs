
use std::sync::Arc;
use anyhow::Result;
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

impl NestedLoopJoinExec {
    pub fn try_new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, filter: Option<Arc<dyn PhysicalExpr>>, join_type: &JoinType) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        
    }
}