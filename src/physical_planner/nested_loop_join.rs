use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use crate::physical_planner::utils::collect_batch_stream;
use crate::common::schema::Schema;
use futures::Stream;
use std::collections::HashMap;
use crate::common::record_batch::RecordBatch;
use crate::expr::logical_plan::JoinType;
use crate::physical_expr::PhysicalExpr;
use crate::physical_planner::utils::{OnceAsync,OnceFut};
use crate::{expr::logical_plan::builder::build_join_schema, physical_planner::SchemaRef};
use anyhow::Result;
use std::task::Poll;
use std::sync::Arc;
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
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        join_type: &JoinType,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let schema = build_join_schema(&left_schema, &right_schema, join_type)?;
        Ok(NestedLoopJoinExec {
            left: left,
            right: right,
            filter: filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            inner_table: Default::default(),
        })
    }
}

struct NestedLoopJoinStream {
    schema: Arc<Schema>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    join_type: JoinType,
    outer_table: SendableRecordBatchStream,
    inner_table: OnceFut<RecordBatch>,
    is_exhausted: bool,
    visited_leftside: Option<HashMap<i64, bool>>,
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }
    fn with_new_chilren(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NestedLoopJoinExec::try_new(children[0].clone(), children[1].clone(), self.filter.clone(), &self.join_type)?))
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        // for right and full join, left is inner table, right is outer table
        // otherwise (left, inner), right is the inner table, left is outer table
        let (outer_table, inner_table) = if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let inner_table = self.inner_table.once(||collect_batch_stream(self.left.clone()));
            let outer_table = self.right.execute()?;
            (outer_table, inner_table)
        }else{
            let inner_table = self.inner_table.once(||collect_batch_stream(self.right.clone()));
            let outer_table = self.left.execute()?;
            (outer_table, inner_table)   
        };
        Ok(Box::pin(NestedLoopJoinStream{
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            outer_table,
            inner_table,
            is_exhausted: false,
            visited_leftside: None,
        }))
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.poll_next_impl_for_build_left(cx)
        }else{
            self.poll_next_impl_for_build_right(cx)
        }
    }
}

impl NestedLoopJoinStream {
    fn poll_next_impl_for_build_left(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        return Poll::Ready(None);
    }
    fn poll_next_impl_for_build_right(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<Result<RecordBatch>>> {

        return Poll::Ready(None);
    }
    
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}