use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::types::DataValue;
use crate::expr::logical_plan::JoinType;
use crate::physical_expr::PhysicalExpr;
use crate::physical_planner::utils::collect_batch_stream;
use crate::physical_planner::utils::{OnceAsync, OnceFut};
use crate::{expr::logical_plan::builder::build_join_schema, physical_planner::SchemaRef};
use anyhow::Result;
use futures::{ready, Stream, StreamExt};
use itertools::multiunzip;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;
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
    visited_leftside: HashMap<usize, bool>,
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
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NestedLoopJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.filter.clone(),
            &self.join_type,
        )?))
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        // for right and full join, left is inner table, right is outer table
        // otherwise (left, inner), right is the inner table, left is outer table
        let (outer_table, inner_table) =
            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                let inner_table = self
                    .inner_table
                    .once(|| collect_batch_stream(self.left.clone()));
                let outer_table = self.right.execute()?;
                (outer_table, inner_table)
            } else {
                let inner_table = self
                    .inner_table
                    .once(|| collect_batch_stream(self.right.clone()));
                let outer_table = self.left.execute()?;
                (outer_table, inner_table)
            };
        Ok(Box::pin(NestedLoopJoinStream {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            outer_table,
            inner_table,
            is_exhausted: false,
            visited_leftside: HashMap::default(),
        }))
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.poll_next_impl_for_build_left(cx)
        } else {
            self.poll_next_impl_for_build_right(cx)
        }
    }
}

impl NestedLoopJoinStream {
    fn poll_next_impl_for_build_left(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let left_data = match ready!(self.inner_table.get(cx)) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        self.outer_table
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(right_batch)) => {
                    let result = join_inner_and_outer_batch(
                        left_data,
                        &right_batch,
                        self.join_type,
                        self.filter.as_ref(),
                        &self.schema,
                        &mut self.visited_leftside,
                    );
                    Some(result)
                }
                Some(err) => Some(err),
                None => {
                    if self.join_type == JoinType::Full && !self.is_exhausted {
                        let mut final_rows = Vec::new();
                        for idx in 0..left_data.rows.len() {
                            if self.visited_leftside.contains_key(&idx) == false {
                                let mut final_row = left_data.rows[idx].clone();
                                final_row.extend(vec![
                                    DataValue::Null;
                                    self.outer_table.schema().fields().len()
                                ]);
                                final_rows.push(final_row);
                            }
                        }
                        self.is_exhausted = true;
                        Some(Ok(RecordBatch {
                            schema: self.schema.clone(),
                            rows: final_rows,
                        }))
                    } else {
                        None
                    }
                }
            })
    }

    fn poll_next_impl_for_build_right(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        return Poll::Ready(None);
    }
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn join_inner_and_outer_batch(
    inner_batch: &RecordBatch,
    outer_batch: &RecordBatch,
    join_type: JoinType,
    filter: Option<&Arc<dyn PhysicalExpr>>,
    schema: &Schema,
    visited_inner_side: &mut HashMap<usize, bool>,
) -> Result<RecordBatch> {
    let merged_rows_with_idices: Vec<(Vec<DataValue>, usize, usize)> = inner_batch
        .rows
        .iter()
        .enumerate()
        .flat_map(|(left_idx, lrow)| {
            outer_batch
                .rows
                .iter()
                .enumerate()
                .map(|(right_idx, rrow)| {
                    let mut merged_row = lrow.clone();
                    merged_row.extend(rrow.clone());
                    (merged_row, left_idx, right_idx)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let (merged_rows, left_idices, right_indices): (Vec<Vec<DataValue>>, Vec<usize>, Vec<usize>) =
        multiunzip(merged_rows_with_idices);
    let merged_batch = RecordBatch {
        schema: Arc::new(schema.clone()),
        rows: merged_rows,
    };

    if let Some(filter_exp) = filter {
        let mut visited_outer_indices = HashMap::new();
        let filter_column = filter_exp.evaluate(&merged_batch)?;
        let mut filtered_rows: Vec<Vec<DataValue>> = Vec::new();
        for (((row, flag), inner_idx), outer_idx) in merged_batch
            .rows
            .into_iter()
            .zip(filter_column.into_iter())
            .zip(left_idices.into_iter())
            .zip(right_indices.into_iter())
        {
            match flag {
                DataValue::Boolean(v) => {
                    if let Some(true) = v {
                        filtered_rows.push(row);
                        visited_inner_side.insert(inner_idx, true);
                        visited_outer_indices.insert(outer_idx, true);
                    }
                }
                _ => return Err(anyhow::anyhow!("Invalid Filter evaluation result")),
            }
        }
        //if not inner join, for unmatched outer rows, add NULLs, for full join, will add other side in the caller
        if join_type != JoinType::Inner {
            for idx in 0..outer_batch.rows.len() {
                if visited_outer_indices.contains_key(&idx) == false {
                    match join_type {
                        JoinType::Right | JoinType::Full => {
                            let mut append_row =
                                vec![DataValue::Null; inner_batch.schema.fields.len()];
                            append_row.extend(outer_batch.rows[idx].clone());
                            filtered_rows.push(append_row);
                        }
                        JoinType::Left => {
                            let mut append_row = outer_batch.rows[idx].clone();
                            append_row
                                .extend(vec![DataValue::Null; inner_batch.schema.fields.len()]);
                            filtered_rows.push(append_row);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
        Ok(RecordBatch {
            schema: Arc::new(schema.clone()),
            rows: filtered_rows,
        })
    } else {
        Ok(merged_batch)
    }
}
