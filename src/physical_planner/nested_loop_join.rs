use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::types::DataValue;
use crate::expr::logical_plan::JoinType;
use crate::physical_expr::PhysicalExpr;
use crate::physical_planner::utils::collect_batch_stream;
use crate::physical_planner::utils::{OnceAsync, OnceFut};
use crate::session::SessionState;
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
    fn execute(&self, state: &SessionState) -> Result<SendableRecordBatchStream> {
        // for right and full join, left is inner table, right is outer table
        // otherwise (left, inner), right is the inner table, left is outer table
        let state_cloned = state.clone();
        let (outer_table, inner_table) =
            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                let inner_table = self
                    .inner_table
                    .once(|| collect_batch_stream(self.left.clone(), state_cloned));
                let outer_table = self.right.execute(state)?;
                (outer_table, inner_table)
            } else {
                let inner_table = self
                    .inner_table
                    .once(|| collect_batch_stream(self.right.clone(), state_cloned));
                let outer_table = self.left.execute(state)?;
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
                    let result = join_left_and_right_batch(
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
        // right table is the inner table, has all data
        let right_data = match ready!(self.inner_table.get(cx)) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        self.outer_table
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(left_batch)) => {
                    let result = join_left_and_right_batch(
                        &left_batch,
                        right_data,
                        self.join_type,
                        self.filter.as_ref(),
                        &self.schema,
                        &mut self.visited_leftside, // will not be used to consturct last rows
                    );
                    Some(result)
                }
                Some(err) => Some(err),
                None => None,
            })
    }
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn join_left_and_right_batch(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    join_type: JoinType,
    filter: Option<&Arc<dyn PhysicalExpr>>,
    schema: &Schema,
    visited_left_side: &mut HashMap<usize, bool>,
) -> Result<RecordBatch> {
    let merged_rows_with_idices: Vec<(Vec<DataValue>, usize, usize)> = left_batch
        .rows
        .iter()
        .enumerate()
        .flat_map(|(left_idx, lrow)| {
            right_batch
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
        let mut visited_right_indices = HashMap::new();
        let filter_column = filter_exp.evaluate(&merged_batch)?;
        let mut filtered_rows: Vec<Vec<DataValue>> = Vec::new();
        for (((row, flag), left_idx), right_idx) in merged_batch
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
                        visited_left_side.insert(left_idx, true);
                        visited_right_indices.insert(right_idx, true);
                    }
                }
                _ => return Err(anyhow::anyhow!("Invalid Filter evaluation result")),
            }
        }

        match join_type {
            // for right join and full join, the left side has all the data, right side is looped as a stream
            // here, for rows in right side stream, there is not matched left, add left as NULLs

            // for full join, still, left is build side has all data, ROWs with unmatched right side (left NULLs) are added here
            // meanwhile, visited_left_side will accumulate the visited (matched) left rows.
            //when right side stream is done, then then finally look at remaining unvisited left index

            // as such, the full join for another half of unmatched rows (left unmatched, add NULLs right), is only processed in left is build side
            // poll_next_impl_for_build_left
            JoinType::Right | JoinType::Full => {
                for idx in 0..right_batch.rows.len() {
                    if visited_right_indices.contains_key(&idx) == false {
                        let mut extra_row = vec![DataValue::Null; left_batch.schema.fields.len()];
                        extra_row.extend(right_batch.rows[idx].clone());
                        filtered_rows.push(extra_row);
                    }
                }
            }
            // for left join, right side has all the data, left side is the stream
            // for rows in the stream not matched, add NULLs for right
            JoinType::Left => {
                for idx in 0..left_batch.rows.len() {
                    if visited_left_side.contains_key(&idx) == false {
                        let mut extra_row = left_batch.rows[idx].clone();
                        extra_row.extend(vec![DataValue::Null; right_batch.schema.fields.len()]);
                        filtered_rows.push(extra_row);
                    }
                }
            }
            _ => {}
        }
        Ok(RecordBatch {
            schema: Arc::new(schema.clone()),
            rows: filtered_rows,
        })
    } else {
        Ok(merged_batch)
    }
}
