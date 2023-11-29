use std::sync::Arc;
use anyhow::Result;
use futures::StreamExt;
use std::pin::Pin;
use core::cmp::min;
use std::task::Poll;
use futures::Stream;
use futures::ready;
use crate::common::record_batch::RecordBatch;
use crate::common::schema::SchemaRef;
use crate::common::types::DataValue;
use crate::physical_expr::sort::PhysicalSortExpr;
use crate::physical_planner::utils::transpose_matrix;
use crate::physical_planner::ExecutionState;
use super::{ExecutionPlan, SendableRecordBatchStream, RecordBatchStream};

#[derive(Debug)]
pub struct SortExec {
    pub input: Arc<dyn ExecutionPlan>,
    expr: Vec<PhysicalSortExpr>,
    fetch: Option<usize>, // not supported yet
}


impl SortExec {
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Self {
        Self {
            input, 
            expr,
            fetch
        }
    }
}

impl ExecutionPlan for SortExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> crate::common::schema::SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn with_new_chilren(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        let new_sort = SortExec::new(self.expr.clone(), children[0].clone(), self.fetch.clone());
        Ok(Arc::new(new_sort))
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        //todo: let batch size configurable
        let batch_size = 2;
        let input = self.input.execute()?;
        Ok(Box::pin(InMemSorter::new(self.input.schema().clone(), input, self.expr.clone(), batch_size)))
    }
}

fn sort_batch(batch: RecordBatch, sort_expr: &Vec<PhysicalSortExpr>) -> Result<RecordBatch> {
    //note here each inner vec is the expr values at different rows
    let schema = batch.schema.clone();
    let sort_values = sort_expr.iter().map(|pse|pse.expr.evaluate(&batch)).collect::<Result<Vec<_>>>()?;
    let sort_values = transpose_matrix(sort_values)?;
    struct Item {
        row: Vec<DataValue>,
        sort_value: Vec<DataValue>,
    }
    let mut items = Vec::new();
    for (row, sort_value) in batch.rows.into_iter().zip(sort_values.into_iter()) {
        items.push(Item{row, sort_value})
    }
    items.sort_by(|a,b|{
       for (i, se) in sort_expr.iter().enumerate() {
            let ai = &a.sort_value[i];
            let bi = &b.sort_value[i];
            match ai.partial_cmp(bi) {
                Some(std::cmp::Ordering::Equal) => {},
                Some(o)=>{
                    return if se.descending {o.reverse()} else {o}
                }
                None=>{}
            }
       }
       std::cmp::Ordering::Equal
    });
    Ok(RecordBatch { schema: schema, rows: items.into_iter().map(|item|item.row).collect()})
}


struct InMemSorter {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    input: SendableRecordBatchStream,
    expr: Vec<PhysicalSortExpr>,
    batch_size: usize,
    fetched_index: usize,
    exec_phase: ExecutionState,
    sorted_batch: Option<RecordBatch>
}

impl InMemSorter {
    pub fn new(schema: SchemaRef, input: SendableRecordBatchStream, expr: Vec<PhysicalSortExpr>, batch_size: usize) -> Self {
        Self {
            schema: schema,
            batches: vec![],
            input: input,
            expr: expr.into(),
            batch_size: batch_size,
            fetched_index: 0,
            exec_phase: ExecutionState::ReadingInput,
            sorted_batch: None,
        }
    }
}

impl RecordBatchStream for InMemSorter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InMemSorter {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.exec_phase {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch))=> {
                            self.batches.push(batch);
                        },
                        Some(Err(e))=>return Poll::Ready(Some(Err(e))),
                        None=>{
                            let mut merged_rows = Vec::new();
                            for batch in self.batches.iter() {
                                merged_rows.extend(batch.rows.clone());
                            }
                            if merged_rows.len() > 0 {
                                let merged_batch = RecordBatch {
                                    schema: self.schema().clone(),
                                    rows: merged_rows,
                                };
                                let sorted_merged_batch = sort_batch(merged_batch, &self.expr)?;
                                self.sorted_batch = Some(sorted_merged_batch);
                            }
                            self.exec_phase = ExecutionState::ProducingOutput;
                        }
                    }
                }
                ExecutionState::ProducingOutput => {
                    if let Some(sbatch) = &self.sorted_batch {
                        // all fetched
                        if self.fetched_index >= sbatch.rows.len() {
                            self.exec_phase = ExecutionState::Done;
                            return Poll::Ready(None);
                        }
                        let end_idx = min(self.fetched_index + self.batch_size, sbatch.rows.len());
                        let ret_batch = RecordBatch{
                            schema: self.schema.clone(),
                            rows: sbatch.rows[self.fetched_index..end_idx].to_vec(),
                        };
                        self.fetched_index += self.batch_size;
                        return Poll::Ready(Some(Ok(ret_batch)));
                    }else{
                        self.exec_phase = ExecutionState::Done;
                    }
                }
                ExecutionState::Done=>return Poll::Ready(None),
            }
        }
    }
}


