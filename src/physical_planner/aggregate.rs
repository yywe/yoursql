use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use crate::common::record_batch::RecordBatch;
use crate::common::schema::{Field, Schema, SchemaRef};
use crate::common::types::DataValue;
use crate::physical_expr::accumulator::Accumulator;
use crate::physical_expr::aggregate::AggregateExpr;
use crate::physical_expr::PhysicalExpr;
use crate::physical_planner::ExecutionState;
use crate::physical_planner::utils::transpose_matrix;
use anyhow::Result;
use core::cmp::min;
use futures::ready;
use futures::stream::BoxStream;
use futures::Stream;
use futures::StreamExt;
use hashbrown::raw::RawTable;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::task::Poll;

#[derive(Clone, Debug, Default)]
pub struct PhysicalGroupBy {
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
}

impl PhysicalGroupBy {
    pub fn new(expr: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
        Self { expr }
    }
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }
    pub fn is_empty(&self) -> bool {
        self.expr.is_empty()
    }
}

impl PartialEq for PhysicalGroupBy {
    fn eq(&self, other: &Self) -> bool {
        self.expr.len() == other.expr.len()
            && self
                .expr
                .iter()
                .zip(other.expr.iter())
                .all(|((expr1, name1), (expr2, name2))| expr1.eq(expr2) && name1 == name2)
    }
}

#[derive(Debug)]
pub struct AggregateExec {
    pub group_by: PhysicalGroupBy,
    pub aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    pub input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

pub struct AggregateStream {
    stream: BoxStream<'static, Result<RecordBatch>>,
    schema: SchemaRef,
}

//each accumulator has one vec of expressions since the accumulator
//input may has multiple expressions
struct AggregateStreamInner {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    accumulators: Vec<Box<dyn Accumulator>>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    finished: bool,
}

impl Stream for AggregateStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for AggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

///aggregate the batch
fn aggregate_batch(
    batch: RecordBatch,
    accumulators: &mut [Box<dyn Accumulator>],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
    accumulators
        .iter_mut()
        .zip(expressions)
        .try_for_each(|(accum, expr)| {
            let values = expr
                .iter()
                .map(|e| e.evaluate(&batch))
                .collect::<Result<Vec<_>>>()?;
            let values: Vec<&[DataValue]> = values.iter().map(|v| v.as_slice()).collect();
            accum.update_batch(values.as_slice())
        })?;
    Ok(())
}

/// get 1 singe row from aggregator for the case of aggregation without group by
fn finalize_aggregation(accumulators: &[Box<dyn Accumulator>]) -> Result<Vec<Vec<DataValue>>> {
    let aggregated_values = accumulators
        .iter()
        .map(|acc| acc.evaluate())
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![aggregated_values])
}

/// in the case when there are no group by, there is only one single row in the final output
impl AggregateStream {
    pub fn new(agg: &AggregateExec) -> Result<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let input = agg.input.execute()?;
        let aggregate_expressions = aggregate_expressions(&agg.aggr_expr)?;
        let accumulators = create_accumulators(&agg.aggr_expr)?;
        let inner = AggregateStreamInner {
            schema: Arc::clone(&agg.schema),
            input,
            accumulators,
            aggregate_expressions,
            finished: false,
        };

        // below impl is somewhat weird. essentially, the final result is just one row
        // can directly compute the row and generate a new stream
        // here it create a stream which first get all data and aggregate and return the stream.
        // use this.finished
        let stream = futures::stream::unfold(inner, |mut this| async move {
            if this.finished {
                return None;
            }
            loop {
                let result = match this.input.next().await {
                    Some(Ok(batch)) => {
                        match aggregate_batch(
                            batch,
                            &mut this.accumulators,
                            &this.aggregate_expressions,
                        ) {
                            Ok(_) => continue, // continue loop fetch next record if okay
                            Err(e) => Err(e),  // get error result
                        }
                    }
                    Some(Err(e)) => Err(e),
                    None => {
                        this.finished = true;
                        let agg_result =
                            finalize_aggregation(&this.accumulators).and_then(|rows| {
                                Ok(RecordBatch {
                                    schema: this.schema.clone(),
                                    rows: rows,
                                })
                            });
                        agg_result
                    }
                };
                //set finished to be true
                //in the case of error, previous this.finished= true will not be executed
                this.finished = true;
                // this will return from the loop regardless of success or failure
                //however, if success, the result has the aggregate result
                // if failure, this break the loop
                return Some((result, this));
            }
        });
        let stream = stream.fuse(); // fuse the stream
        let stream = Box::pin(stream);
        Ok(Self {
            stream: stream,
            schema: agg_schema,
        })
    }
}



pub struct GroupState {
    pub group_by_values: Vec<DataValue>,
    pub accumulator_set: Vec<Box<dyn Accumulator>>,
    pub indices: Vec<u32>, //temp for each batch
}

// the state of all the groups
pub struct AggregationState {
    pub map: RawTable<(u64, usize)>,
    pub group_states: Vec<GroupState>,
}

// again note each AggregateExpr has a vec physicalExpr as input
pub struct GroupedHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
    agg_exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    group_by: PhysicalGroupBy,
    batch_size: usize,
    exec_state: ExecutionState,
    aggr_state: AggregationState,
    row_group_skip_position: usize,
}

impl GroupedHashAggregateStream {
    pub fn new(agg: &AggregateExec) -> Result<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let batch_size = 2; //todo: let it be configurable
        let input = agg.input.execute()?;
        let all_aggregate_expresions = aggregate_expressions(&agg.aggr_expr)?;
        let aggr_state = AggregationState {
            map: RawTable::with_capacity(0),
            group_states: Vec::with_capacity(0),
        };
        let exec_state = ExecutionState::ReadingInput;
        Ok(GroupedHashAggregateStream {
            schema: agg_schema,
            input: input,
            aggregate_expr: agg.aggr_expr.clone(),
            agg_exprs: all_aggregate_expresions,
            group_by: agg_group_by,
            batch_size: batch_size,
            exec_state: exec_state,
            aggr_state: aggr_state,
            row_group_skip_position: 0,
        })
    }

    /// maintain the groups, groups_values is the group by values part of multi-rows
    fn update_group_state(&mut self, group_values: Vec<&[DataValue]>) -> Result<Vec<usize>> {
        let batch_hashes = group_values
            .iter()
            .map(|&group| compute_group_hash(group))
            .collect::<Vec<_>>();
        let AggregationState { map, group_states } = &mut self.aggr_state;
        let mut groups_with_rows = vec![];

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            //although hash designed to avoid conflict, but still it can, here to ensure equality
            let entry = map.get_mut(hash, |(_hash, group_idx)| {
                let group_state = &group_states[*group_idx];
                let row_values = group_values[row];
                group_state.group_by_values == row_values
            });

            match entry {
                //the group key already exists
                Some((_hash, group_idx)) => {
                    let group_state = &mut group_states[*group_idx];
                    // why we need this?
                    // groups_with_rows stores the group index that needs an update due to comin data
                    // in the case of a new group_state it will not be empty since initied with :indices: vec![row as u32]
                    // however, in the next batch, the group_state.indices will be cleared
                    // so whenever we found it is empty, means this is the first time we hit an existing group value
                    // otherwise not empty means, this group has already been added
                    // we add the group index for the first time using if empty
                    // actually, we may consider use a set to do this.
                    if group_state.indices.is_empty() {
                        groups_with_rows.push(*group_idx);
                    };
                    group_state.indices.push(row as u32);
                }
                //not existing key, a new group come in, add it
                None => {
                    let accumulator_set = create_accumulators(&self.aggregate_expr)?;
                    let group_state = GroupState {
                        group_by_values: group_values[row].iter().map(|x| x.clone()).collect(),
                        accumulator_set,
                        indices: vec![row as u32],
                    };
                    let group_idx = group_states.len();
                    map.insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);
                    group_states.push(group_state);
                    groups_with_rows.push(group_idx);
                }
            }
        }
        Ok(groups_with_rows)
    }

    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // get group by values, each Vec<DataValue> is a group by key
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;
        // now get input values to aggregate, 3D vec, 1D->accumulator 2D->physical expr 3D->batch
        let aggr_input_values = evaluate_many(&self.agg_exprs, &batch)?;
        // now for each group by key, update its accumulator set using aggr_input_values
        // note for apache arrow datafusion, group_by_values is 3D cause it supports group set
        // here group by values is 2D since no group set
        let groups_with_rows =
            self.update_group_state(group_by_values.iter().map(|x| x.as_slice()).collect())?;
        // now we can update the accumulators
        self.update_group_accumulators(&groups_with_rows, &aggr_input_values)?;
        Ok(())
    }

    fn update_group_accumulators(
        &mut self,
        groups_with_rows: &[usize],
        accu_exprs_values: &Vec<Vec<Vec<DataValue>>>,
    ) -> Result<()> {
        for group_idx in groups_with_rows {
            let group_state = &mut self.aggr_state.group_states[*group_idx];
            // collect the indices in the batch that have the hash = group state hash
            let indices_in_batch = &group_state.indices;
            // get the corresonpding aggregate input values for the given group, note it is in 3rd DIM
            let state_agg_input_values: Vec<Vec<Vec<DataValue>>> = accu_exprs_values
                .iter()
                .map(|agg_vec| {
                    agg_vec
                        .iter()
                        .map(|batch_vec| {
                            batch_vec
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, value)| {
                                    if indices_in_batch.contains(&(idx as u32)) {
                                        Some(value.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            //finally, now we are ready to update the accumulators
            group_state
                .accumulator_set
                .iter_mut()
                .zip(state_agg_input_values)
                .try_for_each(|(accumulator, aggr_value_vec)| {
                    let sliced_values = aggr_value_vec
                        .iter()
                        .map(|x| x.as_slice())
                        .collect::<Vec<_>>();
                    let sliced_values = sliced_values.as_slice();
                    accumulator.update_batch(sliced_values)
                })?;
            //last clear the indices;
            group_state.indices.clear();
        }
        Ok(())
    }
    /// create record batch with its all group keys and accumulators' state values
    fn create_batch_from_map(&mut self) -> Result<Option<RecordBatch>> {
        let skip_items = self.row_group_skip_position;
        if skip_items > self.aggr_state.group_states.len() {
            return Ok(None);
        }
        if self.aggr_state.group_states.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch {
                schema: schema,
                rows: vec![],
            }));
        }
        let end_idx = min(
            skip_items + self.batch_size,
            self.aggr_state.group_states.len(),
        );
        let group_state_chunk = &self.aggr_state.group_states[skip_items..end_idx];
        if group_state_chunk.is_empty() {
            let schema = self.schema.clone();
            return Ok(Some(RecordBatch {
                schema: schema,
                rows: vec![],
            }));
        }
        let group_keys = group_state_chunk
            .iter()
            .map(|gs| gs.group_by_values.clone())
            .collect::<Vec<_>>();
        let acc_values = group_state_chunk
            .iter()
            .map(|gs| {
                gs.accumulator_set
                    .iter()
                    .map(|acc| acc.evaluate())
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = group_keys
            .into_iter()
            .zip(acc_values.into_iter())
            .map(|(key, acc)| key.into_iter().chain(acc.into_iter()).collect())
            .collect();
        return Ok(Some(RecordBatch {
            schema: self.schema.clone(),
            rows: rows,
        }));
    }
}

///evaluate the exprs for accumulator, 3D array here.
/// 1st DIM is for each aggregator
/// 2nd DIM since each agregator, may have multiple physical expressions
/// 3rd DIM each single physical expression has a vec of values from batch
fn evaluate_many(
    exprs: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<Vec<DataValue>>>> {
    let mut accu_exprs_values = vec![];
    for expr in exprs {
        let accu_values = expr
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        accu_exprs_values.push(accu_values);
    }
    Ok(accu_exprs_values)
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.exec_state {
                ExecutionState::ReadingInput => match ready!(self.input.poll_next_unpin(cx)) {
                    Some(Ok(batch)) => {
                        let result = self.group_aggregate_batch(batch);
                        if let Err(e) = result {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    None => {
                        self.exec_state = ExecutionState::ProducingOutput;
                    }
                },
                ExecutionState::ProducingOutput => {
                    let result = self.create_batch_from_map();
                    self.row_group_skip_position += self.batch_size;
                    match result {
                        Ok(Some(batch)) => {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Ok(None) => {
                            self.exec_state = ExecutionState::Done;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}
impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

enum StreamType {
    AggregateStream(AggregateStream),
    GroupedHashAggregateStream(GroupedHashAggregateStream),
}

impl From<StreamType> for SendableRecordBatchStream {
    fn from(s: StreamType) -> Self {
        match s {
            StreamType::AggregateStream(stream) => Box::pin(stream),
            StreamType::GroupedHashAggregateStream(stream) => Box::pin(stream),
        }
    }
}

///based on group by expression and aggregate expression create the result schema
fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            expr.nullable(input_schema)?,
            None,
        ))
    }
    for expr in aggr_expr {
        fields.push(expr.field()?)
    }
    Ok(Schema::new(fields, HashMap::new()))
}

impl AggregateExec {
    pub fn try_new(
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_by.expr, &aggr_expr)?;
        let schema = Arc::new(schema);
        Ok(AggregateExec {
            group_by,
            aggr_expr,
            input,
            schema,
        })
    }

    fn execute_typed(&self) -> Result<StreamType> {
        if self.group_by.expr.is_empty() {
            Ok(StreamType::AggregateStream(AggregateStream::new(self)?))
        } else {
            Ok(StreamType::GroupedHashAggregateStream(
                GroupedHashAggregateStream::new(self)?,
            ))
        }
    }
}

/// get the physical expressions (2D array) for aggregate expression (1D) since each Agg Expr may has multiple physical exprs
fn aggregate_expressions(
    agg_expr: &[Arc<dyn AggregateExpr>],
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    Ok(agg_expr
        .iter()
        .map(|agg| agg.expressions())
        .collect::<Vec<_>>())
}

fn create_accumulators(aggr_expr: &[Arc<dyn AggregateExpr>]) -> Result<Vec<Box<dyn Accumulator>>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

fn compute_group_hash(group: &[DataValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    group.hash(&mut hasher);
    hasher.finish()
}

impl ExecutionPlan for AggregateExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            self.group_by.clone(),
            self.aggr_expr.clone(),
            children[0].clone(),
        )?))
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        self.execute_typed().map(|stream| stream.into())
    }
}

/// evaluate the group by expression, pay attention to the format
/// here each expr will generate a vector
/// but we need transpose the matrix so each row is is the group key
fn evaluate_group_by(
    group_by: &PhysicalGroupBy,
    batch: &RecordBatch,
) -> Result<Vec<Vec<DataValue>>> {
    let expr_values = group_by
        .expr
        .iter()
        .map(|(e, _name)| e.evaluate(batch))
        .collect::<Result<Vec<_>>>()?;
    transpose_matrix(expr_values)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_hash() -> Result<()> {
        println!(
            "the hash is: {}",
            compute_group_hash(&[DataValue::Int16(Some(11)), DataValue::Float32(Some(12.0))])
        );
        Ok(())
    }
}
