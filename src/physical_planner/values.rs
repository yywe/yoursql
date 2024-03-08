use super::{ExecutionPlan, SendableRecordBatchStream};
use crate::{
    common::{record_batch::RecordBatch, schema::SchemaRef, types::DataValue},
    physical_expr::PhysicalExpr,
    physical_planner::memory::MemoryStream,
    session::SessionState,
};
use anyhow::{anyhow, Result};
use std::sync::Arc;

#[derive(Debug)]
pub struct ValuesExec {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl ValuesExec {
    pub fn try_new(schema: SchemaRef, data: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow!("Value list cannot be empty"));
        }
        let n_col = schema.fields().len();
        /*
        cause the logical values are expressions, we need to evaluate it to get values.
        construct dummy batch, check the literal evaluate function:
            fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
                let dim = batch.rows.len();
                Ok(vec![self.value.clone(); dim])
            }
        here just need the dim
        */
        let dummy_batch = RecordBatch {
            schema: schema.clone(),
            rows: vec![vec![DataValue::Null; n_col]; 1],
        };
        let mut rows = vec![];
        for exprs in data.into_iter() {
            let mut row = vec![];
            for e in exprs {
                row.push(e.evaluate(&dummy_batch)?[0].clone())
            }
            rows.push(row);
        }
        let ret_batch = RecordBatch {
            schema: schema.clone(),
            rows: rows,
        };
        Ok(Self {
            schema: schema,
            data: vec![ret_batch],
        })
    }

    fn data(&self) -> Vec<RecordBatch> {
        self.data.clone()
    }
}

impl ExecutionPlan for ValuesExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ValuesExec {
            schema: self.schema.clone(),
            data: self.data.clone(),
        }))
    }
    fn execute(&self, _session_state: &SessionState) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.data(),
            self.schema.clone(),
            None,
        )?))
    }
}
