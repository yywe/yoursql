use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use super::memory::MemoryStream;
use crate::common::record_batch::RecordBatch;
use crate::common::schema::{Field, Schema, SchemaRef};
use crate::common::types::{DataType, DataValue};
use crate::physical_planner::{ExecutionPlan, SendableRecordBatchStream};
use crate::session::SessionState;

#[derive(Debug)]
pub struct EmptyExec {
    produce_one_row: bool,
    table: SchemaRef,
}

impl EmptyExec {
    pub fn new(produce_one_row: bool, table: SchemaRef) -> Self {
        EmptyExec {
            produce_one_row: produce_one_row,
            table: table,
        }
    }
    pub fn produce_one_row(&self) -> bool {
        self.produce_one_row
    }
    fn data(&self) -> Result<Vec<RecordBatch>> {
        let batch = if self.produce_one_row {
            let n_fields = self.table.fields.len();
            let fields: Vec<Field> = (0..n_fields)
                .map(|i| Field::new(format!("placeholder_{i}"), DataType::Null, true, None))
                .collect();
            let dummy_values: Vec<DataValue> = (0..n_fields).map(|_i| DataValue::Null).collect();
            vec![RecordBatch {
                schema: Arc::new(Schema::new(fields, HashMap::new())),
                rows: vec![dummy_values],
            }]
        } else {
            vec![]
        };
        Ok(batch)
    }
}

impl ExecutionPlan for EmptyExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.table.clone()
    }
    fn execute(&self, _session_state: &SessionState) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.data()?,
            self.schema(),
            None,
        )?))
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(
            self.produce_one_row,
            self.table.clone(),
        )))
    }
}
