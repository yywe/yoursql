use crate::common::schema::Field;
use crate::common::schema::SchemaRef;
use crate::common::types::{DataType, DataValue};
use anyhow::Result;
use crate::physical_plan::ExecutionPlan;
use std::any::Any;
use crate::common::{record_batch::RecordBatch,schema::Fields};
use crate::physical_plan::SendableRecordBatchStream;
use crate::common::schema::Schema;
use super::memory::MemoryStream;
use std::collections::HashMap;
use std::sync::Arc;

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
                .map(|i| Field::new(format!("placeholder_{i}"), DataType::Null, true,None))
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
    fn as_any(&self) -> &dyn Any{
        self
    }
    fn header(&self) -> Fields{
        self.table.fields.clone()
    }
    fn execute(&self) -> Result<SendableRecordBatchStream>{
        Ok(Box::pin(MemoryStream::try_new(self.data()?, self.header(), None)?))
    }
}

