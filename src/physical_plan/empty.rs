use crate::common::types::Field;
use crate::common::types::TableRef;
use crate::common::types::{DataType, DataValue};
use anyhow::Result;
use crate::physical_plan::ExecutionPlan;
use std::any::Any;
use crate::common::{record_batch::RecordBatch,types::Fields};
use crate::physical_plan::SendableRecordBatchStream;

use super::memory::MemoryStream;

#[derive(Debug)]
pub struct EmptyExec {
    produce_one_row: bool,
    table: TableRef,
}

impl EmptyExec {
    pub fn new(produce_one_row: bool, table: TableRef) -> Self {
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
                .map(|i| Field::new(format!("placeholder_{i}"), DataType::Null, true))
                .collect();
            let dummy_values: Vec<DataValue> = (0..n_fields).map(|_i| DataValue::Null).collect();
            vec![RecordBatch {
                header: fields.into(),
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

