use crate::common::types::DataType;
use crate::common::types::TableRef;
#[derive(Clone, Debug)]
pub struct RecordBatch {
    schema: TableRef,
    rows: Vec<Vec<DataType>>,
}