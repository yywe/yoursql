use crate::common::types::DataType;
use crate::common::types::Fields;
#[derive(Clone, Debug)]
pub struct RecordBatch {
    header: Fields,
    rows: Vec<Vec<DataType>>,
}