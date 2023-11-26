use std::fmt::{Debug, Display};
use std::any::Any;
use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use anyhow::Result;
use crate::common::types::DataValue;
use std::sync::Arc;
use std::hash::{Hash, Hasher};

pub mod planner;
pub mod physical_expr;
pub mod accumulator;
pub mod aggregate;


pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>>;
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>)->Result<Arc<dyn PhysicalExpr>>;
    fn dyn_hash(&self, _state: &mut dyn Hasher);
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state)
    }
}