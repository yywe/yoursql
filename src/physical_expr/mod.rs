use std::fmt::{Debug, Display};
use std::any::Any;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use anyhow::Result;

pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    
}