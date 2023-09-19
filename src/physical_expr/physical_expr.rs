use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use crate::common::types::DataValue;
use anyhow::Result;
use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::PhysicalExpr;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Literal {
    value: DataValue,
}

impl Literal {
    pub fn new(value: DataValue) -> Self {
        Self { value }
    }
    pub fn value(&self) -> &DataValue {
        &self.value
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for Literal {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.get_datatype())
    }
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let dim = batch.rows.len();
        Ok(vec![self.value.clone();dim])
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

}

impl PartialEq<dyn Any> for Literal {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::schema::Field;
    use std::collections::HashMap;
    #[test]
    fn test_literal() -> Result<()> {
        let mylit = Literal::new(DataValue::Int32(Some(32)));
        let schema = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false, None),
                Field::new("name", DataType::Utf8, false,None),
                Field::new("age", DataType::Int8, false,None),
                Field::new("address", DataType::Utf8, false,None),
            ],
            HashMap::new(),
        );
        let schema_ref = Arc::new(schema);
        let row_batch = vec![
            vec![DataValue::Int64(Some(1)), DataValue::Utf8(Some("John".into())), DataValue::Int8(Some(20)), DataValue::Utf8(Some("100 bay street".into()))],
            vec![DataValue::Int64(Some(2)), DataValue::Utf8(Some("Andy".into())), DataValue::Int8(Some(21)), DataValue::Utf8(Some("121 hunter street".into()))],
        ];
        let record_batch = RecordBatch {
            schema: schema_ref.clone(),
            rows: row_batch.clone(),
        };
        let ans = mylit.evaluate(&record_batch)?;
        assert_eq!(ans.len(),2);
        assert_eq!(ans[0], DataValue::Int32(Some(32)));
        Ok(())
    }
}
