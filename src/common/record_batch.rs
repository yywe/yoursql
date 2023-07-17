use crate::common::types::DataValue;
use super::schema::SchemaRef;
use anyhow::Context;
use anyhow::Result;
use std::sync::Arc;


#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<DataValue>>,
}

impl RecordBatch {
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch> {
        let projected_schema = self.schema.project(indices)?;
        let projected_rows = self
            .rows
            .iter()
            .map(|row| {
                indices
                    .iter()
                    .map(|i| {
                        row.get(*i)
                            .cloned()
                            .context(format!("project index {} out of bounds", i))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch {
            schema: Arc::new(projected_schema),
            rows: projected_rows,
        })
    }
}