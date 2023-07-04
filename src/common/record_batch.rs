use crate::common::types::DataValue;
use crate::common::types::Fields;
use anyhow::Context;
use anyhow::Result;
#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub header: Fields,
    pub rows: Vec<Vec<DataValue>>,
}
// todo: enrich record batch functions
impl RecordBatch {
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch> {
        let projected_header = self.header.project(indices)?;
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
            header: projected_header,
            rows: projected_rows,
        })
    }
}