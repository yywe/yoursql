use std::sync::Arc;

use anyhow::{Context, Result};

use super::schema::SchemaRef;
use crate::common::types::DataValue;

#[derive(Clone, Debug, PartialEq)]
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
    // get data of specific at specific column index
    pub fn column(&self, index: usize) -> Vec<DataValue> {
        self.rows.iter().map(|row| row[index].clone()).collect()
    }
    pub fn new(schema: SchemaRef, rows: Vec<Vec<DataValue>>) -> Self {
        Self { schema, rows }
    }
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn to_string_table(&self) -> String {
        if self.rows.is_empty() {
            return String::from("Empty set");
        }

        // Get column names and calculate column widths
        let column_names: Vec<&str> = self.schema.fields.iter().map(|f| f.name().as_str()).collect();
        let mut column_widths: Vec<usize> = column_names.iter().map(|name| name.len()).collect();

        // Calculate the maximum width needed for each column
        for row in &self.rows {
            for (col_idx, value) in row.iter().enumerate() {
                let value_str = self.format_data_value(value);
                column_widths[col_idx] = column_widths[col_idx].max(value_str.len());
            }
        }

        let mut result = String::new();

        // Add top border
        result.push_str(&self.create_border(&column_widths));
        result.push('\n');

        // Add header row
        result.push('|');
        for (i, name) in column_names.iter().enumerate() {
            result.push_str(&format!(" {:<width$} |", name, width = column_widths[i]));
        }
        result.push('\n');

        // Add separator after header
        result.push_str(&self.create_border(&column_widths));
        result.push('\n');

        // Add data rows
        for row in &self.rows {
            result.push('|');
            for (col_idx, value) in row.iter().enumerate() {
                let value_str = self.format_data_value(value);
                result.push_str(&format!(" {:<width$} |", value_str, width = column_widths[col_idx]));
            }
            result.push('\n');
        }

        // Add bottom border
        result.push_str(&self.create_border(&column_widths));
        result.push('\n');

        // Add row count
        let row_count = self.rows.len();
        let row_text = if row_count == 1 { "row" } else { "rows" };
        result.push_str(&format!("{} {} in set\n", row_count, row_text));

        result
    }

    /// Create a border line for the table
    fn create_border(&self, column_widths: &[usize]) -> String {
        let mut border = String::from("+");
        for &width in column_widths {
            border.push_str(&"-".repeat(width + 2)); // +2 for spaces around content
            border.push('+');
        }
        border
    }

    /// Format a DataValue for display
    fn format_data_value(&self, value: &DataValue) -> String {
        match value {
            DataValue::Null => "NULL".to_string(),
            DataValue::Boolean(Some(b)) => b.to_string(),
            DataValue::Boolean(None) => "NULL".to_string(),
            DataValue::Int8(Some(i)) => i.to_string(),
            DataValue::Int8(None) => "NULL".to_string(),
            DataValue::Int16(Some(i)) => i.to_string(),
            DataValue::Int16(None) => "NULL".to_string(),
            DataValue::Int32(Some(i)) => i.to_string(),
            DataValue::Int32(None) => "NULL".to_string(),
            DataValue::Int64(Some(i)) => i.to_string(),
            DataValue::Int64(None) => "NULL".to_string(),
            DataValue::UInt8(Some(i)) => i.to_string(),
            DataValue::UInt8(None) => "NULL".to_string(),
            DataValue::UInt16(Some(i)) => i.to_string(),
            DataValue::UInt16(None) => "NULL".to_string(),
            DataValue::UInt32(Some(i)) => i.to_string(),
            DataValue::UInt32(None) => "NULL".to_string(),
            DataValue::UInt64(Some(i)) => i.to_string(),
            DataValue::UInt64(None) => "NULL".to_string(),
            DataValue::Float32(Some(f)) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f) // Show at least one decimal place
                } else {
                    f.to_string()
                }
            },
            DataValue::Float32(None) => "NULL".to_string(),
            DataValue::Float64(Some(f)) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f) // Show at least one decimal place
                } else {
                    f.to_string()
                }
            },
            DataValue::Float64(None) => "NULL".to_string(),
            DataValue::Utf8(Some(s)) => s.clone(),
            DataValue::Utf8(None) => "NULL".to_string(),
            DataValue::Binary(Some(bytes)) => {
                // Display binary data as hex
                format!("Binary({} bytes)", bytes.len())
            },
            DataValue::Binary(None) => "NULL".to_string(),
            _=> "Unsupported".to_string(), // Handle other types as needed
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::schema::{Field, Schema};
    use crate::common::types::{DataType, DataValue};
    #[allow(dead_code)]
    pub fn make_test_record_batch(num_rows: usize) -> RecordBatch {
        let schema = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false, None),
                Field::new("score", DataType::Float32, false, None),
                Field::new("name", DataType::Utf8, false, None),
                Field::new("active", DataType::Boolean, false, None),
            ],
            HashMap::new(),
        );
        let schema_ref = Arc::new(schema);
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            rows.push(vec![
                DataValue::Int64(Some(i as i64)),
                DataValue::Float32(Some(100.0 + i as f32)),
                DataValue::Utf8(Some(format!("user{}", i))),
                DataValue::Boolean(Some(i % 2 == 0)),
            ]);
        }
        RecordBatch {
            schema: schema_ref,
            rows,
        }
    }
}
