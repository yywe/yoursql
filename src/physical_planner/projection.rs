use crate::physical_expr::PhysicalExpr;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use crate::common::types::DataValue;
use crate::common::record_batch::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use crate::common::column::Column;
use crate::common::schema::Field;
use crate::common::schema::{SchemaRef,Schema};
use std::collections::HashMap;

use super::{ExecutionPlan, SendableRecordBatchStream, RecordBatchStream};

#[derive(Debug)]
pub struct ProjectionExec {
    pub expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    schema: SchemaRef, // this is the schema after the projection
    input: Arc<dyn ExecutionPlan>,
}

impl ProjectionExec {
    pub fn try_new(expr: Vec<(Arc<dyn PhysicalExpr>, String)>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let input_schema = input.schema();
        let fields: Result<Vec<Field>> = expr.iter().map(|(e, name)|{
            let mut field = Field::new(name, e.data_type(&input_schema)?, e.nullable(&input_schema)?, None);
            field.set_metadata(get_field_metadata(e, &input_schema).unwrap_or_default());
            Ok(field)
        }).collect();
        let schema = Arc::new(Schema::new_with_metadata(fields?, input_schema.metadata().clone())?);
        Ok(Self { expr: expr, schema: schema, input: input.clone() })
    }

    pub fn expr(&self)->&[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl ExecutionPlan for ProjectionExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn with_new_chilren(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(self.expr.clone(), children[0].clone())?))
    }
    fn execute(&self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(ProjectionStream{
            schema: self.schema.clone(),
            expr: self.expr.iter().map(|x|x.0.clone()).collect(),
            input: self.input.execute()?,
        }))
    }
}

fn get_field_metadata(e: &Arc<dyn PhysicalExpr>, input_schema: &Schema) -> Option<HashMap<String, String>> {
    let name = if let Some(column) = e.as_any().downcast_ref::<Column>(){
        column.name.as_ref()
    }else{
        return None;
    };
    input_schema.field_with_name(None, name).ok().map(|f|f.metadata().clone())
}

struct ProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
}

impl Stream for ProjectionStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x{
            Some(Ok(batch))=>{
                let column_batches = self.expr.iter().map(|e|e.evaluate(&batch)).collect::<Result<Vec<_>>>();
                if column_batches.is_err() {
                    return Some(Err(anyhow!(format!("error when evaluate the batch: {:?}", column_batches.err()))));
                }
                let column_batches = column_batches.ok().unwrap_or(vec![]);
                let n_col = self.expr.len();
                let n_row = batch.rows.len();
                let row_batches: Vec<Vec<DataValue>> = (0..n_row).map(|i|(0..n_col).map(|c|column_batches[c][i].clone()).collect()).collect();
                Some(Ok(RecordBatch{
                    schema: self.schema.clone(),
                    rows: row_batches,
                }))
            }
            other=>other,
        });
       poll
    }
}
impl RecordBatchStream for ProjectionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}