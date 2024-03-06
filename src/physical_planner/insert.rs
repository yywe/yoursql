use super::SendableRecordBatchStream;
use crate::common::record_batch::RecordBatch;
use crate::common::schema::SchemaRef;
use crate::physical_planner::ExecutionPlan;
use crate::physical_planner::RecordBatchStream;
use crate::session::SessionState;
use crate::storage::Table;
use anyhow::Result;
use futures::Stream;
use futures::StreamExt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct InsertExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    table: Arc<dyn Table>,
}

impl Debug for InsertExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InsertExec schema: {:?}", self.schema)
    }
}

impl InsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projected_schema: SchemaRef,
        table: Arc<dyn Table>,
    ) -> Self {
        Self {
            input,
            schema,
            projected_schema,
            table: table.clone(),
        }
    }
}

impl ExecutionPlan for InsertExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            projected_schema: self.projected_schema.clone(),
            table: self.table.clone(),
        }))
    }
    fn execute(&self, session_state: &SessionState) -> Result<SendableRecordBatchStream> {
        let data = self.input.execute(session_state)?;
        Ok(Box::pin(InsertStreamAdapter {
            data: data,
            schema: self.schema.clone(),
            table: self.table.clone(),
        }))
    }
}

struct InsertStreamAdapter {
    data: SendableRecordBatchStream,
    schema: SchemaRef,
    table: Arc<dyn Table>,
}

impl RecordBatchStream for InsertStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InsertStreamAdapter {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.data.poll_next_unpin(cx) {
                Poll::Ready(batch) => match batch {
                    Some(Ok(rows)) => {
                        println!("insert into rows:{:?}", rows);
                        match self.table.insert(rows) {
                            Ok(_) => continue,
                            Err(e) => {
                                //TODO: add output log for this case
                                println!("error detected while insert:{:?}", e);
                                poll = Poll::Ready(None);
                                break;
                            }
                        }
                    }
                    // if error or done, break the loop
                    _ => {
                        poll = Poll::Ready(None);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }
        poll
    }
}
