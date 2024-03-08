use super::SendableRecordBatchStream;
use crate::{
    common::{record_batch::RecordBatch, schema::SchemaRef},
    physical_planner::{ExecutionPlan, RecordBatchStream},
    session::SessionState,
    storage::Table,
};
use anyhow::Result;
use futures::{ready, Future, FutureExt, Stream, StreamExt};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

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
            pending_insert_fut: None,
        }))
    }
}

struct TableInsertFuture {
    future: Pin<Box<dyn Future<Output = Result<usize>> + Send>>,
}
impl Future for TableInsertFuture {
    type Output = Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
struct InsertStreamAdapter {
    data: SendableRecordBatchStream,
    schema: SchemaRef,
    table: Arc<dyn Table>,
    pending_insert_fut: Option<TableInsertFuture>,
}

impl RecordBatchStream for InsertStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InsertStreamAdapter {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(fut) = &mut self.pending_insert_fut {
                let result = ready!(fut.poll_unpin(cx));
                match result {
                    Ok(_) => {
                        self.pending_insert_fut = None;
                        continue;
                    }
                    Err(e) => {
                        println!("error detected while insert:{:?}", e);
                        self.pending_insert_fut = None;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                }
            } else {
                match ready!(self.data.poll_next_unpin(cx)) {
                    /* took a lot of time to figure things out, there are two concerns
                        here. 1. self.table is immutable reference, we cannot have immutable
                        reference and mutable reference (mut self) at the same time. so by clone
                        self.table and use it, we do not need to use self.table.insert but use
                        the cloned Arc<dyn Table>. 2. the lifetime of cloned Arc<dyn Table>, must
                        live together with the whole future. so here we use async move and package
                        the cloned_tbl to the future instance and addressed the lifetime issue
                        if we remove the move keyword, will see cloned_tbl does not live enough.
                    */
                    Some(Ok(rows)) => {
                        let cloned_tbl = Arc::clone(&self.table);
                        let fut = async move {
                            let out = cloned_tbl.insert(rows).await;
                            out
                        };
                        self.pending_insert_fut = Some(TableInsertFuture {
                            future: Box::pin(fut),
                        });
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}
