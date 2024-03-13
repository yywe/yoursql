use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use crate::common::schema::{Field, EMPTY_SCHEMA_REF};
use crate::common::table_reference::TableReference;
use crate::physical_planner::empty::EmptyExec;
use crate::physical_planner::{ExecutionPlan, Schema, SchemaRef, SendableRecordBatchStream};
use crate::session::SessionState;
use crate::storage::memory::MemTable;
#[derive(Clone, Debug)]
pub struct CreateTableExec {
    pub dbname: String,
    pub tblname: String,
    pub fields: Vec<Field>,
}

impl CreateTableExec {
    pub fn new(dbname: String, tblname: String, fields: Vec<Field>) -> Self {
        CreateTableExec {
            dbname,
            tblname,
            fields,
        }
    }
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&EMPTY_SCHEMA_REF)
    }
    fn execute(&self, session_state: &SessionState) -> Result<SendableRecordBatchStream> {
        // note have to add table qualifier. but it is okay. cause logical plan will inject qualifer
        // when do scan and physical plan (and expression) does not need qualifer as it
        // using column index
        let schema = Arc::new(Schema::new(self.fields.clone(), HashMap::new()));
        let memtable = Arc::new(MemTable::try_new(schema, vec![])?);
        let table_ref = TableReference::Full {
            database: self.dbname.clone().into(),
            table: self.tblname.clone().into(),
        };
        session_state
            .database_for_ref(table_ref)?
            .register_table(self.tblname.clone(), memtable)?;
        // we need a stream object, just take advantage of empty exec
        EmptyExec::new(false, Arc::clone(&EMPTY_SCHEMA_REF)).execute(session_state)
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_chilren(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }
}
