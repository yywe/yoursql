use crate::common::schema::Field;
use crate::physical_planner::SchemaRef;
use crate::physical_planner::ExecutionPlan;
use crate::common::schema::EMPTY_SCHEMA_REF;
use crate::physical_planner::SendableRecordBatchStream;
use crate::physical_planner::empty::EmptyExec;
use crate::common::table_reference::TableReference;
use crate::physical_planner::Schema;
use crate::session::SessionState;
use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use std::any::Any;
use crate::storage::memory::MemTable;
#[derive(Clone, Debug)]
pub struct CreateTableExec {
    pub dbname: String,
    pub tblname: String,
    pub fields: Vec<Field>,
}

impl CreateTableExec {
    pub fn new(dbname: String, tblname: String, fields: Vec<Field>) -> Self {
        CreateTableExec{dbname, tblname, fields}
    }
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn Any{
        self
    }
    fn schema(&self) -> SchemaRef{
        Arc::clone(&EMPTY_SCHEMA_REF)
    }
    fn execute(&self, session_state: &SessionState) -> Result<SendableRecordBatchStream>{
        let schema = Arc::new(Schema::new(self.fields.clone(), HashMap::new()));
        let memtable = Arc::new(MemTable::try_new(schema, vec![])?);
        let table_ref = TableReference::Full{database: self.dbname.clone().into(), table: self.tblname.clone().into()};
        session_state.database_for_ref(table_ref)?.register_table(self.tblname.clone(), memtable)?;
        EmptyExec::new(false, Arc::clone(&EMPTY_SCHEMA_REF)).execute(session_state)
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_chilren(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }
}

