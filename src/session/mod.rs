use crate::catalog::MemoryDB;
use crate::catalog::DB;
use crate::catalog::MemoryDBList;
use crate::catalog::DBList;
use crate::common::config::ConfigOptions;
use crate::common::table_reference::OwnedTableReference;
use crate::common::table_reference::ResolvedTableReference;
use crate::common::table_reference::TableReference;
use crate::logical_planner::ParserOptions;
use crate::logical_planner::object_name_to_table_refernce;
use crate::physical_planner::DefaultPhysicalPlanner;
use crate::physical_planner::PhysicalPlanner;
use crate::storage::Table;
use crate::expr::logical_plan::LogicalPlan;
use crate::logical_planner::PlannerContext;
use crate::logical_planner::LogicalPlanner;
use anyhow::Context;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use parking_lot::RwLock;
use std::sync::Arc;
use std::ops::ControlFlow;
use uuid::Uuid;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use crate::physical_planner::ExecutionPlan;
use crate::common::record_batch::RecordBatch;

#[derive(Clone)]
pub struct SessionContext {
    pub session_id: String,
    pub session_start_time: DateTime<Utc>,
    pub state: Arc<RwLock<SessionState>>,
}

#[derive(Clone)]
pub struct SessionState {
    session_id: String,
    databases: Arc<dyn DBList>,
    config: ConfigOptions,
}

struct ReferredTables<'a>{
    state: &'a SessionState,
    tables: HashMap<String, Arc<dyn Table>>,
}

impl<'a> PlannerContext for ReferredTables<'a> {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn Table>>{
        let name = self.state.resolve_table_ref(name).to_string();
        self.tables.get(&name).cloned().context(format!("table '{name}' not found"))
    }
    fn options(&self) -> &ConfigOptions{
        &self.state.config
    }
}

impl SessionState {
    pub fn new(config: ConfigOptions, databases: Arc<dyn DBList>) -> Self {
        let session_id = Uuid::new_v4().to_string();
        Self {
            session_id,
            databases,
            config,
        }
    }
    pub fn catalogs(&self) -> Arc<dyn DBList> {
        self.databases.clone()
    }
    pub fn config(&self) -> &ConfigOptions {
        &self.config
    }
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        table_ref.into().resolve(
            &self.config.catalog.default_database
        )
    }

    pub fn database_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn DB>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        self.databases
            .database(&resolved_ref.database)
            .context(format!("failed to resolve database:{}", resolved_ref.database))
    }

    /// create logical plan based on AST statment
    pub async fn make_logical_plan(&self, statement: sqlparser::ast::Statement) -> Result<LogicalPlan> {
        let references = self.extract_table_references(&statement)?;

        let mut referred_tables = ReferredTables {
            state: self,
            tables: HashMap::with_capacity(references.len()),
        };

        let enable_ident_normalization = self.config.sql_parser.enable_ident_normalization;
        let parse_float_as_decimal = self.config.sql_parser.parse_float_as_decimal;

        for reference in references {
            let table = reference.table_name();
            let resolved = self.resolve_table_ref(&reference);
            if let Entry::Vacant(v) = referred_tables.tables.entry(resolved.to_string()) {
                if let Ok(database) = self.database_for_ref(resolved) {
                    if let Some(table_provider) = database.get_table(table).await {
                        v.insert(table_provider);
                    }
                }
            }
        }

        let planner = LogicalPlanner::new_with_options(&referred_tables, ParserOptions {
            parse_float_as_decimal,
            enable_ident_normalization,
        });

        planner.statement_to_plan(statement)
    }

    /// create a physical plan based on logical plan
    pub async fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        // first optimze the logical plan
        let logical_plan = self.logical_optimize(logical_plan)?;

        let planner = DefaultPhysicalPlanner::default();
        // create the physical plan and do the physical optimization
        let physical_plan = planner.create_physical_plan(&logical_plan, self).await?;
        planner.physical_optimize(physical_plan, self)
    }

    /// logical optimization
    pub fn logical_optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        //todo: impl logical optimize
        println!("todo: implement logical optimizer");
        Ok(plan.clone())
    }

    /// finally, execute the physical plan (optimized) and get result, i.e, vector of batch.
    pub async fn execute_physical_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        let stream = plan.execute()?;
        stream.try_collect::<Vec<_>>().await
    }

    /// for test purpose, print the result record batches
    pub fn print_record_batches(&self, batches: &Vec<RecordBatch>) {
        let batch_iters = batches.iter();
        let mut i = 0;
        for batch in batch_iters {
            if i == 0 {
                let header = batch.schema.all_fields().iter().map(|f|(*f).clone()).collect::<Vec<_>>();
                println!("{}", header.iter().map(|f|f.name().as_str()).collect::<Vec<_>>().join("|"));
            }
            for row in &batch.rows {
                println!("{}", row.iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"))
            }
            i+=1;
        }
    }

    /// Parse (Walk) the AST and extract tables referred in this statement
    /// This used the Visitor Interface in sqlparser package
    /// https://docs.rs/sqlparser/latest/sqlparser/ast/trait.Visitor.html
    /// need to enable visitor feature from Cargo.toml
    pub fn extract_table_references(&self, statement: &sqlparser::ast::Statement) -> Result<Vec<OwnedTableReference>> {
        use sqlparser::ast::*;
        // container to hold the used relations
        let mut relations = hashbrown::HashSet::with_capacity(10);
        
        // visitor action, when hit a reltion, insert (a clone) into the visitor
        struct RelationVisitor<'a>(&'a mut hashbrown::HashSet<ObjectName>);
        impl<'a> RelationVisitor<'a> {
            fn insert(&mut self, relation: &ObjectName) {
                self.0.get_or_insert_with(relation, |_|relation.clone());
            }
        }
        impl<'a> Visitor for RelationVisitor<'a> {
            type Break = ();
            fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
                self.insert(relation);
                ControlFlow::Continue(())
            }
            fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
                if let Statement::ShowCreate { obj_type: ShowCreateObject::Table | ShowCreateObject::View, obj_name } = statement {
                    self.insert(obj_name)
                }
                ControlFlow::Continue(())
            }
        }
        // create a visitor and visit
        let mut visitor = RelationVisitor(&mut relations);
        statement.visit(&mut visitor);

        // collect the owned table reference
        let enable_ident_normalization = self.config.sql_parser.enable_ident_normalization;
        relations.into_iter().map(|x| object_name_to_table_refernce(x, enable_ident_normalization)).collect::<Result<_>>()
    }

}

impl SessionContext {
    pub fn new_inmemory_ctx() -> Self {
        let config = ConfigOptions::new();
        let databases = MemoryDBList::new();
        if config.catalog.create_default_catalog_and_schema {
            let default_database = MemoryDB::new();
            databases.register_database(
                config.catalog.default_database.clone(),
                Arc::new(default_database),
            );
        }
        let state = SessionState::new(config, Arc::new(databases));
        Self {
            session_id: state.session_id.clone(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.state.read().databases.database_names()
    }
    pub fn database(&self, name: &str) -> Option<Arc<dyn DB>> {
        self.state.read().databases.database(name)
    }
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
    }

    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    pub fn register_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
        table: Arc<dyn Table>,
    ) -> Result<Option<Arc<dyn Table>>> {
        let table_ref = table_ref.into();
        let table_name = table_ref.table_name().to_owned();
        self.state
            .read()
            .database_for_ref(table_ref)?
            .register_table(table_name, table)
    }

    pub fn register_database(&self, name: impl Into<String>, database: Arc<dyn DB>) -> Option<Arc<dyn DB>> {
        let name = name.into();
        self.state.read().databases.register_database(name, database)
    }

    pub fn state(&self) -> SessionState {
        self.state.read().clone()
    }
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new_inmemory_ctx()
    }
}


#[cfg(test)]
pub mod test {
    use super::*;
    use crate::common::record_batch::RecordBatch;
    use crate::common::types::DataType;
    use crate::common::types::DataValue;
    use crate::common::schema::Field;
    use crate::common::schema::Schema;
    use crate::common::table_reference::OwnedTableReference;
    use crate::storage::empty::EmptyTable;
    use crate::storage::memory::MemTable;
    use crate::parser::parse;
    use anyhow::Result;
    use futures::StreamExt;
    use std::collections::HashMap;

    pub fn init_mem_testdb(session: &mut SessionContext) -> Result<()> {
        let test_database = MemoryDB::new();
        session.register_database("testdb", Arc::new(test_database));
        // first table: student
        let table1 = OwnedTableReference::Full {
            database: "testdb".to_string().into(),
            table: "student".to_string().into(),
        };
        let table1_def = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false,Some(table1.clone())),
                Field::new("name", DataType::Utf8, false,Some(table1.clone())),
                Field::new("age", DataType::Int8, false,Some(table1.clone())),
                Field::new("address", DataType::Utf8, false,Some(table1.clone())),
            ],
            HashMap::new(),
        );
        let table1_ref = Arc::new(table1_def);
        let row_batch1 = vec![
            vec![DataValue::Int64(Some(1)), DataValue::Utf8(Some("John".into())), DataValue::Int8(Some(20)), DataValue::Utf8(Some("100 bay street".into()))],
            vec![DataValue::Int64(Some(2)), DataValue::Utf8(Some("Andy".into())), DataValue::Int8(Some(21)), DataValue::Utf8(Some("121 hunter street".into()))],
        ];
        let row_batch2 = vec![
            vec![DataValue::Int64(Some(3)), DataValue::Utf8(Some("Angela".into())), DataValue::Int8(Some(18)), DataValue::Utf8(Some("172 carolin street".into()))],
            vec![DataValue::Int64(Some(4)), DataValue::Utf8(Some("Jingya".into())), DataValue::Int8(Some(22)), DataValue::Utf8(Some("100 main street".into()))],
        ];
        let batch1 = RecordBatch {
            schema: table1_ref.clone(),
            rows: row_batch1.clone(),
        };
        let batch2 = RecordBatch {
            schema: table1_ref.clone(),
            rows: row_batch2.clone(),
        };
        let memtable1 = MemTable::try_new(table1_ref, vec![batch1, batch2])?;
        let table_ref1 = Arc::new(memtable1);
        session.register_table(table1, table_ref1.clone())?;
    
        // 2nd table: course
        let table2 = OwnedTableReference::Full {
            database: "testdb".to_string().into(),
            table: "course".to_string().into(),
        };
        let table2_def = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false,Some(table2.clone())),
                Field::new("name", DataType::Utf8, false,Some(table2.clone())),
                Field::new("instructor", DataType::Utf8, false,Some(table2.clone())),
                Field::new("classroom", DataType::Utf8, false,Some(table2.clone())),
            ],
            HashMap::new(),
        );
        let table2_ref = Arc::new(table2_def);
        let row_batch1 = vec![
            vec![DataValue::Int64(Some(1)), DataValue::Utf8(Some("Math".into())), DataValue::Utf8(Some("JK Roll".into())), DataValue::Utf8(Some("ROOM 202".into()))],
            vec![DataValue::Int64(Some(2)), DataValue::Utf8(Some("Physicas".into())), DataValue::Utf8(Some("Dr Liu".into())), DataValue::Utf8(Some("ROOM 201".into()))],
        ];
        let row_batch2 = vec![
            vec![DataValue::Int64(Some(3)), DataValue::Utf8(Some("Chemistry".into())), DataValue::Utf8(Some("Dr Zheng".into())), DataValue::Utf8(Some("ROOM 101".into()))],
            vec![DataValue::Int64(Some(4)), DataValue::Utf8(Some("Music".into())), DataValue::Utf8(Some("Dr Wang".into())), DataValue::Utf8(Some("ROOM 102".into()))],
        ];
        let batch1 = RecordBatch {
            schema: table2_ref.clone(),
            rows: row_batch1.clone(),
        };
        let batch2 = RecordBatch {
            schema: table2_ref.clone(),
            rows: row_batch2.clone(),
        };
        let memtable2 = MemTable::try_new(table2_ref, vec![batch1, batch2])?;
        let table_ref2 = Arc::new(memtable2);
        session.register_table(table2, table_ref2.clone())?;

        // 3rd table: student enroll course
        let table3 = OwnedTableReference::Full {
            database: "testdb".to_string().into(),
            table: "enroll".to_string().into(),
        };
        let table3_def = Schema::new(
            vec![
                Field::new("student_id", DataType::Int64, false,Some(table3.clone())),
                Field::new("couse_id", DataType::Int64, false,Some(table3.clone())),
                Field::new("score", DataType::Int64, false,Some(table3.clone())),
            ],
            HashMap::new(),
        );
        let table3_ref = Arc::new(table3_def);
        let row_batch1 = vec![
            vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(1)),DataValue::Int64(Some(100))],
            vec![DataValue::Int64(Some(2)), DataValue::Int64(Some(4)),DataValue::Int64(Some(98))],
        ];
        let row_batch2 = vec![
            vec![DataValue::Int64(Some(3)), DataValue::Int64(Some(2)),DataValue::Int64(Some(110))],
            vec![DataValue::Int64(Some(4)), DataValue::Int64(Some(3)),DataValue::Int64(Some(120))],
        ];
        let batch1 = RecordBatch {
            schema: table3_ref.clone(),
            rows: row_batch1.clone(),
        };
        let batch2 = RecordBatch {
            schema: table3_ref.clone(),
            rows: row_batch2.clone(),
        };
        let memtable3 = MemTable::try_new(table3_ref, vec![batch1, batch2])?;
        let table_ref3 = Arc::new(memtable3);
        session.register_table(table3, table_ref3.clone())?;
        Ok(())
    }

    #[test]
    fn test_register_table() -> Result<()> {
        let session = SessionContext::default();
        let empty_table = EmptyTable::new(Arc::new(Schema::new(
            vec![
                Field::new("a", DataType::Int64, false,  None),
                Field::new("b", DataType::Boolean, false, None),
            ],
            HashMap::new(),
        )));
        let table_referene = TableReference::Bare {
            table: "testa".into(),
        };
        // register to master database
        session.register_table(table_referene.clone(), Arc::new(empty_table))?;
        // assert "testa" exist in default(master) database
        let database = session.state.read().database_for_ref(table_referene)?;
        assert_eq!(database.table_exist("testa"), true);
        Ok(())
    }

    #[tokio::test]
    async fn test_memtable_scan() -> Result<()> {
        let mut session = SessionContext::default();
        init_mem_testdb(&mut session)?;
        let testdb = session.database("testdb").unwrap();
        let student_table = testdb.get_table("student").await.unwrap();
        let exec = student_table.scan(&session.state(), None, &[]).await?;
        let mut it = exec.execute()?;
        // note 1st unwrap is for option, 2nd the item of the stream is Result of RecordBatch, so use ?
        let fetch_batch1: RecordBatch = it.next().await.unwrap()?;
        //println!("first batch:{:?}", fetch_batch1);
        assert_eq!(fetch_batch1.rows.len(), 2);
        let fetch_batch2: RecordBatch = it.next().await.unwrap()?;
        //println!("2nd batch:{:?}", fetch_batch2);
        assert_eq!(fetch_batch2.rows.len(), 2);
        let done  = it.next().await;
        assert_eq!(true, done.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_logical_planner() -> Result<()> {
        let mut session = SessionContext::default();
        init_mem_testdb(&mut session)?;
        let sql = "SELECT A.id, A.name, B.score from testdb.student A inner join testdb.enroll B on A.id=B.student_id inner join testdb.course C on A.id=C.id where B.score > 99 order by B.score";
        let statement = parse(sql).unwrap();
        let _referred_tables = session.state.read().extract_table_references(&statement).unwrap();
        //println!("referered tables: {:#?}", _referred_tables);
        let _ans_plan = session.state.read().make_logical_plan(statement).await;
        //println!("logical plan:{}", _ans_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_physical_planner() -> Result<()> {
        let mut session = SessionContext::default();
        init_mem_testdb(&mut session)?;
        let sql = "SELECT id, name from testdb.student where age <19";
        let statement = parse(sql).unwrap();
        let logical_plan = session.state.read().make_logical_plan(statement).await?;
        println!("logical plan:{:?}", logical_plan);
        let physical_plan = session.state.read().create_physical_plan(&logical_plan).await?;
        let record_batches = session.state.read().execute_physical_plan(physical_plan).await?;
        session.state.read().print_record_batches(&record_batches);
        Ok(())
    }
}
