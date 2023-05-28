use crate::execution::Storage;
use async_trait::async_trait;
use crate::execution::Executor;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::Mutex;
use super::ResultSet;
use crate::storage::Table;
pub struct CreateDatabase {
    database: String
}
impl CreateDatabase {
    pub fn new(database: String) -> Box<Self> {
        Box::new(Self{database})
    }
}

#[async_trait]
impl<T: Storage+'static> Executor<T> for CreateDatabase {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
        store.lock().await.create_database(&self.database).await?;
        Ok(ResultSet::CreateDatabase { name: self.database.clone()})
    }
}

pub struct ShowDatabase;
impl ShowDatabase {
    pub fn new() -> Box<Self> {
        Box::new(ShowDatabase)
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for ShowDatabase {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
        let ans = store.lock().await.listdbs().await?;
        Ok(ResultSet::ShowDatabase { dblist: ans })
    }
}
pub struct DropDatabase {
    database: String
}
impl DropDatabase {
    pub fn new(database: String) -> Box<Self> {
        Box::new(Self{database})
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for DropDatabase {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
        store.lock().await.drop_database(&self.database).await?;
        Ok(ResultSet::DropDatabase { name: self.database.clone() })
    }
}

pub struct UseDatabase {
    database: String
}
impl UseDatabase {
    pub fn new(database: String) -> Box<Self> {
        Box::new(Self{database})
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for UseDatabase {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
        store.lock().await.usedb(&self.database).await?;
        Ok(ResultSet::UseDatabase { name: self.database.clone() })
    }
}

pub struct CreateTable {
    table: Table,
}

impl CreateTable {
    pub fn new(table: Table) -> Box<Self> {
        Box::new(Self{table})
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for CreateTable {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
       store.lock().await.create_table(&self.table).await?;
       Ok(ResultSet::CreateTable { name: self.table.name.clone() })
    }
}

pub struct DropTable {
    table: String,
}
impl DropTable {
    pub fn new(table: String) -> Box<Self> {
        Box::new(Self{table})
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for DropTable {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
       store.lock().await.drop_table(self.table.as_str()).await?;
       Ok(ResultSet::DropTable { name: self.table.clone() })
    }
}

pub struct ShowTable;
impl ShowTable {
    pub fn new() -> Box<Self> {
        Box::new(ShowTable)
    }
}
#[async_trait]
impl<T: Storage+'static> Executor<T> for ShowTable {
    async fn execute(self: Box<Self>, store:Arc<Mutex<T>>) -> Result<ResultSet> {
      let tables = store.lock().await.listtbls().await?;
        Ok(ResultSet::ShowTable { tblist: tables })
    }
}

#[cfg(test)]
mod test{
    use super::*;
    use crate::execution::print_resultset;
    use crate::storage::SledStore;
    use anyhow::Result;
    use crate::storage::Column;
    use crate::execution::test::gen_test_db;
    #[tokio::test]
    async fn test_db_op() -> Result<()> {
        let ss: SledStore = gen_test_db("tdbop".into()).await?;
        //ss.usedb(&"master".into()).await?;

        // use master
        let usedbop = UseDatabase::new("master".into());
        let db = Arc::new(Mutex::new(ss));
        let res = usedbop.execute(db.clone()).await?;
        print_resultset(res).await?;

        // create database
        let createdbop = CreateDatabase::new("newdb2".into());
        let res = createdbop.execute(db.clone()).await?;
        print_resultset(res).await?;
        // list database
        let listdbop = ShowDatabase::new();
        let res = listdbop.execute(db.clone()).await?;
        print_resultset(res).await?;

        // drop database
        let dropdbop = DropDatabase::new("newdb2".into());
        let res = dropdbop.execute(db.clone()).await?;
        print_resultset(res).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tb_op() -> Result<()> {
        let ss: SledStore = gen_test_db("ttbop".into()).await?;
        let db = Arc::new(Mutex::new(ss));
        // create table
        let test_table = Table {
            name: "testtable2".into(),
            columns: vec![
                Column {
                    name: "mycol1".into(),
                    primary_key: true,
                    ..Column::default()
                },
                Column {
                    name: "mycol2".into(),
                    ..Column::default()
                },
                Column {
                    name: "mycol3".into(),
                    ..Column::default()
                },
            ],
        };
        let createtbop = CreateTable::new(test_table);
        let res = createtbop.execute(db.clone()).await?;
        print_resultset(res).await?;
        // list tables
        let listtbop = ShowTable::new();
        let res = listtbop.execute(db.clone()).await?;
        print_resultset(res).await?;

        // drop table
        let droptbop = DropTable::new("testtable2".into());
        let res = droptbop.execute(db.clone()).await?;
        print_resultset(res).await?;

        // show table again
        let listtbop = ShowTable::new();
        let res = listtbop.execute(db.clone()).await?;
        print_resultset(res).await?;
        Ok(())

    }
}