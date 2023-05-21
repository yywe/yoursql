use crate::execution::Storage;
use async_trait::async_trait;
use crate::execution::Executor;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::Mutex;
use super::ResultSet;
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

#[cfg(test)]
mod test{
    use super::*;
    use crate::execution::print_resultset;
    use crate::storage::SledStore;
    use anyhow::Result;
    use crate::execution::test::gen_test_db;
    #[tokio::test]
    async fn test_db_op() -> Result<()> {
        let mut ss: SledStore = gen_test_db("tdbop".into()).await?;
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
}