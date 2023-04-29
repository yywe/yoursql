mod sled;
use anyhow::Result;
use async_trait::async_trait;
pub use self::sled::SledStore;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Local};

#[derive(Serialize, Deserialize, Debug)]
pub struct DbMeta {
    pub id: u32,
    pub name: String,
    pub create_time: DateTime<Local>,
}


#[async_trait]
pub trait Catalog {
    async fn create_database(&self, database_name: &String) ->Result<()>;
    async fn drop_database(&self, database_name: &String) -> Result<()>;
    async fn listdbs(&self) -> Result<Vec<DbMeta>>;
    async fn usedb(&mut self, database_name: &String) -> Result<()>;
}