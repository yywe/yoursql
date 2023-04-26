mod sled;
use anyhow::Result;
use async_trait::async_trait;

pub use self::sled::SledStore;

#[async_trait]
pub trait Catalog {
    async fn create_database(&self, database_name: &String) ->Result<()>;
    async fn drop_database(&self, database_name: &String) -> Result<()>;
}