use chrono::Utc;
use sled::Db;
use std::fs;
use std::path::Path;
use super::Catalog;
use async_trait::async_trait;
use anyhow::Result;
use anyhow::anyhow;
use tracing::info;
use chrono::{Local,DateTime};
use serde::{Serialize, Deserialize};

pub struct SledStore {
    pub root: Box<Path>,
    pub db: Db,
}


#[derive(Serialize, Deserialize,Debug)]
pub struct DbMeta {
    pub id: u8,
    pub name: String,
    pub create_time: DateTime<Local>,
}

impl SledStore {
    pub fn new(path: &str) -> Result<Self> {
        let root = Path::new(path);
        if !root.exists() {
            info!("the path {} does not exist, creating...", path);
            fs::create_dir_all(path)?;
        }
        let master_path = root.join("master");
        let master_db = if !master_path.exists(){
            let db = sled::open(root.join("master"))?;
            db.insert("next_dbid", &1u8.to_be_bytes())?;
            
            let dbmeta = DbMeta{id: 0, name: "master".into(), create_time: Local::now()};
            println!("the original is: {:#?}", dbmeta);


            let encoded: Vec<u8> = bincode::serialize(&dbmeta)?;
            println!("the encoded is: {:#?}", encoded);
            let decoded: DbMeta = bincode::deserialize(&encoded)?;

            println!("the decoded is: {:#?}", decoded);

            db
        }else{
            sled::open(root.join("master"))?
        };
        
        Ok(Self { root: root.into(), db: master_db})
    }
}

#[async_trait]
impl Catalog for SledStore {
    async fn create_database(&self, database_name: &String) ->Result<()>{
       let dbpath = self.root.join(database_name);
       
       sled::open(dbpath)?;
        Ok(())
    }
    async fn drop_database(&self, database_name: &String) -> Result<()>{

        Ok(())
    }
}

#[cfg(test)]
mod test{
    use super::*;
    #[test]
    fn test_sledstore() {
        _ = SledStore::new("/home/yy/Learning/Database/yoursql/tdb");
    }
}



