use super::Catalog;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use sled::Db;
use async_fs as fs;
use std::path::Path;
use tracing::info;
use crate::error::StorageError;

pub struct SledStore {
    pub root: Box<Path>,
    pub db: Db,
    pub curdbid: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DbMeta {
    pub id: u32,
    pub name: String,
    pub create_time: DateTime<Local>,
}

impl SledStore {
    pub async fn init(path: &str) -> Result<Self> {
        let root = Path::new(path);
        if !root.exists() {
            info!("the path {} does not exist, creating...", path);
            fs::create_dir_all(path).await?;
        }
        let master_path = root.join("master");
        let master_db = if !master_path.exists() {
            let db = sled::open(root.join("master"))?;
            let dbmeta = DbMeta {
                id: 0,
                name: "master".into(),
                create_time: Local::now(),
            };
            let encoded: Vec<u8> = bincode::serialize(&dbmeta)?;
            db.insert(format!("db{}", 0), encoded)?;
            db.insert("next_dbid", &1u32.to_be_bytes())?;
            db
        } else {
            sled::open(root.join("master"))?
        };
        master_db.flush()?;
        Ok(Self {
            root: root.into(),
            db: master_db,
            curdbid: 0,
        })
    }

    pub fn listdbs(&self) -> Result<Vec<DbMeta>> {
        if self.curdbid != 0 {
            return Err(StorageError::MustInMaster(self.curdbid).into());
        }
        let dblist = self.db.scan_prefix("db").map(|record|{
            let (_, encoded) = record?;
            let decoded: DbMeta = bincode::deserialize(&encoded)?;
            Ok(decoded)
        }).collect::<Result<Vec<_>>>()?;
        Ok(dblist)
    }

}

#[async_trait]
impl Catalog for SledStore {
    async fn create_database(&self, database_name: &String) -> Result<()> {
        if self.curdbid != 0 {
            return Err(StorageError::MustInMaster(self.curdbid).into());
        }
        let dbpath = self.root.join(database_name);
        if dbpath.exists() {
            return Err(StorageError::DBRegistered(database_name.clone()).into());
        }
        if self.listdbs()?.into_iter().find(|meta| meta.name==*database_name).is_some(){
            return Err(StorageError::DBRegistered(database_name.clone()).into());
        }
        // todo add lock when update next_dbid, racing condition
        let next_dbid = match self.db.get("next_dbid")? {
            Some(v) => {
                let bytes: [u8;4] = v.as_ref().try_into()?;
                u32::from_be_bytes(bytes)
            }
            None => return Err(StorageError::NextDBIDNotFound.into())
        };
        sled::open(self.root.join(database_name))?;
        let dbmeta = DbMeta {
            id: next_dbid,
            name: database_name.clone(),
            create_time: Local::now(),
        };
        let encoded: Vec<u8> = bincode::serialize(&dbmeta)?;
        // todo if error should delete the folder
        self.db.insert(format!("db{}", next_dbid), encoded)?;
        self.db.insert("next_dbid", &(next_dbid+1).to_be_bytes())?;
        Ok(())
    }

    async fn drop_database(&self, database_name: &String) -> Result<()> {
        if self.curdbid != 0 {
            return Err(StorageError::MustInMaster(self.curdbid).into());
        }
        let dblist = self.listdbs()?.into_iter().find(|meta| meta.name==*database_name);
        match dblist {
            Some(meta) => {
                self.db.remove(format!("db{}", meta.id))?;
            }
            None => {}
        }
        let dbpath = self.root.join(database_name);
        fs::remove_dir_all(dbpath).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_sledstore() {
        _ = SledStore::init("/home/yy/Learning/Database/yoursql/tdb");
    }
}
