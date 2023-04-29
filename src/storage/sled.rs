use super::Catalog;
use crate::error::StorageError;
use anyhow::Result;
use async_fs as fs;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use tracing::info;

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
        let dblist = self
            .db
            .scan_prefix("db")
            .map(|record| {
                let (_, encoded) = record?;
                let decoded: DbMeta = bincode::deserialize(&encoded)?;
                Ok(decoded)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(dblist)
    }
}

#[async_trait]
impl Catalog for SledStore {
    async fn create_database(&self, database_name: &String) -> Result<()> {
        info!("creating database {}", database_name);
        if self.curdbid != 0 {
            return Err(StorageError::MustInMaster(self.curdbid).into());
        }
        let dbpath = self.root.join(database_name);
        if dbpath.exists() {
            return Err(StorageError::DBRegistered(database_name.clone()).into());
        }
        if self
            .listdbs()?
            .into_iter()
            .find(|meta| meta.name == *database_name)
            .is_some()
        {
            return Err(StorageError::DBRegistered(database_name.clone()).into());
        }
        // todo add lock when update next_dbid, racing condition
        let next_dbid = match self.db.get("next_dbid")? {
            Some(v) => {
                let bytes: [u8; 4] = v.as_ref().try_into()?;
                u32::from_be_bytes(bytes)
            }
            None => return Err(StorageError::NextDBIDNotFound.into()),
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
        self.db
            .insert("next_dbid", &(next_dbid + 1).to_be_bytes())?;
        Ok(())
    }

    async fn drop_database(&self, database_name: &String) -> Result<()> {
        info!("droping database {}", database_name);
        if self.curdbid != 0 {
            return Err(StorageError::MustInMaster(self.curdbid).into());
        }
        let dblist = self
            .listdbs()?
            .into_iter()
            .find(|meta| meta.name == *database_name);
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
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    #[tokio::test]
    async fn test_sledstore() -> Result<()> {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
            .add_directive("sled=off".parse().unwrap());
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(env_filter)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        let test_folder = "./tdb";
        let ss = SledStore::init(test_folder).await?;
        let initdbs = ss.listdbs()?;
        assert_eq!(initdbs.len(), 1);
        assert_eq!(initdbs[0].name, "master");
        ss.create_database(&"newdb".into()).await?;
        let alldbs = ss.listdbs()?;
        assert_eq!(alldbs.len(), 2);
        assert_eq!(alldbs[1].name, "newdb");
        ss.drop_database(&"newdb".into()).await?;
        let afterdrop = ss.listdbs()?;
        assert_eq!(afterdrop.len(), 1);
        fs::remove_dir_all(test_folder).await?;
        Ok(())
    }
}
