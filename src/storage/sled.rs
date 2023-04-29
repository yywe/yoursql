use super::Catalog;
use super::DbMeta;
use crate::error::StorageError;
use anyhow::Result;
use async_fs as fs;
use async_trait::async_trait;
use chrono::Local;
use sled::Db;
use std::path::Path;
use tracing::info;

pub struct SledStore {
    pub root: Box<Path>,
    pub db: Db,
    pub curdbid: u32,
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
        info!("init finished with master path {:?}",master_path);
        Ok(Self {
            root: root.into(),
            db: master_db,
            curdbid: 0,
        })
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
            .listdbs()
            .await?
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
            .listdbs()
            .await?
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
    async fn listdbs(&self) -> Result<Vec<DbMeta>> {
        let mdb = match self.curdbid {
            0 => self.db.clone(),
            _=>sled::open(self.root.join("master"))?,
        };
        let dblist = mdb
            .scan_prefix("db")
            .map(|record| {
                let (_, encoded) = record?;
                let decoded: DbMeta = bincode::deserialize(&encoded)?;
                Ok(decoded)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(dblist)
    }

    async fn usedb(&mut self, database_name: &String) -> Result<()> {
        info!("use database {}", database_name);
        let dbpath = self.root.join(database_name);
        if !dbpath.exists() {
            return Err(StorageError::DBNotExist(database_name.clone()).into());
        }
        let targetdb = self
            .listdbs()
            .await?
            .into_iter()
            .find(|meta| meta.name == *database_name);
        if targetdb.is_none() {
            return Err(StorageError::DBNotExist(database_name.clone()).into());
        }
        self.db = sled::open(self.root.join(database_name))?;
        self.curdbid = targetdb.unwrap().id;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    impl Drop for SledStore {
        fn drop(&mut self) {
            _ = std::fs::remove_dir_all(self.root.as_os_str());
        }
    }
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
        let mut ss = SledStore::init(test_folder).await?;
        let initdbs = ss.listdbs().await?;
        assert_eq!(initdbs.len(), 1);
        assert_eq!(initdbs[0].name, "master");
        ss.create_database(&"newdb".into()).await?;
        let alldbs = ss.listdbs().await?;
        assert_eq!(alldbs.len(), 2);
        assert_eq!(alldbs[1].name, "newdb");
        ss.usedb(&String::from("newdb")).await?;
        assert_eq!(1, ss.curdbid);
        //switch back to master before we can drop the db
        ss.usedb(&String::from("master")).await?;
        ss.drop_database(&"newdb".into()).await?;
        let afterdrop = ss.listdbs().await?;
        assert_eq!(afterdrop.len(), 1);
        Ok(())
    }
}
