use super::Storage;
use super::{DbMeta,Table, Row, Value,IndexValue, Batch};
use crate::error::StorageError;
use anyhow::Result;
use async_fs as fs;
use async_trait::async_trait;
use chrono::Local;
use sled::Db;
use std::collections::HashSet;
use std::path::Path;
use tracing::info;
use anyhow::{anyhow,Context};
use sled::Iter;

pub struct SledStore {
    pub root: Box<Path>,
    pub db: Db,
    pub curdbid: u32,
    pub batchsize: usize,
}

impl SledStore {
    pub async fn init(path: &str, batchsize: usize) -> Result<Self> {
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
            batchsize: batchsize,
        })
    }

    // validate if the row is legal to insert/update to table
    pub fn validate_row(&self, table: &Table, row: &Row, is_updaterow: Option<u64>) -> Result<()> {
        if row.len() != table.columns.len() {
            return Err(StorageError::RowError(table.name.clone(),"length does not match".into()).into());
        }
        for (index, (column, value)) in table.columns.iter().zip(row.iter()).enumerate(){
            // validate data type
            match value.datatype() {
                None if column.nullable => {},
                None => return Err(StorageError::RowError(table.name.clone(),"column cannot be null".into()).into()),
                Some(datatype)=>{
                    if datatype != column.datatype {
                        return Err(StorageError::RowError(table.name.clone(),"data type mismatch".into()).into());
                    }
                },
            }
            // if the column is a foreign key
            if let Some(target) = &column.references {
                match value {
                    Value::Null => {},
                    _=>{
                        let fkvalue = self.read_indices(target, &column.name, value)?;
                        if fkvalue.is_none() {
                            return Err(StorageError::RowError(table.name.clone(),"refered primary key does not exists".into()).into());
                        }
                    },
                }
            }
            // primary key validation
            if column.primary_key {
                if let Some(row2udpate) = is_updaterow {
                    let oldrow = self.read_row_inner(table.name.as_str(), row2udpate)?;
                    match oldrow{
                        Some(row)=>{
                            let oldpk = table.get_row_pk(&row).unwrap_or(Value::Null);
                            // if primary key get updated, need to ensure not conflict with other pk
                            if oldpk != *value{
                                let pkindex = self.read_indices(table.name.as_ref(), &column.name, value)?;
                                if pkindex.is_some() {
                                    return Err(StorageError::RowError(table.name.clone(),"primary key already exists".into()).into());
                                }
                            }
                        }
                        None=> return Err(StorageError::RowError(table.name.clone(),"row does not exists".into()).into()),
                    }
                }else{
                    let pkindex = self.read_indices(table.name.as_ref(), &column.name, value)?;
                    if pkindex.is_some() {
                        return Err(StorageError::RowError(table.name.clone(),"primary key already exists".into()).into());
                    }
                }
            }
            // other uniqueness validation,non-primary key, needs full scan (assume no index)
            if column.unique && !column.primary_key && value != &Value::Null {
                let mut records = self.db.scan_prefix(format!("table/{}/", table.name));
                while let Some((k, v)) = records.next().transpose()? {
                    // be careful for update case cause of the uniqueness constrain
                    let existing: Row = bincode::deserialize(&v)?;
                    if existing.get(index).unwrap_or(&Value::Null)==value {
                        if let Some(update_rowid) = is_updaterow {
                            if k.as_ref() != format!("table/{}/{}",table.name, update_rowid).as_bytes() {
                                return Err(StorageError::RowError(table.name.clone(),"column value not unique".into()).into());
                            }
                            // if we are update this row, no need to check
                        }else{
                            return Err(StorageError::RowError(table.name.clone(),"column value not unique".into()).into());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<Table>{
        let key = format!("catalog/{}", name);
        match self.db.get(&key)? {
            Some(ivec)=>{
                let table: Table = bincode::deserialize(&ivec)?;
                return Ok(table);
            },
            None => return Err(StorageError::InvalidTableName(name.into(), String::from("table does not exists")).into()),
        }
    }

    pub fn read_row_inner(&self, table: &str, id: u64)->Result<Option<Row>> {
        let rowkey = format!("table/{}/{}", table, id);
        match self.db.get(&rowkey)? {
            Some(ivec)=>{
                let row: Row = bincode::deserialize(&ivec)?;
                Ok(Some(row))
            },
            None=> Ok(None),
        }
    }

    pub fn read_indices(&self, table: &str, column: &str, value: &Value) -> Result<Option<IndexValue>> {
        let indexkey = format!("index/{}/{}/{}", table, column, value);
        match self.db.get(&indexkey)? {
            Some(ivec)=>{
                Ok(Some(bincode::deserialize(&ivec)?))
            },
            None=> Ok(None),
        }
    }
}

#[async_trait]
impl Storage for SledStore {
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

    async fn listtbls(&self) -> Result<Vec<Table>>{
        self.db.scan_prefix("catalog/")
        .map(|record| {
            let (_, encoded) = record?;
            let decoded: Table = bincode::deserialize(&encoded)?;
            Ok(decoded)
        })
        .collect::<Result<Vec<_>>>()
    }

    async fn create_table(&self, table: &Table) -> Result<()>{
        if table.name == "catalog" {
            return Err(StorageError::InvalidTableName(table.name.clone(),String::from("catalog cannot be table name")).into());
        }
        let key = format!("catalog/{}", table.name);
        if self.db.get(&key)?.is_some() {
            return Err(StorageError::InvalidTableName(table.name.clone(), String::from("table name alreay exists")).into());
        }
        self.db.insert(key, bincode::serialize(&table)?)?;
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()>{
        info!("dropping table:{}",name);
        if name == "catalog" {
            return Err(StorageError::InvalidTableName(name.into(),String::from("catalog cannot be table name")).into());
        } 
        let table_key = format!("catalog/{}", name);
        match self.db.get(&table_key)?{
            Some(_)=>{
                // delete table catalog
                self.db.remove(table_key)?;
                // delete table records
                let mut rows = self.db.scan_prefix(format!("table/{}/", name));
                while let Some((id, _)) = rows.next().transpose()? {
                    self.db.remove(id)?;
                }
                // delete table index
                let mut rows = self.db.scan_prefix(format!("index/{}/", name));
                while let Some((id, _)) = rows.next().transpose()? {
                    self.db.remove(id)?;
                }
            },
            None=>  return Err(StorageError::InvalidTableName(name.into(),String::from("table does not exists")).into()),
        }
        Ok(())
    }

    async fn insert_row(&self, table: &str, row: Row) -> Result<u64>{
        info!("insert row to table :{}",table);
        let tabledef = self.get_table(table)?;
        self.validate_row(&tabledef, &row, None)?;
        //todo generate id
        let rowid = self.db.generate_id()?;
        let rowkey = format!("table/{}/{}",table, rowid);
        let rowvalue = bincode::serialize(&row)?;
        self.db.insert(rowkey, rowvalue)?;
        // insert pk if has one
        if let Some(pkcolumn) = tabledef.get_pk_name(){
            let pk = tabledef.get_row_pk(&row);
            if let Some(pkv) = pk {
                let indexkey = format!("index/{}/{}/{}", table, pkcolumn, pkv);
                // for pk since unique, the hashset size is 1
                let indexvalue = bincode::serialize(&IndexValue(HashSet::from([rowid])))?;
                self.db.insert(indexkey, indexvalue)?;
            }
        }
        Ok(rowid)
    }
    async fn read_row(&self, table: &str, id: u64)->Result<Option<Row>>{
        return self.read_row_inner(table, id);
    }
    async fn update_row(&self, table: &str, id: u64, row: Row) -> Result<()>{
        info!("update row to table :{}",table);
        let tabledef = self.get_table(table)?;
        self.validate_row(&tabledef, &row, Some(id))?;
        let rowkey = format!("table/{}/{}",table, id);
        let oldrow = self.read_row_inner(table, id)?.context("row does not exist")?;
        let newrow = bincode::serialize(&row)?;
        self.db.insert(rowkey, newrow)?;
        // if table exists pk and the pk gets updated, need to update the pk index as well
        if let Some(pkfieldname) = tabledef.get_pk_name(){
            let oldpk = tabledef.get_row_pk(&oldrow).context("fail to get pk")?;
            let newpk = tabledef.get_row_pk(&row).context("fail to get pk")?;
            if oldpk != newpk {
                // delete old pk index, and add new pk index
                let oldindexkey = format!("index/{}/{}/{}", table, pkfieldname, oldpk);
                self.db.remove(oldindexkey)?;
                let newindexkey = format!("index/{}/{}/{}", table, pkfieldname, newpk);
                let indexvalue = bincode::serialize(&IndexValue(HashSet::from([id])))?;
                self.db.insert(newindexkey, indexvalue)?;
            }
        }
        //todo: support index for non-primary key, similarlly when update, also update index
        Ok(())
    }
    async fn delete_row(&self, table: &str, id: u64) -> Result<()> {
        info!("delete row {} from table :{}", id,table);
        let rowkey = format!("table/{}/{}",table, id);
        let oldrow = self.read_row_inner(table, id)?.context("row does not exist")?;
        self.db.remove(rowkey)?;
        let tabledef = self.get_table(table)?;
        if let Some(pkfieldname) = tabledef.get_pk_name(){
            let oldpk = tabledef.get_row_pk(&oldrow).context("fail to get pk")?;
            let oldindexkey = format!("index/{}/{}/{}", table, pkfieldname, oldpk);
            self.db.remove(oldindexkey)?;
        }
        //todo: delete non-pk index if later add support
        Ok(())
    }

    async fn scan_table(&self, table: &str) -> Result<Batch> {
        let rowiter =  self.db.scan_prefix(format!("table/{}/", table));
        Ok(Box::new(BatchIter{
            max_size: self.batchsize,
            rowiter: rowiter,
        }))        
    }
}

pub struct BatchIter {
    pub max_size: usize,
    pub rowiter: Iter,
}

impl Iterator for BatchIter {
    type Item = Result<Vec<Row>>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut local_batch: Vec<Row> = Vec::with_capacity(self.max_size);
        while let Some(res) = self.rowiter.next() {
           match res {
            Ok((_, value)) => {
                let row = bincode::deserialize(&value);
                match row {
                    Ok(row) => {
                        local_batch.push(row);
                        if local_batch.len() >= self.max_size {
                         return Some(Ok(local_batch)); 
                        }
                    },
                    Err(e)=>{
                        return Some(Err(anyhow!(e.to_string())));
                    }
                }
            },
            Err(error)=>{
                return Some(Err(anyhow!(error.to_string())));
            }
           }
        }
        if local_batch.len()>0{
            return Some(Ok(local_batch)); 
        }
        None
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use super::super::Column;
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
        let mut ss = SledStore::init(test_folder,2).await?;
        let initdbs = ss.listdbs().await?;
        assert_eq!(initdbs.len(), 1);
        assert_eq!(initdbs[0].name, "master");
        ss.create_database(&"newdb".into()).await?;
        let alldbs = ss.listdbs().await?;
        assert_eq!(alldbs.len(), 2);
        assert_eq!(alldbs[1].name, "newdb");
        ss.usedb(&String::from("newdb")).await?;
        assert_eq!(1, ss.curdbid);

        let tables = ss.listtbls().await?;
        assert_eq!(tables.len(),0);
        let test_table = Table {
            name: "testtable".into(),
            columns: vec![Column{name: "column1".into(), primary_key: true, ..Column::default()},
                          Column{name: "column2".into(), ..Column::default()},
                          Column{name: "column3".into(), ..Column::default()}]
        };
        ss.create_table(&test_table).await?;
        let tables = ss.listtbls().await?;
        assert_eq!(tables.len(),1);
        //info!("The list of tables:{:#?}", tables);
        let got_table = ss.get_table("testtable")?;
        assert_eq!(got_table, test_table);

        let testrow0: Row = vec![Value::String("c1".into()), Value::String("c2".into()),Value::String("c3".into())];
        let ansrowid = ss.insert_row("testtable", testrow0.clone()).await?;
        let ansrow = ss.read_row("testtable", ansrowid).await?;
        assert_eq!(Some(testrow0), ansrow);
       
        let testrow1: Row = vec![Value::String("c1".into()), Value::String("c2".into()),Value::String("c3".into())];
        let ansrow1 = ss.insert_row("testtable", testrow1).await;
        assert_eq!(ansrow1.is_err(), true);

        let testrow2: Row = vec![Value::String("c11".into()), Value::String("c2".into()),Value::String("c3".into())];
        let row2id = ss.insert_row("testtable", testrow2.clone()).await?;

        let ansrow2 = ss.read_row("testtable", row2id).await?;
        assert_eq!(Some(testrow2), ansrow2);

        let updaterow: Row = vec![Value::String("c11".into()), Value::String("c22".into()),Value::String("c33".into())];
        ss.update_row("testtable", row2id, updaterow.clone()).await?;

        let ansrow2 = ss.read_row("testtable", row2id).await?;
        assert_eq!(Some(updaterow), ansrow2);

        ss.delete_row("testtable", row2id).await?;
        let ansrow3 = ss.read_row("testtable", row2id).await?;
        assert_eq!(None, ansrow3);
        
        let testrow4: Row = vec![Value::String("c41".into()), Value::String("c42".into()),Value::String("c43".into())];
        ss.insert_row("testtable", testrow4).await?;
        let testrow5: Row = vec![Value::String("c51".into()), Value::String("c52".into()),Value::String("c53".into())];
        ss.insert_row("testtable", testrow5).await?;
        let testrow6: Row = vec![Value::String("c61".into()), Value::String("c62".into()),Value::String("c63".into())];
        ss.insert_row("testtable", testrow6).await?;
        let testrow7: Row = vec![Value::String("c71".into()), Value::String("c72".into()),Value::String("c73".into())];
        ss.insert_row("testtable", testrow7).await?;

        let mut scanres = ss.scan_table("testtable").await?;
        while let Some(batch) = scanres.next(){
            //info!("got batch:{:?}",batch);
            assert_eq!(batch.is_ok(), true);
        }
        ss.drop_table("testtable").await?;
        let tables = ss.listtbls().await?;
        assert_eq!(tables.len(),0);
    
        //switch back to master before we can drop the db
        ss.usedb(&String::from("master")).await?;
        ss.drop_database(&"newdb".into()).await?;
        let afterdrop = ss.listdbs().await?;
        assert_eq!(afterdrop.len(), 1);
        Ok(())
    }
}
