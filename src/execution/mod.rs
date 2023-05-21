mod source;
mod query;
mod catalog;
use crate::storage::Row;
use crate::storage::Storage;
use async_trait::async_trait;
use anyhow::Error;
use futures::StreamExt;
use futures_async_stream::try_stream;
use std::pin::Pin;
use futures::Stream;
use anyhow::Result;
use derivative::Derivative;
use std::sync::Arc;
use std::sync::mpsc::channel;
use tokio::sync::Mutex;
use crate::storage::DbMeta;
use crate::storage::Table;


const MAX_BATCH_SIZE: usize = 2;

// column def in result is very simple, even column name is optional
#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
}
pub type Columns = Vec<Column>;
pub type Rows = Vec<Row>;
pub type RowStream = Pin<Box<dyn Stream<Item = Result<Rows>> + Send>>;
#[derive(Derivative)]
#[derivative(Debug)]
pub enum ResultSet {
    Query {
        columns: Columns,
        #[derivative(Debug="ignore")]
        rows: RowStream,
    },
    CreateDatabase {
        name: String,
    },
    ShowDatabase {
        dblist: Vec<DbMeta>,
    },
    DropDatabase {
        name: String,
    },
    UseDatabase {
        name: String,
    },
    CreateTable {
        name: String,
    },
    ShowTable {
        tblist: Vec<Table>,
    },
    DropTable {
        name: String,
    },
}


// print the resultset, will consume the stream
pub async fn print_resultset(res: ResultSet) -> Result<()> {
    match res {
        ResultSet::Query{columns, rows}=>{
            println!("{}", columns.iter().map(|c|c.name.as_deref().unwrap_or("?")).collect::<Vec<_>>().join("|"));
            let mut resstream: Pin<Box<dyn Stream<Item = std::result::Result<Vec<Vec<crate::storage::Value>>, Error>> + Send>> = rows;
            /* 
            // for the case of synchrouns (i.e, remove async keyword of this func so we can pull data from stream)
            let (sender, receiver) = channel();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                while let Some(batchrows) = resstream.next().await{
                    sender.send(batchrows).unwrap();
                }
                drop(sender)
            });
            for batchrows in receiver {
                let mut rows = batchrows.unwrap().into_iter();
                while let Some(row) = rows.next() {
                    println!("{}", row.into_iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"));
                }
            }*/
            while let Some(batchrows) = resstream.next().await{
                let mut rows = batchrows.unwrap().into_iter();
                while let Some(row) = rows.next() {
                    println!("{}", row.into_iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"));
                }
            }   
        },
        ResultSet::CreateDatabase{name} => {
            println!("Databaes {} is created", name)
        },
        ResultSet::ShowDatabase { dblist } => {
            println!("id|name|create_time");
            for dbmeta in dblist {
                println!("{}|{}|{}", dbmeta.id, dbmeta.name, dbmeta.create_time)
            }
        },
        ResultSet::DropDatabase { name } => {
            println!("Databaes {} is dropped", name)
        },
        ResultSet::UseDatabase { name } => {
            println!("switch to database {}", name)
        },
        ResultSet::CreateTable { name } => {
            println!("table {} created", name);
        },
        ResultSet::DropTable { name } => {
            println!("table {} droped", name)
        },
        ResultSet::ShowTable { tblist }=>{
            println!("name");
            for tb in tblist {
                println!("{}",tb.name)
            }
        },
        _=>{
            println!("invalid result type");
        }
    }
    Ok(())
}

#[async_trait]
pub trait Executor<T: Storage> {
    // try_stream here only allow a single life time bound, if use &self will 
    // introduce another bound beside &mut T
    // here use Box<Self> will sovle it, or make store as Arc<T>
    // that is why later for each operator its new method return a Boxed object

    /* 
    #[try_stream(boxed, ok=ResultBatch, error = Error)]
    async fn execute(self: Box<Self>, store: &mut T);
    */

    async fn execute(self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet>;
}


#[cfg(test)]
mod test {
    use crate::storage::SledStore;
    use crate::storage::Storage;
    use crate::storage::Value;
    use anyhow::Result;
    use crate::storage::Table;
    use crate::storage::Column;
    use crate::storage::Row;

    pub async fn gen_test_db(pathname: String) -> Result<SledStore> {
        let mut ss: SledStore = SledStore::init(format!("./{}", pathname).as_str(), 2).await?;
        ss.create_database(&"newdb".into()).await?;
        ss.usedb(&String::from("newdb")).await?;
        assert_eq!(1, ss.curdbid);
        let test_table = Table {
            name: "testtable".into(),
            columns: vec![
                Column {
                    name: "column1".into(),
                    primary_key: true,
                    ..Column::default()
                },
                Column {
                    name: "column2".into(),
                    ..Column::default()
                },
                Column {
                    name: "column3".into(),
                    ..Column::default()
                },
            ],
        };
        ss.create_table(&test_table).await?;
        let testrow0: Row = vec![
            Value::String("r1".into()),
            Value::String("a".into()),
            Value::String("b".into()),
        ];
        ss.insert_row("testtable", testrow0.clone()).await?;
        let testrow2: Row = vec![
            Value::String("r2".into()),
            Value::String("a".into()),
            Value::String("c".into()),
        ];
        ss.insert_row("testtable", testrow2.clone()).await?;
        let testrow4: Row = vec![
            Value::String("r3".into()),
            Value::String("a".into()),
            Value::String("e".into()),
        ];
        ss.insert_row("testtable", testrow4).await?;
        let testrow5: Row = vec![
            Value::String("r4".into()),
            Value::String("x".into()),
            Value::String("c".into()),
        ];
        ss.insert_row("testtable", testrow5).await?;
        let testrow6: Row = vec![
            Value::String("r5".into()),
            Value::String("x".into()),
            Value::String("c".into()),
        ];
        ss.insert_row("testtable", testrow6).await?;
        let testrow7: Row = vec![
            Value::String("r6".into()),
            Value::String("y".into()),
            Value::String("d".into()),
        ];
        ss.insert_row("testtable", testrow7).await?;

        Ok(ss)
    }
}