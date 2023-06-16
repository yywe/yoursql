use std::io;
use tokio::io::AsyncWrite;

use opensrv_mysql::*;
use tokio::net::TcpListener;

use yoursql::executor::ResultSet;
use yoursql::storage::SledStore;
use yoursql::parser::parse;
use tokio::sync::Mutex;
use std::sync::Arc;
use anyhow::Result;

use std::borrow::Borrow;

use opensrv_mysql::Column;
use yoursql::planner::Plan;

use std::pin::Pin;
use futures::Stream;
use yoursql::storage::ScanedRow;
use futures::StreamExt;
//use anyhow::Error;
use yoursql::storage::Value;


#[derive(Clone)]
struct Backend{
    dbstore: Arc<Mutex<SledStore>>,
}
impl Backend {
    async fn new(dbpath: String) -> Result<Self> {
        let mut ss: SledStore = SledStore::init(dbpath.as_str(), 2).await?;
        Ok(Self {
            dbstore: Arc::new(Mutex::new(ss)),
        })
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("execute sql {:?}", sql);
        let astvec = parse(sql);
        if astvec.is_err() {
            let err = astvec.err().unwrap();
            results.error(ErrorKind::ER_PARSE_ERROR, AsRef::<[u8]>::as_ref(&err.to_string())).await?;
   
            return Ok(());
        }
        let ast = astvec.unwrap()[0].clone();
        let plan = Plan::build(ast, self.dbstore.clone()).await;
        if plan.is_err() {
            let err = plan.err().unwrap();
            results.error(ErrorKind::ER_PARSE_ERROR,  AsRef::<[u8]>::as_ref(&err.to_string())).await?;
            return Ok(());
        }
        let ans = plan.unwrap().execute( self.dbstore.clone()).await;
        if ans.is_err() {
            let err = ans.err().unwrap();
            results.error(ErrorKind::ER_PARSE_ERROR,  AsRef::<[u8]>::as_ref(&err.to_string())).await?;
            return Ok(());
        }
        let result = ans.unwrap();
        //todo: finish with affected rows.
        match result {
            ResultSet::Query{columns, rows}=>{
                println!("{}", columns.iter().map(|c|c.name.as_deref().unwrap_or("?")).collect::<Vec<_>>().join("|"));
                let sendcols: Vec<Column> = columns.iter().map(|c|Column{table:"unkowntbl".into(), column: c.name.clone().unwrap(), coltype:mysql_common::constants::ColumnType::MYSQL_TYPE_STRING ,colflags:mysql_common::constants::ColumnFlags::NOT_NULL_FLAG}).collect();
                //write row back to client
                // convert all the types to string type for simplicity.
                let mut row_writer = results.start(&sendcols).await?;
                let mut resstream = rows;
                while let Some(batchrows) = resstream.next().await{
                    let mut rows= batchrows.unwrap().into_iter();
                    while let Some(row) = rows.next() {
                        println!("{}", row.values.iter().map(|v|format!("{}",v)).collect::<Vec<_>>().join("|"));
                        let mut ansrow = Vec::new();

                        for val in row.values {
                            match val {
                                Value::Null => {ansrow.push(String::from("NULL"))},
                                Value::Boolean(b)=>{ansrow.push(format!("{}",b))},
                                Value::Integer(i)=>{ansrow.push(format!("{}",i))},
                                Value::Float(f)=>{ansrow.push(format!("{}",f))},
                                Value::String(s)=>{ansrow.push(s)},
                            }
                        }
                        row_writer.write_row(ansrow).await?;
                    }
                }   
                return row_writer.finish().await;
            },

            ResultSet::CreateDatabase{name} => {
                println!("Databaes {} is created", name);
                return results.completed(OkResponse{info:format!("Databaes {} is created", name),..Default::default()}).await
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
                return results.completed(OkResponse{info:format!("table {} is created", name),..Default::default()}).await
            },
            ResultSet::DropTable { name } => {
                println!("table {} droped", name);
                return results.completed(OkResponse{info:format!("table {} is dropped", name),..Default::default()}).await
            },
            ResultSet::ShowTable { tblist }=>{
                return results.completed(OkResponse{info:format!("{}", tblist.iter().map(|t|t.name.clone()).collect::<Vec<_>>().join("\n")),..Default::default()}).await
            },
            ResultSet::Insert { count } => {
                println!("inserted {} record", count);
                return results.completed(OkResponse{affected_rows:count,..Default::default()}).await
            },
            ResultSet::Update { count } => {
                println!("updated {} record", count);
                return results.completed(OkResponse{affected_rows:count,..Default::default()}).await
            },
            ResultSet::Delete { count } => {
                println!("deleted {} record", count);
                return results.completed(OkResponse{affected_rows:count,..Default::default()}).await
            },
            _=>{
                println!("invalid result type");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3306").await?;
    
    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let mybackend = Backend::new("./mdb".into()).await?;
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(mybackend, r, w).await });
    }
}
