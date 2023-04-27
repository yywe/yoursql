use yoursql::storage::{SledStore, Catalog};
use tracing::{debug, info, span, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .init();
    info!("starting the database...");

    let ss = SledStore::init("/home/yy/Learning/Database/yoursql/tempdb").await?;


    let ans = ss.listdbs()?;
    println!("list of dbs:{:#?}",ans);

    ss.create_database(&"testdb2".into()).await?;

    let ans = ss.listdbs()?;
    println!("list of dbs:{:#?}",ans);

    ss.drop_database(&"testdb2".into()).await?;

    Ok(())
}