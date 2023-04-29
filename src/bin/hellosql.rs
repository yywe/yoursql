use yoursql::storage::{SledStore, Catalog};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing::{debug, info, span, Level};
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("sled=off".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(env_filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("starting the database...");
    let ss = SledStore::init("./tempdb").await?;
    let ans = ss.listdbs().await?;
    println!("list of dbs:{:#?}",ans);
    ss.create_database(&"testdb2".into()).await?;
    let ans = ss.listdbs().await?;
    println!("list of dbs:{:#?}",ans);
    ss.drop_database(&"testdb2".into()).await?;
    Ok(())
}