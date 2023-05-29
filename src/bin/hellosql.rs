use anyhow::Result;
use tracing::{debug, info, span, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use yoursql::executor::Columns;
use yoursql::storage::Column;
use yoursql::storage::Row;
use yoursql::storage::Table;
use yoursql::storage::Value;
use yoursql::storage::{SledStore, Storage};
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
    let mut ss = SledStore::init("./tempdb", 2).await?;

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

    Ok(())
}
