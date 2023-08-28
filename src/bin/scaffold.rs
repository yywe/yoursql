use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use yoursql::common::record_batch::RecordBatch;
use yoursql::common::table_reference::TableReference;
use yoursql::common::table_reference::OwnedTableReference;
use yoursql::common::types::DataType;
use yoursql::common::types::DataValue;
use yoursql::common::schema::Field;
use yoursql::common::schema::Schema;
use yoursql::physical_plan::print_batch_stream;
use yoursql::physical_plan::RecordBatchStream;
use yoursql::session::SessionContext;
use yoursql::storage::memory::MemTable;
use yoursql::storage::Table;

/// cargo run --package yoursql --bin scaffold
#[tokio::main]
async fn main() -> Result<()> {
    // init a session
    let session = SessionContext::default();
    // prepare a memory table
    let qualifier = OwnedTableReference::Full {
        database: "testdb".to_string().into(),
        table: "testtable".to_string().into(),
    };
    let memtable_def = Schema::new(
        vec![
            Field::new("a", DataType::Int64, false,  Some(qualifier.clone())),
            Field::new("b", DataType::Boolean, false,  Some(qualifier.clone())),
            Field::new("c", DataType::Utf8, false,  Some(qualifier.clone())),
           
        ],
        HashMap::new(),
    );
    let memtable_ref = Arc::new(memtable_def);
    let row_batch1 = vec![
        vec![
            DataValue::Int64(Some(1)),
            DataValue::Boolean(Some(false)),
            DataValue::Utf8(Some("hello".into())),
        ],
        vec![
            DataValue::Int64(Some(2)),
            DataValue::Boolean(Some(false)),
            DataValue::Utf8(Some("world".into())),
        ],
    ];
    let row_batch2 = vec![
        vec![
            DataValue::Int64(Some(3)),
            DataValue::Boolean(Some(true)),
            DataValue::Utf8(Some("your".into())),
        ],
        vec![
            DataValue::Int64(Some(4)),
            DataValue::Boolean(Some(true)),
            DataValue::Utf8(Some("sql".into())),
        ],
    ];
    let batch1 = RecordBatch {
        schema: memtable_ref.clone(),
        rows: row_batch1.clone(),
    };
    let batch2 = RecordBatch {
        schema: memtable_ref.clone(),
        rows: row_batch2.clone(),
    };
    let memtable = MemTable::try_new(memtable_ref, vec![batch1, batch2])?;

    // register the table to catalog
    let table_referene = TableReference::Bare {
        table: "testa".into(),
    };
    let table_ref = Arc::new(memtable);
    session.register_table(table_referene.clone(), table_ref.clone())?;

    // scan the table and print the result
    let exec = table_ref.scan(&session.state(), None, &[]).await?;
    let it: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = exec.execute()?;
    print_batch_stream(it).await?;
    Ok(())
}
