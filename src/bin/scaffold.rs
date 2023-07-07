use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use yoursql::common::record_batch::RecordBatch;
use yoursql::common::table_reference::TableReference;
use yoursql::common::types::DataType;
use yoursql::common::types::DataValue;
use yoursql::common::types::Field;
use yoursql::common::types::TableDef;
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
    let memtable_def = TableDef::new(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Utf8, false),
        ],
        HashMap::new(),
    );
    let row_batch1 = vec![
        vec![
            DataValue::Int64(1),
            DataValue::Boolean(false),
            DataValue::Utf8("hello".into()),
        ],
        vec![
            DataValue::Int64(2),
            DataValue::Boolean(false),
            DataValue::Utf8("world".into()),
        ],
    ];
    let row_batch2 = vec![
        vec![
            DataValue::Int64(3),
            DataValue::Boolean(true),
            DataValue::Utf8("your".into()),
        ],
        vec![
            DataValue::Int64(4),
            DataValue::Boolean(true),
            DataValue::Utf8("sql".into()),
        ],
    ];
    let batch1 = RecordBatch {
        header: memtable_def.fields.clone(),
        rows: row_batch1.clone(),
    };
    let batch2 = RecordBatch {
        header: memtable_def.fields.clone(),
        rows: row_batch2.clone(),
    };
    let memtable = MemTable::try_new(Arc::new(memtable_def), vec![batch1, batch2])?;

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
