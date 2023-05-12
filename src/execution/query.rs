use crate::{plan::Expression, storage::Storage};
use crate::execution::Executor;
use futures::StreamExt;
use futures_async_stream::try_stream;
use crate::execution::ResultBatch;
use anyhow::{Error, Context};
use crate::execution::Row;
use crate::execution::Columns;

use super::MAX_BATCH_SIZE;

pub struct Filter<T: Storage> {
    predicate: Expression,
    source: Box<dyn Executor<T>+Send>, // source needs Send when call its execute method
}

impl<T: Storage> Filter<T> {
    pub fn new(source: Box<dyn Executor<T>+Send>, predicate: Expression) ->Box<Self> {
        Box::new(Self{source, predicate})
    }
}

impl<T: Storage> Executor<T> for Filter<T> {
    #[try_stream(boxed, ok=ResultBatch, error = Error)]
    async fn execute(self: Box<Self>, store: &mut T){
        let mut ds = self.source.execute(store);
        let mut local_batch: Vec<Row> = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut header: Option<Columns> = None;
        while let Some(rb) = ds.next().await.transpose()?{
            match rb {
                ResultBatch::Query { columns, rows }=>{
                    // save a copy of the header for the remaining rows if any
                    if header.is_none() {
                        header =Some(columns.clone());
                    }
                    
                    local_batch.extend(rows);
                    if local_batch.len() >=MAX_BATCH_SIZE {
                        yield ResultBatch::Query {
                            columns: columns,
                            rows: std::mem::take(&mut local_batch),
                        }
                    }                  
                },
                _=>{
                    panic!("invalid data source for filter operator")
                }
            }
        }
        if local_batch.len()>0 {
            yield ResultBatch::Query {
                columns:  header.context("invalid header")?,
                rows: std::mem::take(&mut local_batch),
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::execution::Row;
    use crate::storage::Column;
    use crate::storage::SledStore;
    use crate::storage::Storage;
    use crate::storage::Table;
    use crate::storage::Value;
    use anyhow::Result;
    use crate::execution::source::Scan;
    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let test_folder = "./tdbfilter";
        let mut ss = SledStore::init(test_folder, 2).await?;
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

        let scanop = Scan::new("testtable".into(), None);
        let mut scanss = scanop.execute(&mut ss);
        while let Some(v) = scanss.next().await.transpose()? {
            //println!("the output batch:{:#?}", v);
            match v {
                ResultBatch::Query { columns: _, rows } => {
                    assert_eq!(rows.len(), 2);
                }
                _ => panic!("invalid type"),
            }
        }

        Ok(())
    }
}

