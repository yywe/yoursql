use super::Column;
use super::ResultBatch;
use crate::execution::Executor;
use crate::{plan::Expression, storage::Storage};
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct Scan {
    table: String,
    // not used yet
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table, filter })
    }
}

impl<T: Storage> Executor<T> for Scan {
    #[try_stream(boxed, ok=ResultBatch, error = anyhow::Error)]
    async fn execute(self: Box<Self>, store: &mut T) {
        let tbl = store.get_table_def(self.table.as_str()).await?;
        let mut dbscan = store.scan_table(self.table.as_str()).await?;
        while let Some(data) = dbscan.next() {
            let rows = data?;
            yield ResultBatch::Query {
                columns: tbl
                    .columns
                    .iter()
                    .map(|c| Column {
                        name: Some(c.name.clone()),
                    })
                    .collect(),
                rows: rows,
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
    use futures::StreamExt;

    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let test_folder = "./tdbscan";
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
            Value::String("c1".into()),
            Value::String("c2".into()),
            Value::String("c3".into()),
        ];
        ss.insert_row("testtable", testrow0.clone()).await?;
        let testrow2: Row = vec![
            Value::String("c11".into()),
            Value::String("c2".into()),
            Value::String("c3".into()),
        ];
        ss.insert_row("testtable", testrow2.clone()).await?;
        let testrow4: Row = vec![
            Value::String("c41".into()),
            Value::String("c42".into()),
            Value::String("c43".into()),
        ];
        ss.insert_row("testtable", testrow4).await?;
        let testrow5: Row = vec![
            Value::String("c51".into()),
            Value::String("c52".into()),
            Value::String("c53".into()),
        ];
        ss.insert_row("testtable", testrow5).await?;
        let testrow6: Row = vec![
            Value::String("c61".into()),
            Value::String("c62".into()),
            Value::String("c63".into()),
        ];
        ss.insert_row("testtable", testrow6).await?;
        let testrow7: Row = vec![
            Value::String("c71".into()),
            Value::String("c72".into()),
            Value::String("c73".into()),
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
