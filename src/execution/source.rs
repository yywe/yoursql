use super::Column;
use super::ResultSet;
use crate::execution::Executor;
use crate::execution::Rows;
use crate::storage::Batch;
use crate::execution::Arc;
use crate::{plan::Expression, storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures_async_stream::try_stream;

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

impl Scan {
    #[try_stream(boxed, ok=Rows, error = anyhow::Error)]
    async fn pull_stream<T: Storage + 'static>(self: Box<Self>, store:Arc<T>) {
        //let tbl = store.get_table_def(self.table.as_str()).await?;
        let mut dbscan = store.scan_table(self.table.as_str()).await?;
        while let Some(data) = dbscan.next() {
            let rows = data?;
            yield rows
            /*
            yield ResultBatch::Query {
                columns: tbl
                    .columns
                    .iter()
                    .map(|c| Column {
                        name: Some(c.name.clone()),
                    })
                    .collect(),
                rows: rows,
            }*/
        }
    }
}

#[async_trait]
impl<T: Storage+'static> Executor<T> for Scan {
    async fn execute(self: Box<Self>, store:Arc<T>) -> Result<ResultSet> {
        let tbl = store.get_table_def(self.table.as_str()).await?;
        let row_stream = self.pull_stream(store);

        Ok(ResultSet::Query {
            columns: tbl
                .columns
                .iter()
                .map(|c| Column {
                    name: Some(c.name.clone()),
                })
                .collect(),
            rows: row_stream,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::execution::Row;
    use crate::execution::print_resultset;
    use crate::storage::Column;
    use crate::storage::SledStore;
    use crate::storage::Storage;
    use crate::storage::Table;
    use crate::storage::Value;
    use anyhow::Result;
    use futures::StreamExt;
    use crate::execution::test::gen_test_db;
    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let mut ss: SledStore = gen_test_db().await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(Arc::new(ss)).await?;
        print_resultset(scanres).await?;
        Ok(())
    }
}
