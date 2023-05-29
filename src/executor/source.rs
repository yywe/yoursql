use super::Column;
use super::ResultSet;
use crate::executor::Executor;
//use crate::execution::Rows;
use crate::executor::ScanedRows;
use crate::executor::Arc;
use crate::storage::Batch;
use crate::{planner::Expression, storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use futures_async_stream::try_stream;
use tokio::sync::Mutex;

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
    #[try_stream(boxed, ok=ScanedRows, error = anyhow::Error)]
    async fn pull_stream<T: Storage + 'static>(self: Box<Self>, store: Arc<Mutex<T>>) {
        let mutexgard = store.lock().await;
        let mut dbscan =mutexgard.scan_table(self.table.as_str()).await?;
        while let Some(data) = dbscan.next() {
            let rows = data?;
            yield rows
        }
    }
}

#[async_trait]
impl<T: Storage+'static> Executor<T> for Scan {
    async fn execute(self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let tbl = store.lock().await.get_table_def(self.table.as_str()).await?;
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
    use crate::executor::print_resultset;
    use crate::storage::SledStore;
    use anyhow::Result;
    use crate::executor::test::gen_test_db;
    use tokio::sync::Mutex;
    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let ss: SledStore = gen_test_db("tscan".into()).await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(Arc::new(Mutex::new(ss))).await?;
        print_resultset(scanres).await?;
        Ok(())
    }
}
