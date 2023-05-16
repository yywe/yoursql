use crate::execution::Columns;
use crate::execution::Executor;
use crate::execution::Rows;
use crate::storage::Value;
use crate::{plan::Expression, storage::Storage};
use anyhow::{Context, Error};
use futures::StreamExt;
use futures_async_stream::try_stream;

use super::MAX_BATCH_SIZE;

pub struct Filter<T: Storage> {
    predicate: Expression,
    source: Box<dyn Executor<T> + Send>, // source needs Send when call its execute method
}

impl<T: Storage> Filter<T> {
    pub fn new(source: Box<dyn Executor<T> + Send>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

/* 
impl<T: Storage> Executor<T> for Filter<T> {
    #[try_stream(boxed, ok=Rows, error = Error)]
    async fn execute(self: Box<Self>, store: &mut T) {
        let mut ds = self.source.execute(store);
        let mut local_batch: Vec<Row> = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut header: Option<Columns> = None;
        while let Some(rb) = ds.next().await.transpose()? {
            match rb {
                ResultBatch::Query { columns, rows } => {
                    // save a copy of the header for the remaining rows if any
                    if header.is_none() {
                        header = Some(columns.clone());
                    }
                    // note the error case when evaluate is ignored
                    let filteredrow = rows.into_iter().filter_map(|row| {
                        match self.predicate.evaluate(Some(&row)).ok() {
                            Some(Value::Boolean(true)) => Some(row),
                            _ => None,
                        }
                    });
                    local_batch.extend(filteredrow);
                    if local_batch.len() >= MAX_BATCH_SIZE {
                        yield ResultBatch::Query {
                            columns: columns,
                            rows: std::mem::take(&mut local_batch),
                        }
                    }
                }
                _ => {
                    panic!("invalid data source for filter operator")
                }
            }
        }
        if local_batch.len() > 0 {
            yield ResultBatch::Query {
                columns: header.context("invalid header")?,
                rows: std::mem::take(&mut local_batch),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::execution::source::Scan;
    use crate::plan::Expression;
    use crate::storage::SledStore;
    use crate::storage::Value;
    use anyhow::Result;
    use crate::execution::test::gen_test_db;

    #[tokio::test]
    async fn test_filter() -> Result<()> {
        let mut ss: SledStore = gen_test_db().await?;
        let scanop = Scan::new("testtable".into(), None);
        //expect 1st column/field = "a"
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(1, None)),
            Box::new(Expression::Constant(Value::String("a".into()))),
        );
        let filterop: Box<Filter<SledStore>> = Filter::new(scanop, filterexp);
        let mut res = filterop.execute(&mut ss);
        while let Some(v) = res.next().await.transpose()? {
            println!("the output batch:{:#?}", v);
            
        }
        Ok(())
    }
}
*/