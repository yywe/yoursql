use crate::execution::Executor;
use crate::execution::ScanedRows;
use crate::execution::ScanedRow;
use crate::storage::Value;
use anyhow::Result;
use super::ResultSet;
use crate::execution::RowStream;
use crate::{plan::Expression, storage::Storage};
use anyhow::Error;
use futures::StreamExt;
use async_trait::async_trait;
use std::sync::Arc;
use crate::execution::Row;
use futures_async_stream::try_stream;
use futures::Stream;
use tokio::sync::Mutex;
use crate::execution::Column;

use super::MAX_BATCH_SIZE;

pub struct Filter<T: Storage> {
    predicate: Expression,
    // an ugly workaround, I do not know what are the better solution
    // the problem is later i first call the execute on source. if I 
    // do not use option, then I hit the partial move issue and cannot 
    // call self.filter_stream cause self.source will move ownership
    // here the workaround is to make this an option, later i will 
    // take it. feels like this is jus a temporal storage for the trait obj
    source: Option<Box<dyn Executor<T> + Send+Sync>>, // source needs Send when call its execute method

}

impl<T: Storage+'static> Filter<T> {
    pub fn new(source: Box<dyn Executor<T> + Send+Sync>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source: Some(source), predicate:predicate })
    }
    #[try_stream(boxed, ok=ScanedRows, error = Error)]
    async fn filter_stream(self: Box<Self>, mut input: RowStream) {
        let mut local_batch: Vec<ScanedRow> = Vec::with_capacity(MAX_BATCH_SIZE);
        while let Some(rows) = input.next().await.transpose()? {
            // note the error case when evaluate is ignored
            let filteredrow = rows.into_iter().filter_map(|row| {
                match self.predicate.evaluate(Some(&row.values)).ok() {
                    Some(Value::Boolean(true)) => Some(row),
                    _ => None,
                }
            });
            local_batch.extend(filteredrow);
            if local_batch.len() >= MAX_BATCH_SIZE {
                yield std::mem::take(&mut local_batch)
            }
        }
        if local_batch.len() > 0 {
            yield std::mem::take(&mut local_batch)
        }
    }
}


#[async_trait]
impl<T: Storage+'static> Executor<T> for Filter<T> {
    async fn execute(mut self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let source = std::mem::take(&mut self.source).unwrap();
        let rs = source.execute(store).await?;
        match rs {
            ResultSet::Query { columns, rows } => {
                let fs = self.filter_stream(rows);
                return Ok(ResultSet::Query {
                    columns: columns,
                    rows:fs,
                })
            },
            _ => {
                panic!("invalid result set found!");
            },
        }
    }
}


pub struct Projection<T: Storage> {
    source: Option<Box<dyn Executor<T> + Send+Sync>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl <T: Storage+'static> Projection<T>{
    pub fn new(source: Box<dyn Executor<T> + Send+Sync>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self> {
        Box::new(Self{
            source: Some(source), 
            expressions: expressions,
        })
    }
    #[try_stream(boxed, ok=ScanedRows, error = Error)]
    async fn project_stream(self: Box<Self>, mut input: RowStream) {
        let mut local_batch: Vec<ScanedRow> = Vec::with_capacity(MAX_BATCH_SIZE);
        while let Some(rows) = input.next().await.transpose()? {
            let projectrow = rows.iter().map(|row|{
                ScanedRow{
                    id: row.id,
                    values: self.expressions.iter().map(|(exp, _)|{exp.evaluate(Some(&row.values)).unwrap()}).collect(),
                }
            }).collect::<Vec<ScanedRow>>();
            local_batch.extend(projectrow);
            if local_batch.len() >= MAX_BATCH_SIZE {
                yield std::mem::take(&mut local_batch)
            }
        }
        if local_batch.len() > 0 {
            yield std::mem::take(&mut local_batch)
        }
    }
}

#[async_trait]
impl<T: Storage+'static> Executor<T> for Projection<T> {
    async fn execute(mut self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let source = std::mem::take(&mut self.source).unwrap();
        let rs = source.execute(store).await?;  
        match rs {
            ResultSet::Query { columns, rows } => {
                let projcols = self.expressions.iter().map(|(exp, label)|{
                    if let Some(col) = label {
                        Column{name: Some(col.clone())}
                    }else if let Expression::Field(i,_ ) = exp { // note Field itself's label is ignored
                        columns.get(*i).cloned().unwrap_or(Column{name: None})
                    }else{
                        Column{name: None}
                    }
                }).collect();
                let ps = self.project_stream(rows);
                return Ok(ResultSet::Query {
                    columns: projcols,
                    rows:ps,
                })
            },
            _ => {
                panic!("invalid result set found!");
            },
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
    use crate::execution::print_resultset;
    use tokio::sync::Mutex;
    #[tokio::test]
    async fn test_filter() -> Result<()> {
        let mut ss: SledStore = gen_test_db("tfilter".into()).await?;
        let scanop = Scan::new("testtable".into(), None);
        //expect 1st column/field = "a"
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(1, None)),
            Box::new(Expression::Constant(Value::String("a".into()))),
        );
        let filterop: Box<Filter<SledStore>> = Filter::new(scanop, filterexp);
        let res = filterop.execute(Arc::new(Mutex::new(ss))).await?;
        println!("result of filter:");
        print_resultset(res).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_project() -> Result<()> {
        let mut ss: SledStore = gen_test_db("tproject".into()).await?;
        let scanop = Scan::new("testtable".into(), None);
        //expect 1st column/field = "a"
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(1, None)),
            Box::new(Expression::Constant(Value::String("a".into()))),
        );
        let filterop: Box<Filter<SledStore>> = Filter::new(scanop, filterexp);
        // Vec<(Expression, Option<String>)>
        let projectop: Box<Projection<SledStore>> = Projection::new(filterop, vec![(Expression::Field(1, None), None),(Expression::Field(2, None),None)]);
        let res = projectop.execute(Arc::new(Mutex::new(ss))).await?;
        println!("result of projection:");
        print_resultset(res).await?;
        Ok(())
    }
}
