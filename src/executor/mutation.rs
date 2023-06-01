use super::ResultSet;
use crate::executor::Executor;
use crate::executor::Row;
use crate::executor::Storage;
use crate::planner::Expression;
use crate::storage::Table;
use crate::storage::Value;
use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
            table,
            columns,
            rows,
        })
    }
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(anyhow::anyhow!(
                "length of columns and values does not match"
            ));
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.into_iter()) {
            table.get_column(c)?;
            if inputs.insert(c.clone(), v).is_some() {
                return Err(anyhow::anyhow!("column {} given multiple times", c));
            }
        }
        let mut row = Row::new();
        for col in table.columns.iter() {
            if let Some(v) = inputs.get(&col.name) {
                row.push(v.clone())
            } else if let Some(def) = &col.default {
                row.push(def.clone())
            } else {
                return Err(anyhow::anyhow!(
                    "column {} do not have default value",
                    col.name
                ));
            }
        }
        Ok(row)
    }
    pub fn pad_row(table: &Table, mut row: Row) -> Result<Row> {
        for remaining_col in table.columns.iter().skip(row.len()) {
            if let Some(v) = &remaining_col.default {
                row.push(v.clone())
            } else {
                return Err(anyhow::anyhow!(
                    "column {} do not have default value",
                    remaining_col.name
                ));
            }
        }
        Ok(row)
    }
}

#[async_trait]
impl<T: Storage + 'static> Executor<T> for Insert {
    async fn execute(self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let mut count = 0;
        let table = store
            .lock()
            .await
            .get_table_def(&self.table.as_str())
            .await?;
        for row in self.rows {
            let mut row = row
                .iter()
                .map(|e| e.evaluate(None))
                .collect::<Result<_>>()?;
            if self.columns.is_empty() {
                row = Self::pad_row(&table, row)?;
            } else {
                row = Self::make_row(&table, &self.columns, row)?;
            }
            store
                .lock()
                .await
                .insert_row(self.table.as_str(), row)
                .await?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}

pub struct Delete<T: Storage> {
    table: String,
    source: Option<Box<dyn Executor<T> + Send + Sync>>,
}

impl<T: Storage> Delete<T> {
    pub fn new(table: String, source: Option<Box<dyn Executor<T> + Send + Sync>>) -> Box<Self> {
        Box::new(Self {
            table: table,
            source: source,
        })
    }
}

/*
Note for the mutation of update and delete, where we
first scan the table and then do upate or delete, we may
have dead lock. cause while doing the query we will need to
call lock() on the mutex.

and for update/delete operation we will also call lock().
and the 2 lock will makes the program waiting.

To break the situation. here when do the query, we save the work
like the (id, newrow) or ids to delete. when the stream is finished
polling, then we do update or delete on the saved result
 */

#[async_trait]
impl<T: Storage> Executor<T> for Delete<T> {
    async fn execute(mut self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let mut count = 0_u64;
        let source = std::mem::take(&mut self.source).unwrap();
        let rs = source.execute(store.clone()).await?;
        match rs {
            ResultSet::Query { columns, mut rows } => {
                let mut id_todelete = Vec::new();
                while let Some(brow) = rows.next().await.transpose()? {
                    for r in brow {
                        let rowid = r.id;
                        id_todelete.push(rowid);
                        count += 1;
                    }
                }
                // The stream is done, now we can call lock. otherwise call lock will be blocked.
                let guard = store.lock().await;
                for rowid in id_todelete {
                    guard.delete_row(&self.table.as_str(), rowid).await?;
                }
            }
            _ => {
                panic!("invalid source type for delete")
            }
        }
        Ok(ResultSet::Delete { count })
    }
}

pub struct Update<T: Storage> {
    table: String,
    source: Option<Box<dyn Executor<T> + Send + Sync>>,
    expressions: Vec<(usize, Expression)>, // column position, new expression
}

impl<T: Storage> Update<T> {
    pub fn new(
        table: String,
        source: Option<Box<dyn Executor<T> + Send + Sync>>,
        expressions: Vec<(usize, Expression)>,
    ) -> Box<Self> {
        Box::new(Self {
            table: table,
            source: source,
            expressions: expressions,
        })
    }
}

#[async_trait]
impl<T: Storage> Executor<T> for Update<T> {
    async fn execute(mut self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        let mut count = 0_u64;
        let source = std::mem::take(&mut self.source).unwrap();
        let rs = source.execute(store.clone()).await?;
        match rs {
            ResultSet::Query {
                columns: columns,
                rows,
            } => {
                let mut update_id_values = Vec::new();
                let mut resstream = rows;
                while let Some(batchrows) = resstream.next().await {
                    let mut rows = batchrows.unwrap().into_iter();
                    while let Some(r) = rows.next() {
                        let rowid = r.id;
                        let mut newvalues = r.values.clone();
                        for (field, expr) in &self.expressions {
                            newvalues[*field] = expr.evaluate(Some(&r.values))?;
                        }
                        update_id_values.push((rowid, newvalues));
                        count += 1;
                    }
                }
                let mugard = store.lock().await;
                for (rid, newvalues) in update_id_values {
                    mugard
                        .update_row(&self.table.as_str(), rid, newvalues)
                        .await?;
                }
            }
            _ => {
                panic!("invalid source type for delete")
            }
        }
        Ok(ResultSet::Update { count })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor::print_resultset;
    use crate::executor::query::Filter;
    use crate::executor::source::Scan;
    use crate::executor::test::gen_test_db;
    use crate::planner::Expression;
    use crate::storage::Column;
    use crate::storage::SledStore;
    use anyhow::Result;

    #[tokio::test]
    async fn test_mutation_insert() -> Result<()> {
        // inser 2 rows
        let ss: SledStore = gen_test_db("tmutation_insert".into()).await?;
        let row1 = vec![
            Expression::Constant(Value::String(String::from("r7"))),
            Expression::Constant(Value::String(String::from("y"))),
            Expression::Constant(Value::String(String::from("e"))),
        ];
        let row2 = vec![
            Expression::Constant(Value::String(String::from("r8"))),
            Expression::Constant(Value::String(String::from("z"))),
            Expression::Constant(Value::String(String::from("f"))),
        ];
        let db = Arc::new(Mutex::new(ss));
        let insertop = Insert::new(
            "testtable".into(),
            vec!["column1".into(), "column2".into(), "column3".into()],
            vec![row1, row2],
        );
        let res = insertop.execute(db.clone()).await?;
        print_resultset(res).await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(db.clone()).await?;
        print_resultset(scanres).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mutation_update() -> Result<()> {
        let ss: SledStore = gen_test_db("tmutation_update".into()).await?;
        let db = Arc::new(Mutex::new(ss));
        //update r3 since col3 = e and new col3 is h
        let scanop = Scan::new("testtable".into(), None);
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(2, None)),
            Box::new(Expression::Constant(Value::String("c".into()))),
        );
        let filterop: Box<Filter<SledStore>> = Filter::new(scanop, filterexp);
        let exps = vec![(2, Expression::Constant(Value::String(String::from("h"))))];
        let updateop = Update::new("testtable".into(), Some(filterop), exps);
        let updateres = updateop.execute(db.clone()).await?;
        print_resultset(updateres).await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(db.clone()).await?;
        print_resultset(scanres).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mutation_delete() -> Result<()> {
        println!("starting");
        let ss: SledStore = gen_test_db("tmutation_delete".into()).await?;
        let db = Arc::new(Mutex::new(ss));
        let scanop = Scan::new("testtable".into(), None);
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(2, None)),
            Box::new(Expression::Constant(Value::String("c".into()))),
        );
        let filterop: Box<Filter<SledStore>> = Filter::new(scanop, filterexp);
        let deleteop = Delete::new("testtable".into(), Some(filterop));
        let deleteres = deleteop.execute(db.clone()).await?;
        print_resultset(deleteres).await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(db.clone()).await?;
        print_resultset(scanres).await?;
        Ok(())
    }
}