use crate::execution::Storage;
use async_trait::async_trait;
use crate::execution::Executor;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::Mutex;
use super::ResultSet;
use crate::storage::Table;
use crate::plan::Expression;
use crate::storage::Value;
use crate::execution::Row;
use futures::StreamExt;
use std::collections::HashMap;

pub struct Insert {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table: String, columns: Vec<String>, rows: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self{table, columns, rows})
    }
    pub fn make_row(table: &Table, columns: &[String], values: Vec<Value>) -> Result<Row> {
        if columns.len() != values.len() {
            return Err(anyhow::anyhow!("length of columns and values does not match"));
        }
        let mut inputs = HashMap::new();
        for (c, v) in columns.iter().zip(values.into_iter()){
            table.get_column(c)?; 
            if inputs.insert(c.clone(), v).is_some() {
                return Err(anyhow::anyhow!("column {} given multiple times", c));
            }       
        }
        let mut row = Row::new();
        for col in table.columns.iter(){
            if let Some(v) = inputs.get(&col.name){
                row.push(v.clone())
            }else if let Some(def) = &col.default{
                row.push(def.clone())
            }else{
                return Err(anyhow::anyhow!("column {} do not have default value", col.name));
            }
        }        
        Ok(row)
    }
    pub fn pad_row(table: &Table, mut row: Row) -> Result<Row> {
        for remaining_col in table.columns.iter().skip(row.len()){
            if let Some(v) = &remaining_col.default{
                row.push(v.clone())
            }else{
                return Err(anyhow::anyhow!("column {} do not have default value", remaining_col.name));
            }
        }
        Ok(row)
    }
}

#[async_trait]
impl<T: Storage + 'static> Executor<T> for Insert {
    async fn execute(self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet>{
        let mut count = 0;
        let table = store.lock().await.get_table_def(&self.table.as_str()).await?;
        for row in self.rows {
            let mut row = row.iter().map(|e|e.evaluate(None)).collect::<Result<_>>()?;
            if self.columns.is_empty(){
                row = Self::pad_row(&table, row)?;
            }else{
                row = Self::make_row(&table, &self.columns, row)?;
            }
            store.lock().await.insert_row(self.table.as_str(), row).await?;
            count+=1;
        }
        Ok(ResultSet::Insert{count})
    }
}


pub struct Delete<T: Storage> {
    table: String,
    source: Option<Box<dyn Executor<T> + Send+Sync>>,    
}

impl<T: Storage> Delete<T> {
    pub fn new(table: String, source: Option<Box<dyn Executor<T> + Send + Sync>>) -> Box<Self> {
        Box::new(Self{table: table, source:source})
    }
}

#[async_trait]
impl<T: Storage> Executor<T> for Delete<T>{
    async fn execute(mut self: Box<Self>, store: Arc<Mutex<T>>) -> Result<ResultSet>{
        let mut count = 0_u64;
        let source = std::mem::take(&mut self.source).unwrap();
        let rs = source.execute(store.clone()).await?;
        match rs {
            ResultSet::Query { columns, mut rows }=>{
                while let Some(brow) = rows.next().await.transpose()?{
                    for r in brow {
                        let rowid = r.id;
                        store.lock().await.delete_row(&self.table.as_str(), rowid).await?;
                        count +=1;
                    }
                }
            },
            _=>{
                panic!("invalid source type for delete")
            }
        }
        Ok(ResultSet::Delete {count})
    }
}

#[cfg(test)]
mod test{
    use super::*;
    use crate::execution::print_resultset;
    use crate::storage::SledStore;
    use anyhow::Result;
    use crate::storage::Column;
    use crate::execution::test::gen_test_db;
    use crate::plan::Expression;
    use crate::execution::source::Scan;
    use crate::execution::query::Filter;
    #[tokio::test]
    async fn test_mutation() -> Result<()> {
        // inser 2 rows
        let ss: SledStore = gen_test_db("tmutation".into()).await?;
        let row1 = vec![
            Expression::Constant(Value::String(String::from("r7"))),
            Expression::Constant(Value::String(String::from("y"))),
            Expression::Constant(Value::String(String::from("e")))
        ];
        let row2 = vec![
            Expression::Constant(Value::String(String::from("r8"))),
            Expression::Constant(Value::String(String::from("z"))),
            Expression::Constant(Value::String(String::from("f")))
        ];
        let db = Arc::new(Mutex::new(ss));
        let insertop = Insert::new("testtable".into(), vec!["column1".into(), "column2".into(), "column3".into()],vec![row1, row2]);
        let res = insertop.execute(db.clone()).await?;
        print_resultset(res).await?;
        let scanop = Scan::new("testtable".into(), None);
        let scanres = scanop.execute(db.clone()).await?;
        print_resultset(scanres).await?;

        // now delete the row with columnc = 'f', need to construct a source filter.
        let scanop = Scan::new("testtable".into(), None);
        //expect last column/field = "a"
        let filterexp = Expression::Equal(
            Box::new(Expression::Field(2, None)),
            Box::new(Expression::Constant(Value::String("f".into()))),
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
