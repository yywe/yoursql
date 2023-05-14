mod source;
mod query;
use crate::storage::Row;
use crate::storage::Storage;
use anyhow::Error;
use futures_async_stream::try_stream;


const MAX_BATCH_SIZE: usize = 2;

// column def in result is very simple, even column name is optional
#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
}
pub type Columns = Vec<Column>;

#[derive(Debug)]
pub enum ResultBatch {
    Query {
        columns: Columns,
        rows: Vec<Row>,
    }
}


pub trait Executor<T: Storage> {
    // try_stream here only allow a single life time bound, if use &self will 
    // introduce another bound beside &mut T
    // here use Box<Self> will sovle it, or make store as Arc<T>
    // that is why later for each operator its new method return a Boxed object
    #[try_stream(boxed, ok=ResultBatch, error = Error)]
    async fn execute(self: Box<Self>, store: &mut T);
}


#[cfg(test)]
mod test {
    use crate::storage::SledStore;
    use crate::storage::Storage;
    use crate::storage::Value;
    use anyhow::Result;
    use crate::storage::Table;
    use crate::storage::Column;
    use crate::storage::Row;

    pub async fn gen_test_db() -> Result<SledStore> {
        let mut ss: SledStore = SledStore::init("./testdb", 2).await?;
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

        Ok(ss)
    }
}