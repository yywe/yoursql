mod sled;
pub use self::sled::SledStore;
use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use sqlparser::ast::ColumnDef;
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Debug)]
pub struct DbMeta {
    pub id: u32,
    pub name: String,
    pub create_time: DateTime<Local>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct IndexValue(HashSet<u64>);

#[derive(Serialize, Deserialize,Clone, Debug, Default, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    #[default]
    String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn get_row_pk(&self, row: &Row) -> Option<Value> {
        match self.columns.iter().position(|c| c.primary_key) {
            Some(index) => {
                return row.get(index).cloned();
            }
            None => return None,
        }
    }
    pub fn get_pk_name(&self) -> Option<String> {
        Some(self.columns.iter().find(|c| c.primary_key)?.name.clone())
    }

    pub fn get_column(&self, name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .context("column does not exist")
    }
    pub fn get_column_by_index(&self, k: usize) -> Result<&Column> {
        Ok(&self.columns[k])
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub default: Option<Value>,
    pub unique: bool,
    pub references: Option<String>,
}

impl Column {
    pub fn extract_column(cdf: &ColumnDef) -> Result<Self> {
        use sqlparser::ast::DataType::*;
        use sqlparser::ast::Value::*;
        let name = cdf.name.value.clone();
        let datatype = match cdf.data_type {
            Binary(_) => DataType::Boolean,
            Integer(_) | Int(_) => DataType::Integer,
            Float(_) | Double => DataType::Float,
            String => DataType::String,
            _ => return Err(anyhow::anyhow!("unsupported datatype")),
        };
        let mut primary_key = false;
        let mut nullable = true;
        let mut default = None;
        let mut unique = false;
        let mut references = None;
        for option in cdf.options.iter() {
            let opt = option.option.clone();
            match opt {
                sqlparser::ast::ColumnOption::NotNull => {
                    nullable = false;
                }
                sqlparser::ast::ColumnOption::Null => {
                    nullable = true;
                }
                sqlparser::ast::ColumnOption::Unique { is_primary }=>{
                    unique = true;
                    if is_primary {
                        primary_key = true;
                    }
                }
                sqlparser::ast::ColumnOption::Default(Expr)=>{
                    match Expr {
                        sqlparser::ast::Expr::Value(val)=>{
                            match val {
                                Number(s, _)=> {default = Some(Value::Float(s.parse::<f64>().unwrap()))},
                                DoubleQuotedString(s)=>{default = Some(Value::String(s))},
                                _=>{ return Err(anyhow::anyhow!("unsupported default value type"));}
                            }
                        }
                        _=>{
                            return Err(anyhow::anyhow!("unsupported default type"));
                        }
                    }
                }
                sqlparser::ast::ColumnOption::ForeignKey{foreign_table,..}=>{
                    let idents = foreign_table.0;
                    if idents.len()!=1{
                        return Err(anyhow::anyhow!("incorrect foreign ident"));
                    }
                    references = Some(idents[0].value.clone())
                }
                _=>{
                    return Err(anyhow::anyhow!("unsupported option"));
                }
            }
        }
        Ok(Column { name: name, datatype: datatype, primary_key: primary_key, nullable: nullable, default: default, unique: unique, references: references })
    }
}
// this only means the the values, no row id
pub type Row = Vec<Value>;

// early design issue. row should has id
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanedRow {
    pub id: u64,
    pub values: Row,
}
pub type Batch = Box<dyn Iterator<Item = Result<Vec<ScanedRow>>> + Send>;

#[async_trait]
pub trait Storage: Sync + Send {
    // methods for database level operation
    async fn create_database(&self, database_name: &String) -> Result<()>;
    async fn drop_database(&self, database_name: &String) -> Result<()>;
    async fn listdbs(&self) -> Result<Vec<DbMeta>>;
    async fn usedb(&mut self, database_name: &String) -> Result<()>;

    // methods for table catalog operation
    async fn create_table(&self, table: &Table) -> Result<()>;
    async fn listtbls(&self) -> Result<Vec<Table>>;
    async fn drop_table(&self, name: &str) -> Result<()>;
    async fn get_table_def(&self, name: &str) -> Result<Table>;

    // methods for table data operation
    async fn insert_row(&self, table: &str, row: Row) -> Result<u64>;
    async fn read_row(&self, table: &str, id: u64) -> Result<Option<Row>>;
    async fn update_row(&self, table: &str, id: u64, row: Row) -> Result<()>;
    async fn delete_row(&self, table: &str, id: u64) -> Result<()>;
    async fn scan_table(&self, table: &str) -> Result<Batch>;
}
