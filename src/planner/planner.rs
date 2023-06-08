use crate::storage::{Storage, Table, Column};
use anyhow::{Result, anyhow};
use crate::planner::Plan;
use sqlparser::ast::{self, SetExpr};
use crate::planner::Node;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::planner::Expression;
pub struct Planner<T: Storage> {
    store: Arc<Mutex<T>>
}

impl<S: Storage> Planner<S> {
    pub fn new(store: Arc<Mutex<S>>) ->Self {
        Self {store}
    }
    pub async fn build(&self, statement: ast::Statement) -> Result<Plan>{
        Ok(Plan(self.build_statement(statement).await?))
    }
    async fn build_statement(&self, statement: ast::Statement)->Result<Node> {
        match statement {
            ast::Statement::CreateTable {name, columns, constraints,..} => {
                let idents = name.0;
                if idents.len()!=1 {
                    return Err(anyhow!("table name must be 1 part for now"))
                }
                let tblname = idents[0].value.clone();
                let mut cols = Vec::new();
                for col in columns {
                    cols.push(Column::extract_column(&col)?)
                }
                Ok(Node::CreateTable { table: Table{name: tblname, columns: cols }})
            },
            ast::Statement::Insert {table_name, columns,source,.. }=>{
                let tblname = table_name.0[0].value.clone();
                let cols = columns.into_iter().map(|col|col.value).collect::<Vec<String>>();
                let mut values: Vec<Vec<Expression>> = Vec::new();
                match *source.body {
                    SetExpr::Values(vals)=>{
                        let rows = vals.rows;
                        for row in rows {
                            let mut rowvals = Vec::new();
                            for (idx, v) in row.iter().enumerate() {
                                let tabledef = self.store.lock().await.get_table_def(tblname.as_str()).await?;
                                let coldef = if cols.len()>0 { // get column def from name
                                    tabledef.get_column(cols[idx].as_str())
                                }else{
                                    tabledef.get_column_by_index(idx) // get column def from index
                                }?;
                                let datatype = coldef.datatype.clone();
                                match v {
                                    sqlparser::ast::Expr::Value(val)=>{
                                        match val {
                                            sqlparser::ast::Value::Number(s, _)=> {
                                                match datatype {
                                                    crate::storage::DataType::Float => {
                                                        rowvals.push(Expression::Constant(crate::planner::Value::Float(s.parse::<f64>().unwrap())))
                                                    }
                                                    crate::storage::DataType::Integer => {
                                                        rowvals.push(Expression::Constant(crate::planner::Value::Integer(s.parse::<i64>().unwrap())))
                                                    }
                                                    _=>{
                                                        return Err(anyhow::anyhow!("invalid datatype"));
                                                    }
                                                }
                                            },
                                            sqlparser::ast::Value::DoubleQuotedString(s)=>{
                                                rowvals.push(Expression::Constant(crate::planner::Value::String(s.clone())))
                                            },
                                            sqlparser::ast::Value::Boolean(b)=>{
                                                rowvals.push(Expression::Constant(crate::planner::Value::Boolean(b.clone())))
                                            },
                                            _=>{ return Err(anyhow::anyhow!("unsupported default value type"));}
                                        }
                                    }
                                    _=>{
                                        return Err(anyhow::anyhow!("unsupported value type {:?}",v));
                                    }
                                }
                            }
                            values.push(rowvals);
                        }
                    },
                    _=>{
                        return Err(anyhow!("unsupported insert body"))
                    }
                };
                Ok(Node::Insert { table:tblname, columns:cols, rows:values})
            },
            _=> return Err(anyhow!("unsupported statment yet:{:?}", statement))
        }
    }
}