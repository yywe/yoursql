use crate::storage::{Storage, Table, Column};
use anyhow::{Result, anyhow};
use crate::planner::Plan;
pub use sqlparser::ast::{self, SetExpr, TableFactor};
use crate::planner::Node;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::planner::Expression;
use sqlparser::ast::BinaryOperator;
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
            ast::Statement::Query(query)=>{
                // define the root node to return, the bottom will be scan
                let mut root;
                let tblname;

                match *query.body {
                    SetExpr::Select(select)=>{
                        let from_clause = select.from;
                        if from_clause.len()!=1 {
                            return Err(anyhow!("join not supported yet, only support single table"));
                        }
                        let table = from_clause[0].relation.clone();
                        match table {
                            // for simplicity alias is not used yet
                            TableFactor::Table{name,..}=>{
                                tblname = name.0[0].value.clone();
                                root = Node::Scan { table: tblname.clone(), alias: None, filter: None };
                            },
                            _=>{
                                return Err(anyhow!("unsupported table factor type"));
                            }
                        }
                        // build filter, the current filter only support equal condition
                        match select.selection {
                            Some(expr)=>{
                                let tabledef = self.store.lock().await.get_table_def(tblname.as_str()).await?;
                                let predicate = self.build_expression(expr, &tabledef)?;
                                root = Node::Filter { source:Box::new(root), predicate: predicate};
                            },
                            _=>{},
                        }
                        // build projection
                        let projection = select.projection;
                        if projection.len()>0 {
                            let tabledef = self.store.lock().await.get_table_def(tblname.as_str()).await?;
                            let mut exprs = Vec::new();
                            for item in projection {
                                match item {
                                    sqlparser::ast::SelectItem::UnnamedExpr(expr)=>{
                                        exprs.push((self.build_expression(expr, &tabledef)?,None))
                                    },
                                    sqlparser::ast::SelectItem::ExprWithAlias { expr, alias}=>{
                                        let builtexpr = self.build_expression(expr, &tabledef)?;
                                        let builtalias = alias.value;
                                        exprs.push((builtexpr, Some(builtalias)));
                                    },
                                    sqlparser::ast::SelectItem::Wildcard(_) =>{
                                        // In this case, skip project node, i.e, get all column
                                        return Ok(root);
                                    },
                                    _=>{
                                            return Err(anyhow!("unsupported select item yet"));
                                    }
                                }
                            }
                            root = Node::Projection { source: Box::new(root), expression: exprs}
                        }
                    }
                    _=>{
                        return Err(anyhow!("unexpected body type for query"));
                    }
                }
                Ok(root)
            },
            ast::Statement::ShowTables {..}=>{
                Ok(Node::ShowTable)
            },
            ast::Statement::Drop { object_type, names, ..}=>{
                match object_type {
                    sqlparser::ast::ObjectType::Table =>{}
                    _=>{
                        return Err(anyhow!("unsupported drop type {:?}", object_type));
                    }
                }
                if names.len()>1 {
                    return Err(anyhow!("only one table can be dropped at at time."));
                }
                let tblname = names[0].0[0].value.clone();
                Ok(Node::DropTable { table: tblname })
            },
            _=> return Err(anyhow!("unsupported statment yet:{:?}", statement))
        }
    }

    // build expression from AST expression
    // not currently since single table is supported
    // here directly map the column name to the column index
    // for join case, it can be complicated. refer to the solution of using scope in toydb
    fn build_expression(&self, expr: sqlparser::ast::Expr, tabledef: &Table) -> Result<Expression> {
        Ok(match expr {
            sqlparser::ast::Expr::Value(val) => {
                {
                    match val {
                        sqlparser::ast::Value::Number(s, _)=> {
                            Expression::Constant(crate::planner::Value::Float(s.parse::<f64>().unwrap()))
                        },
                        sqlparser::ast::Value::DoubleQuotedString(s)=>{
                            Expression::Constant(crate::planner::Value::String(s.clone()))
                        },
                        sqlparser::ast::Value::Boolean(b)=>{
                            Expression::Constant(crate::planner::Value::Boolean(b.clone()))
                        },
                        _=>{ return Err(anyhow::anyhow!("unsupported default value type"));}
                    }
                }
            },
            sqlparser::ast::Expr::Identifier(ident) =>{
                let colname = ident.value;
                let idx = tabledef.columns.iter().position(|c|c.name == colname).unwrap();
                Expression::Field(idx, Some((Some(tabledef.name.clone()), tabledef.columns[idx].name.clone())))
            },
            sqlparser::ast::Expr::BinaryOp { left, op, right } =>{
                match op {
                    BinaryOperator::Eq=>{
                        let leftexp = self.build_expression(*left, tabledef)?;
                        let rightexp = self.build_expression(*right, tabledef)?;
                        Expression::Equal(leftexp.into(), rightexp.into())
                    },
                    _=>{
                        return Err(anyhow!("unsupported binary op:{:?}", op));
                    },
                }
            }
            _=>{
                return Err(anyhow!("unsupported expr type yet:{:?}", expr));
            }
        })
    }
}