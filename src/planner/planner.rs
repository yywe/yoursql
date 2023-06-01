use crate::storage::{Storage, Table, Column};
use anyhow::{Result, anyhow};
use crate::planner::Plan;
use sqlparser::ast;
use crate::planner::Node;

pub struct Planner<'a, S: Storage> {
    store: &'a S,
}

impl<'a, S: Storage> Planner<'a, S> {
    pub fn new(store: &'a S) ->Self {
        Self {store}
    }
    pub fn build(&self, statement: ast::Statement) -> Result<Plan>{
        Ok(Plan(self.build_statement(statement)?))
    }
    fn build_statement(&self, statement: ast::Statement)->Result<Node> {
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
            _=> return Err(anyhow!("unsupported statment yet:{:?}", statement))
        }
    }
}