use crate::storage::Storage;
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
            _=> return Err(anyhow!("unsupported statment yet:{:?}", statement))
        }
    }
}