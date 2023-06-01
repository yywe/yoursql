use crate::storage::Value;
use crate::storage::Row;
use anyhow::Result;
mod planner;
use sqlparser::ast;
use crate::storage::Storage;
use crate::storage::Table;
use crate::executor::ResultSet;
use crate::executor::Executor;
use std::sync::Arc;
use tokio::sync::Mutex;

use planner::Planner;

#[derive(Clone, Debug)]
pub enum Expression {
    Constant(Value),
    // column index ,optional <optional table name, column name>
    Field(usize, Option<(Option<String>, String)>),
    // equal condition
    Equal(Box<Expression>, Box<Expression>),
}

#[derive(Debug)]
pub struct Plan(pub Node);


#[derive(Debug)]
pub enum Node {
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    CreateTable {
        table: Table,
    }
}

impl Expression {
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value> {
        use Value::*;
        Ok(match self {
            Self::Constant(c) => c.clone(),
            Self::Field(i,_) => row.and_then(|row|row.get(*i).cloned()).unwrap_or(Null),
            Self::Equal(lhs, rhs) => match (lhs.evaluate(row)?, rhs.evaluate(row)?) {
                (Boolean(lhs), Boolean(rhs))=>Boolean(lhs==rhs),
                (Integer(lhs), Integer(rhs)) => Boolean(lhs==rhs),
                (Integer(lhs), Float(rhs)) => Boolean(lhs as f64 == rhs),
                (Float(lhs), Integer(rhs)) => Boolean(lhs == rhs as f64),
                (Float(lhs), Float(rhs)) => Boolean(lhs == rhs),
                (String(lhs), String(rhs)) => Boolean(lhs == rhs),
                (Null, _) | (_, Null) => Null,
                (lhs, rhs) => {
                    return Err(anyhow::anyhow!(format!("cannot compare {} and {}", lhs, rhs)));
                }
            },
        })
    }
}

impl Plan {
    pub fn build<T: Storage>(statement: ast::Statement, store: &T) ->Result<Self> {
        Planner::new(store).build(statement)
    }
    pub async fn execute<T: Storage + 'static>(self, store: Arc<Mutex<T>>) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(store).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::SledStore;
    use crate::parser::parse;
    use crate::executor::print_resultset;
    #[tokio::test]
    async fn test_plan() -> Result<()> {
        let mut ss: SledStore = SledStore::init(format!("./testplandb", ).as_str(), 2).await?;
        let sql = "create table tbl1(id int)";
        let astvec = parse(sql)?;
        let ast = astvec[0].clone();
        let plan = Plan::build(ast, &ss)?;
        let ans = plan.execute(Arc::new(Mutex::new(ss))).await?;
        print_resultset(ans).await?;
        Ok(())
    }
}