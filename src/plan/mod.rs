use crate::storage::Value;
use crate::storage::Row;
use anyhow::Result;
pub enum Expression {
    Constant(Value),
    // column index ,optional <optional table name, column name>
    Field(usize, Option<(Option<String>, String)>),
    // equal condition
    Equal(Box<Expression>, Box<Expression>),
}

pub enum Node {
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
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