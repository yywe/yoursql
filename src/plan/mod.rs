use crate::storage::Value;
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