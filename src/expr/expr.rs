use crate::common::{column::Column, scalar::ScalarValue};
pub enum Expr {
    Alias(Box<Expr>, String),
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
}


pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
}