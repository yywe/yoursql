use crate::common::types::DataType;
use crate::common::{column::Column, types::DataValue};
use crate::expr_vec_fmt;
use anyhow::{anyhow, Result};
use std::collections::HashSet;

use super::type_coercion::{coerce_types, NUMERICS};
use crate::expr::type_coercion::signature;
use crate::expr::utils::extract_columns_from_expr;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    Alias(Box<Expr>, String),
    Column(Column),
    Literal(DataValue),
    BinaryExpr(BinaryExpr),
    Like(Like),
    ILike(Like),
    Not(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsNull(Box<Expr>),
    IsTrue(Box<Expr>),
    IsFalse(Box<Expr>),
    IsNotTrue(Box<Expr>),
    IsNotFalse(Box<Expr>),
    Between(Between),
    Sort(Sort),
    AggregateFunction(AggregateFunction),
    Wildcard,
    QualifiedWildcard { qualifier: String },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}

pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum AggregateFunctionType {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl std::str::FromStr for AggregateFunctionType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "count" => AggregateFunctionType::Count,
            "sum" => AggregateFunctionType::Sum,
            "min" => AggregateFunctionType::Min,
            "max" => AggregateFunctionType::Max,
            "avg" => AggregateFunctionType::Avg,
            _ => return Err(anyhow!("unsupported function name {}", s)),
        })
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Sort {
    pub expr: Box<Expr>,
    pub asc: bool,
    pub nulls_first: bool,
}

impl AggregateFunctionType {
    fn name(&self) -> &str {
        match self {
            AggregateFunctionType::Count => "COUNT",
            AggregateFunctionType::Sum => "SUM",
            AggregateFunctionType::Min => "MIN",
            AggregateFunctionType::Max => "MAX",
            AggregateFunctionType::Avg => "AVG",
        }
    }
    pub fn return_type(fun: &AggregateFunctionType, input_exprs: &[DataType]) -> Result<DataType> {
        let coerced_data_types = coerce_types(fun, input_exprs, &signature(fun))?;
        match fun {
            AggregateFunctionType::Count => Ok(DataType::Int64),
            AggregateFunctionType::Max | AggregateFunctionType::Min => {
                Ok(coerced_data_types[0].clone())
            }
            AggregateFunctionType::Sum => match &coerced_data_types[0] {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    Ok(DataType::Int64)
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Ok(DataType::UInt64)
                }
                DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
                other => Err(anyhow!(format!("sum does not support type {other:?}"))),
            },
            AggregateFunctionType::Avg => match &coerced_data_types[0] {
                _arg_type if NUMERICS.contains(&coerced_data_types[0]) => Ok(DataType::Float64),
                other => Err(anyhow!(format!("avg does not support type {other:?}"))),
            },
        }
    }
}

impl std::fmt::Display for AggregateFunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AggregateFunction {
    pub fun: AggregateFunctionType,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<Expr>>,
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_child(
            f: &mut std::fmt::Formatter<'_>,
            expr: &Expr,
            precedence: u8,
        ) -> std::fmt::Result {
            match expr {
                Expr::BinaryExpr(child) => {
                    let p = child.op.precedence();
                    if p == 0 || p < precedence {
                        write!(f, "({child})")?;
                    } else {
                        write!(f, "{child}")?;
                    }
                }
                _ => write!(f, "{expr}")?,
            }
            Ok(())
        }
        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
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
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Between {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub low: Box<Expr>,
    pub high: Box<Expr>,
}

impl Operator {
    pub fn is_numerical_operator(&self) -> bool {
        matches!(
            self,
            Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
        )
    }
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 5,
            Operator::And => 10,
            Operator::NotEq
            | Operator::Eq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => 20,
            Operator::Plus | Operator::Minus => 30,
            Operator::Multiply | Operator::Divide | Operator::Modulo => 40,
            Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => 0,
        }
    }
}

impl std::ops::Add for Expr {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        binary_expr(self, Operator::Plus, rhs)
    }
}

impl std::ops::Sub for Expr {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        binary_expr(self, Operator::Minus, rhs)
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::IsDistinctFrom => "IS DISTINCT FROM",
            Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        };
        write!(f, "{display}")
    }
}

impl Expr {
    pub fn display_name(&self) -> Result<String> {
        create_name(self)
    }

    pub fn try_into_col(&self) -> Result<Column> {
        match self {
            Expr::Column(c) => Ok(c.clone()),
            _ => Err(anyhow!(format!("cannot convert '{self}' into Column"))),
        }
    }

    pub fn eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(self),
            Operator::Eq,
            Box::new(other),
        ))
    }

    pub fn alias(self, name: impl Into<String>) -> Expr {
        match self {
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort {
                expr: Box::new(expr.alias(name)),
                asc: asc,
                nulls_first: nulls_first,
            }),
            _ => Expr::Alias(Box::new(self), name.into()),
        }
    }

    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(expr, _) => expr.as_ref().clone(),
            _ => self,
        }
    }
    pub fn sort(self, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort(Sort {
            expr: Box::new(self),
            asc: asc,
            nulls_first: nulls_first,
        })
    }

    /// get all columns that are used in this expr
    pub fn used_columns(&self) -> Result<HashSet<Column>> {
        let mut used_columns = HashSet::new();
        extract_columns_from_expr(self, &mut used_columns)?;
        Ok(used_columns)
    }
}

fn create_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(c) => Ok(c.flat_name()),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(binary_expr) => {
            let left = create_name(binary_expr.left.as_ref())?;
            let right = create_name(binary_expr.right.as_ref())?;
            Ok(format!("{}{}{}", left, binary_expr.op, right))
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let s = format!(
                "{}{}{}{}",
                expr,
                if *negated { "NOT LIKE" } else { "LIKE" },
                pattern,
                if let Some(char) = escape_char {
                    format!("CHAR '{char}'")
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let s = format!(
                "{}{}{}{}",
                expr,
                if *negated { "NOT ILIKE" } else { "ILIKE" },
                pattern,
                if let Some(char) = escape_char {
                    format!("CHAR '{char}'")
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::Not(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::IsNull(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::Wildcard => Ok("*".to_string()),
        Expr::QualifiedWildcard { .. } => {
            Err(anyhow!("create name does not support qualified wildcard"))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_name(expr)?;
            let low = create_name(low)?;
            let high = create_name(high)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} and {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} and {high}"))
            }
        }
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            filter,
            order_by,
        }) => {
            let mut name = create_function_name(&fun.to_string(), *distinct, args)?;
            if let Some(fe) = filter {
                name = format!("{name} FILTER (WHERE {fe})");
            };
            if let Some(order_by) = order_by {
                name = format!("{name} ORDER BY [{}]", expr_vec_fmt!(order_by));
            };
            Ok(name)
        }
        Expr::Sort { .. } => Err(anyhow!("create name does not support sort expression")),
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{expr} AS {alias}"),
            Expr::Column(c) => write!(f, "{c}"),
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " LIKE {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " LIKE {pattern}")
                }
            }
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " ILIKE {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " ILIKE {pattern}")
                }
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard { qualifier } => write!(f, "{qualifier}.*"),
            Expr::AggregateFunction(AggregateFunction {
                fun,
                distinct,
                ref args,
                filter,
                order_by,
                ..
            }) => {
                fmt_function(f, &fun.to_string(), *distinct, args, true)?;
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe}")?;
                }

                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => {
                if *asc {
                    write!(f, "{expr} ASC")?;
                } else {
                    write!(f, "{expr} DESC")?;
                }
                if *nulls_first {
                    write!(f, " NULLS FIRST")
                } else {
                    write!(f, " NULLS LAST")
                }
            }
        }
    }
}

fn fmt_function(
    f: &mut std::fmt::Formatter,
    func: &str,
    distinct: bool,
    args: &[Expr],
    display: bool,
) -> std::fmt::Result {
    let args: Vec<String> = match display {
        true => args.iter().map(|arg| format!("{arg}")).collect(),
        false => args.iter().map(|arg| format!("{arg:?}")).collect(),
    };
    let distinct_str = match distinct {
        true => "DISTINCT",
        false => "",
    };
    write!(f, "{}({}{})", func, distinct_str, args.join(", "))
}

#[macro_export]
macro_rules! expr_vec_fmt {
    ($ARRAY: expr) => {
        $ARRAY
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ")
    };
}

fn create_function_name(fun: &str, distinct: bool, args: &[Expr]) -> Result<String> {
    let names: Vec<String> = args.iter().map(create_name).collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}
