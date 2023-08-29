use crate::common::{column::Column, types::DataValue};
use anyhow::{Result,anyhow};
use crate::expr_vec_fmt;
use crate::common::types::DataType;

use super::type_coercion::{coerce_types, NUMERICS};
use crate::expr::type_coercion::signature;
#[derive(Debug,Clone)]
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
    AggregateFunction(AggregateFunction),
    Wildcard,
    QualifiedWildcard {qualifier: String},
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone)]
pub enum AggregateFunctionType {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggregateFunctionType {
    fn name(&self) -> &str {
        match self {
            AggregateFunctionType::Count=>"COUNT",
            AggregateFunctionType::Sum=>"SUM",
            AggregateFunctionType::Min=>"MIN",
            AggregateFunctionType::Max=>"MAX",
            AggregateFunctionType::Avg=>"AVG",
        }
    }
    pub fn return_type(fun: &AggregateFunctionType, input_exprs: &[DataType])->Result<DataType>{
        let coerced_data_types = coerce_types(fun, input_exprs, &signature(fun))?;
        match fun {
            AggregateFunctionType::Count=>{
                Ok(DataType::Int64)
            }
            AggregateFunctionType::Max | AggregateFunctionType::Min => {
                Ok(coerced_data_types[0].clone())
            }
            AggregateFunctionType::Sum => {
                match &coerced_data_types[0] {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 =>{
                        Ok(DataType::Int64)
                    }
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 =>{
                        Ok(DataType::UInt64)
                    }
                    DataType::Float32 | DataType::Float64 =>{
                        Ok(DataType::Float64)
                    }
                    other=>{
                        Err(anyhow!(format!("sum does not support type {other:?}")))
                    }
                }
            }
            AggregateFunctionType::Avg =>{
                match &coerced_data_types[0] {
                    _arg_type if NUMERICS.contains(&coerced_data_types[0]) => {
                        Ok(DataType::Float64)
                    }
                    other => Err(anyhow!(format!("avg does not support type {other:?}")))
                }
            }
        }
    }
}

impl std::fmt::Display for AggregateFunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub fun: AggregateFunctionType,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Option<Vec<Expr>>,
}


impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_child(f: &mut std::fmt::Formatter<'_>, expr: &Expr, precedence:u8) -> std::fmt::Result {
            match expr {
                Expr::BinaryExpr(child)=>{
                    let p = child.op.precedence();
                    if p==0 || p<precedence {
                        write!(f, "({child})")?;
                    }else{
                        write!(f, "{child}")?;
                    }
                }
                _=>write!(f, "{expr}")?,
            }
            Ok(())
        }
        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

#[derive(Debug,Clone)]
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

#[derive(Debug,Clone)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
}

#[derive(Debug, Clone)]
pub struct Between {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub low: Box<Expr>,
    pub high: Box<Expr>,
}

impl Operator {
    pub fn is_numerical_operator(&self) -> bool {
        matches!(self, Operator::Plus |Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo )
    }
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 5,
            Operator::And=>10,
            Operator::NotEq 
            | Operator::Eq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => 20,
            Operator::Plus|Operator::Minus=>30,
            Operator::Multiply | Operator::Divide | Operator::Modulo => 40
        }
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match &self {
            Operator::Eq=>"=",
            Operator::NotEq=>"!=",
            Operator::Lt=>"<",
            Operator::LtEq=>"<=",
            Operator::Gt=>">",
            Operator::GtEq=>">=",
            Operator::Plus=>"+",
            Operator::Minus=>"-",
            Operator::Multiply=>"*",
            Operator::Divide=>"/",
            Operator::Modulo=>"%",
            Operator::And=>"AND",
            Operator::Or=>"OR",
        };
        write!(f,"{display}")
    }
}

impl Expr {
    pub fn display_name(&self) -> Result<String> {
        create_name(self)
    }
}

fn create_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(c) => Ok(c.flat_name()),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(binary_expr)=>{
            let left = create_name(binary_expr.left.as_ref())?;
            let right = create_name(binary_expr.right.as_ref())?;
            Ok(format!("{}{}{}", left, binary_expr.op, right))
        },
        Expr::Like(Like { negated, expr, pattern, escape_char })=>{
            let s = format!("{}{}{}{}", expr, if *negated {"NOT LIKE"} else {"LIKE"}, pattern, if let Some(char)=escape_char {format!("CHAR '{char}'")} else {"".to_string()});
            Ok(s)
        },
        Expr::ILike(Like { negated, expr, pattern, escape_char })=>{
            let s = format!("{}{}{}{}", expr, if *negated {"NOT ILIKE"} else {"ILIKE"}, pattern, if let Some(char)=escape_char {format!("CHAR '{char}'")} else {"".to_string()});
            Ok(s) 
        },
        Expr::Not(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::IsNull(expr)=>{
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr)=>{
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT TRUE")) 
        }
        Expr::IsFalse(expr)=>{
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsNotFalse(expr)=>{
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::Wildcard=>Ok("*".to_string()),
        Expr::QualifiedWildcard { .. } => Err(anyhow!("create name does not support qualified wildcard")),
        Expr::Between(Between { expr, negated, low, high })=>{
            let expr = create_name(expr)?;
            let low = create_name(low)?;
            let high= create_name(high)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} and {high}"))
            }else{
                Ok(format!("{expr} BETWEEN {low} and {high}"))
            }
        },
        Expr::AggregateFunction(AggregateFunction{fun, distinct, args, filter, order_by})=>{
            let mut name = create_function_name(&fun.to_string(), *distinct, args)?;
            if let Some(fe) = filter {
                name = format!("{name} FILTER (WHERE {fe})");
            };
            if let Some(order_by) = order_by {
                name= format!("{name} ORDER BY [{}]", expr_vec_fmt!(order_by));
            };
            Ok(name)
        }
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{expr} AS {alias}"),
            Expr::Column(c) => write!(f, "{c}"),
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Not(expr)=> write!(f, "NOT {expr}"),
            Expr::IsNull(expr)=>write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr)=>write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr)=>write!(f, "{expr} IS TRUE"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsFalse(expr)=>write!(f,"{expr} IS FALSE"),
            Expr::IsNotFalse(expr)=>write!(f,"{expr} IS NOT FALSE"),
            Expr::BinaryExpr(expr)=>write!(f,"{expr}"),
            Expr::Between(Between { expr, negated, low, high })=>{
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                }else{
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(Like { negated, expr, pattern, escape_char })=>{
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " LIKE {pattern} ESCAPE '{char}'")
                }else{
                    write!(f, " LIKE {pattern}")
                }
            }
            Expr::ILike(Like { negated, expr, pattern, escape_char })=>{
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " ILIKE {pattern} ESCAPE '{char}'")
                }else{
                    write!(f, " ILIKE {pattern}")
                }
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard { qualifier }=>write!(f, "{qualifier}.*"),
            Expr::AggregateFunction(AggregateFunction{fun, distinct, ref args, filter, order_by,..})=>{
                fmt_function(f, &fun.to_string(), *distinct, args, true);
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe}")?;
                }

                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
        }
    }
}


fn fmt_function(f: &mut std::fmt::Formatter, func: &str, distinct: bool, args: &[Expr], display: bool) -> std::fmt::Result {
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
        $ARRAY.iter().map(|e|format!("{e}")).collect::<Vec<String>>().join(", ")
    };
}

fn create_function_name(fun: &str, distinct: bool, args: &[Expr]) -> Result<String> {
    let names: Vec<String> = args.iter().map(create_name).collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT",
        false=>"",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}
