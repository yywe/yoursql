use crate::common::{column::Column, types::DataValue};
use anyhow::{Result,anyhow};

#[derive(Debug)]
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
    Wildcard,
    QualifiedWildcard {qualifier: String},
}

#[derive(Debug)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
}

#[derive(Debug)]
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
            Expr::QualifiedWildcard { qualifier }=>write!(f, "{qualifier}.*")
        }
    }
}