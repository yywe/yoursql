use crate::common::types::DataType;
use crate::expr::expr::Operator;
use anyhow::{Context, Result};

pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(dt, DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)
}

pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt) || matches!(dt, DataType::UInt8| DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
}

pub fn both_numeric_or_null_and_numeric(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    match (lhs_type, rhs_type) {
        (_, DataType::Null) => is_numeric(lhs_type),
        (DataType::Null, _)=> is_numeric(rhs_type),
        _ => is_numeric(lhs_type) && is_numeric(rhs_type),
    }
}

pub fn get_result_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    let result = match op {
        Operator::And
        | Operator::Or
        | Operator::Eq
        | Operator::NotEq
        | Operator::Gt
        | Operator::GtEq
        | Operator::Lt
        | Operator::LtEq => Some(DataType::Boolean),
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => mathmatical_numerical_coercion(lhs_type, rhs_type),
    };
    result.context(format!(
        "unsupported type. cannot evaluate {lhs_type:?} {op} {rhs_type:?}"
    ))
}

fn mathmatical_numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None
    }
    match (lhs_type, rhs_type) {
        (DataType::Float64, _) | (_, DataType::Float64) => Some(DataType::Float64),
        (DataType::Float32, _) | (_, DataType::Float32) => Some(DataType::Float64),
        (DataType::Int64, _) | (_, DataType::Int64) => Some(DataType::Int64),
        (DataType::Int32, _) | (_, DataType::Int32) => Some(DataType::Int32),
        (DataType::Int16, _) | (_, DataType::Int16) => Some(DataType::Int16),
        (DataType::Int8, _) | (_, DataType::Int8) => Some(DataType::Int8),
        (DataType::UInt64, _) | (_, DataType::UInt64) => Some(DataType::UInt64),
        (DataType::UInt32, _) | (_, DataType::UInt32) => Some(DataType::UInt32),
        (DataType::UInt16, _) | (_, DataType::UInt16) => Some(DataType::UInt16),
        (DataType::UInt8, _) | (_, DataType::UInt8) => Some(DataType::UInt8),  
        _=>None
    }
}
