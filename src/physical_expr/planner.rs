use super::PhysicalExpr;
use crate::common::schema::Schema;
use crate::common::types::DataValue;
use crate::expr::expr::BinaryExpr as LogicalBinaryExpr;
use crate::expr::expr::Expr;
use crate::expr::expr::Operator;
use crate::expr::expr::{binary_expr, Between, Like};
use crate::physical_expr::physical_expr::{BinaryExpr, Column, Literal};
use crate::physical_expr::physical_expr::{IsNotNullExpr, IsNullExpr, LikeExpr, NotExpr};
use anyhow::{Context, Result};
use std::sync::Arc;

/// input_schema is the schema of physical plan, input_logischema is the logical plan schema,
/// main difference is that logical schema may has qualifier while physical schema do not
pub fn create_physical_expr(
    e: &Expr,
    input_schema: &Schema,
    input_logischema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(expr, input_schema, input_logischema)?),
        Expr::Column(c) => {
            let idx = input_logischema
                .index_of_column_by_name(c.relation.as_ref(), &c.name)?
                .context(format!(
                    "failed to find column in create physical expr {:?}",
                    c
                ))?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::BinaryExpr(LogicalBinaryExpr { left, op, right }) => {
            let lhs = create_physical_expr(left, input_schema, input_logischema)?;
            let rhs = create_physical_expr(right, input_schema, input_logischema)?;
            Ok(Arc::new(BinaryExpr::new(lhs, *op, rhs)))
        }
        Expr::IsTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(DataValue::Boolean(Some(true))),
            );
            create_physical_expr(&binary_op, input_schema, input_logischema)
        }
        Expr::IsNotTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(DataValue::Boolean(Some(true))),
            );
            create_physical_expr(&binary_op, input_schema, input_logischema)
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(DataValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_schema, input_logischema)
        }
        Expr::IsNotFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(DataValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_schema, input_logischema)
        }
        Expr::IsNull(expr) => {
            let arg = create_physical_expr(expr, input_schema, input_logischema)?;
            Ok(Arc::new(IsNullExpr::new(arg)))
        }
        Expr::IsNotNull(expr) => {
            let arg = create_physical_expr(expr, input_schema, input_logischema)?;
            Ok(Arc::new(IsNotNullExpr::new(arg)))
        }
        Expr::Not(expr) => {
            let arg = create_physical_expr(expr, input_schema, input_logischema)?;
            Ok(Arc::new(NotExpr::new(arg)))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let value_expr = create_physical_expr(expr, input_schema, input_logischema)?;
            let low_expr = create_physical_expr(low, input_schema, input_logischema)?;
            let high_expr = create_physical_expr(high, input_schema, input_logischema)?;
            let binary_expr = binary(
                binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                Operator::And,
                binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                input_schema,
            );
            if *negated {
                Ok(Arc::new(NotExpr::new(binary_expr?)))
            } else {
                binary_expr
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            if escape_char.is_some() {
                return Err(anyhow::anyhow!("LIKE does not support escape_char"));
            }
            let physical_expr = create_physical_expr(expr, input_schema, input_logischema)?;
            let physical_pattern = create_physical_expr(pattern, input_schema, input_logischema)?;
            Ok(Arc::new(LikeExpr::new(
                *negated,
                false,
                physical_expr,
                physical_pattern,
            )))
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            if escape_char.is_some() {
                return Err(anyhow::anyhow!("ILIKE does not support escape_char"));
            }
            let physical_expr = create_physical_expr(expr, input_schema, input_logischema)?;
            let physical_pattern = create_physical_expr(pattern, input_schema, input_logischema)?;
            Ok(Arc::new(LikeExpr::new(
                *negated,
                true,
                physical_expr,
                physical_pattern,
            )))
        }
        other => Err(anyhow::anyhow!(format!(
            "Physical plan does not support logical expression {other:?}"
        ))),
    }
}

pub fn binary(
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    _input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(BinaryExpr::new(lhs, op, rhs)))
}
