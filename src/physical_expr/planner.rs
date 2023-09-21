use super::PhysicalExpr;
use crate::common::schema::Schema;
use crate::expr::expr::BinaryExpr as LogicalBinaryExpr;
use crate::expr::expr::Expr;
use crate::physical_expr::physical_expr::{BinaryExpr, Column, Literal};
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;

pub fn create_physical_expr(e: &Expr, input_schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(expr, input_schema)?),
        Expr::Column(c) => {
            let idx = input_schema
                .index_of_column_by_name(c.relation.as_ref(), &c.name)?
                .context(format!("failed to find column {:?}", c))?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::BinaryExpr(LogicalBinaryExpr { left, op, right }) => {
            let lhs = create_physical_expr(left, input_schema)?;
            let rhs = create_physical_expr(right, input_schema)?;
            Ok(Arc::new(BinaryExpr::new(lhs, *op, rhs)))
        }
        other => Err(anyhow!(
            "physical expr does not support logical expr {other:?} yet"
        )),
    }
}
