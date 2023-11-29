use super::PhysicalExpr;
use crate::common::schema::Schema;
use crate::expr::expr::BinaryExpr as LogicalBinaryExpr;
use crate::expr::expr::Expr;
use crate::physical_expr::physical_expr::{BinaryExpr, Column, Literal};
use anyhow::{anyhow, Context, Result};
use std::sync::Arc;

/// input_schema is the schema of physical plan, input_logischema is the logical plan schema, 
/// main difference is that logical schema may has qualifier while physical schema do not
pub fn create_physical_expr(e: &Expr, input_schema: &Schema, input_logischema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(expr, input_schema, input_logischema)?),
        Expr::Column(c) => {
            let idx = input_logischema
                .index_of_column_by_name(c.relation.as_ref(), &c.name)?
                .context(format!("failed to find column in create physical expr {:?}", c))?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::BinaryExpr(LogicalBinaryExpr { left, op, right }) => {
            let lhs = create_physical_expr(left, input_schema, input_logischema)?;
            let rhs = create_physical_expr(right, input_schema, input_logischema)?;
            Ok(Arc::new(BinaryExpr::new(lhs, *op, rhs)))
        }
        other => Err(anyhow!(
            "physical expr does not support logical expr {other:?} yet"
        )),
    }
}
