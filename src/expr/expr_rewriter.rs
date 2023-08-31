use anyhow::Result;
use crate::common::tree_node::TreeNode;
use crate::expr::expr::Expr;
use crate::expr::logical_plan::LogicalPlan;
use crate::common::tree_node::Transformed;
use super::logical_plan::builder::LogicalPlanBuilder;

pub fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.transform(& |expr|{
        Ok(
            {
                if let Expr::Column(c) = expr {
                    let col = LogicalPlanBuilder::normalize(plan, c)?;
                    Transformed::Yes(Expr::Column(col))
                }else{
                    Transformed::No(expr)
                }
            }
        )
    })
}