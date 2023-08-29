use crate::common::tree_node::TreeNode;
use crate::common::tree_node::VisitRecursion;
use crate::expr::expr::Expr;
use crate::expr::expr::{Between, BinaryExpr, Like};
use anyhow::Result;

impl TreeNode for Expr {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = match self {
            Expr::Alias(expr, _)
            | Expr::Not(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr) => vec![expr.as_ref().clone()],

            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => vec![],

            Expr::Like(Like { expr, pattern, .. }) | Expr::ILike(Like { expr, pattern, .. }) => {
                vec![expr.as_ref().clone(), pattern.as_ref().clone()]
            }

            Expr::Between(Between {
                expr, low, high, ..
            }) => vec![
                expr.as_ref().clone(),
                low.as_ref().clone(),
                high.as_ref().clone(),
            ],
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                vec![left.as_ref().clone(), right.as_ref().clone()]
            }
        };

        for child in children.iter() {
            match op(child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }
}
