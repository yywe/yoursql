use crate::common::tree_node::TreeNode;
use crate::common::tree_node::VisitRecursion;
use crate::expr::expr::Expr;
use crate::expr::expr::{Between, BinaryExpr, Like, Sort};
use anyhow::Result;
use crate::expr::expr::AggregateFunction;

use super::logical_plan::LogicalPlan;

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
            | Expr::Sort(Sort{expr,..})
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
            },
            Expr::AggregateFunction(AggregateFunction{args,filter, order_by,..}) => {
                let mut expr_vec = args.clone();
                if let Some(f) = filter {
                    expr_vec.push(f.as_ref().clone());
                }
                if let Some(o) = order_by {
                    expr_vec.extend(o.clone())
                }
                expr_vec
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


impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion> 
        where  F: FnMut(&Self) -> Result<VisitRecursion> {
        for child in self.inputs() {
            match op(child)? {
                VisitRecursion::Continue=>{},
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    /// actually since for now we did not support subquery, can use default impl. no need here. same next for visit
    fn apply<F>(&self, op: &mut F) -> Result<VisitRecursion>
        where F: FnMut(&Self) -> Result<VisitRecursion> {
        match op(self)? {
            VisitRecursion::Continue=>{},
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        };
        self.apply_children(&mut |node| node.apply(op))
    }

    fn visit<V: crate::common::tree_node::TreeNodeVisitor<N = Self>>(&self, visitor: &mut V) -> Result<VisitRecursion> {
        match visitor.pre_visit(self)?{
            VisitRecursion::Continue=>{},
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        match self.apply_children(&mut |node| node.visit(visitor))?{
            VisitRecursion::Continue=>{},
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        visitor.post_visit(self)
    }
}
