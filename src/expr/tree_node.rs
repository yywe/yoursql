use crate::{
    common::tree_node::{TreeNode, VisitRecursion},
    expr::expr::{AggregateFunction, Between, BinaryExpr, Expr, Like, Sort},
};
use anyhow::Result;

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
            | Expr::Sort(Sort { expr, .. })
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
            Expr::AggregateFunction(AggregateFunction {
                args,
                filter,
                order_by,
                ..
            }) => {
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

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let mut transform = transform;
        Ok(match self {
            Expr::Alias(expr, name) => Expr::Alias(transform_boxed(expr, &mut transform)?, name),
            Expr::Column(_) => self,
            Expr::Literal(value) => Expr::Literal(value),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => Expr::BinaryExpr(BinaryExpr::new(
                transform_boxed(left, &mut transform)?,
                op,
                transform_boxed(right, &mut transform)?,
            )),
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::Like(Like {
                negated: negated,
                expr: transform_boxed(expr, &mut transform)?,
                pattern: transform_boxed(pattern, &mut transform)?,
                escape_char,
            }),
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::ILike(Like {
                negated: negated,
                expr: transform_boxed(expr, &mut transform)?,
                pattern: transform_boxed(pattern, &mut transform)?,
                escape_char,
            }),
            Expr::Not(expr) => Expr::Not(transform_boxed(expr, &mut transform)?),
            Expr::IsNull(expr) => Expr::IsNull(transform_boxed(expr, &mut transform)?),
            Expr::IsNotNull(expr) => Expr::IsNotNull(transform_boxed(expr, &mut transform)?),
            Expr::IsTrue(expr) => Expr::IsTrue(transform_boxed(expr, &mut transform)?),
            Expr::IsNotTrue(expr) => Expr::IsNotTrue(transform_boxed(expr, &mut transform)?),
            Expr::IsNotFalse(expr) => Expr::IsNotFalse(transform_boxed(expr, &mut transform)?),
            Expr::IsFalse(expr) => Expr::IsFalse(transform_boxed(expr, &mut transform)?),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Expr::Between(Between {
                expr: transform_boxed(expr, &mut transform)?,
                negated: negated,
                low: transform_boxed(low, &mut transform)?,
                high: transform_boxed(high, &mut transform)?,
            }),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort {
                expr: transform_boxed(expr, &mut transform)?,
                asc: asc,
                nulls_first: nulls_first,
            }),
            Expr::AggregateFunction(AggregateFunction {
                fun,
                args,
                distinct,
                filter,
                order_by,
            }) => Expr::AggregateFunction(AggregateFunction {
                fun: fun,
                args: transform_vec(args, &mut transform)?,
                distinct: distinct,
                filter: transform_option_box(filter, &mut transform)?,
                order_by: transform_option_vec(order_by, &mut transform)?,
            }),
            Expr::Wildcard => Expr::Wildcard,
            Expr::QualifiedWildcard { qualifier } => Expr::QualifiedWildcard { qualifier },
        })
    }
}

fn transform_boxed<F>(boxed_expr: Box<Expr>, transform: &mut F) -> Result<Box<Expr>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    let expr: Expr = *boxed_expr;
    let rewritten_expr = transform(expr)?;
    Ok(Box::new(rewritten_expr))
}

fn transform_option_box<F>(
    option_box: Option<Box<Expr>>,
    transform: &mut F,
) -> Result<Option<Box<Expr>>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    option_box
        .map(|expr| transform_boxed(expr, transform))
        .transpose()
}

fn transform_option_vec<F>(
    option_vec: Option<Vec<Expr>>,
    transform: &mut F,
) -> Result<Option<Vec<Expr>>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    Ok(if let Some(expr) = option_vec {
        Some(transform_vec(expr, transform)?)
    } else {
        None
    })
}

fn transform_vec<F>(v: Vec<Expr>, transform: &mut F) -> Result<Vec<Expr>>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    v.into_iter().map(transform).collect()
}

impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.inputs() {
            match op(child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    /// actually since for now we did not support subquery, can use default impl. no need here. same next for visit
    fn apply<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        match op(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        };
        self.apply_children(&mut |node| node.apply(op))
    }

    fn visit<V: crate::common::tree_node::TreeNodeVisitor<N = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitRecursion> {
        match visitor.pre_visit(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        match self.apply_children(&mut |node| node.visit(visitor))? {
            VisitRecursion::Continue => {}
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        visitor.post_visit(self)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let old_children = self.inputs();
        let new_children = old_children
            .iter()
            .map(|&c| c.clone())
            .map(transform)
            .collect::<Result<Vec<_>>>()?;
        if old_children
            .iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != &c2)
        {
            self.with_new_inputs(new_children.as_slice())
        } else {
            Ok(self)
        }
    }
}
