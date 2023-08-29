use crate::common::column::Column;
use crate::common::tree_node::VisitRecursion;
use crate::{common::tree_node::TreeNode, expr::expr::Expr};
use anyhow::Result;

use super::logical_plan::Aggregate;

pub fn inspect_expr_pre<F>(expr: &Expr, mut f: F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<()>,
{
    let mut err = Ok(());
    expr.apply(&mut |expr| {
        if let Err(e) = f(expr) {
            err = Err(e);
            Ok(VisitRecursion::Stop)
        } else {
            Ok(VisitRecursion::Continue)
        }
    })
    .expect("cannot happen here");
    err
}

pub fn find_columns_referred_by_expr(e: &Expr) -> Vec<Column> {
    let mut exprs = vec![];
    inspect_expr_pre(e, |expr| {
        if let Expr::Column(c) = expr {
            exprs.push(c.clone())
        }
        Ok(())
    })
    .expect("cannot happen");
    exprs
}

pub fn find_column_exprs(exprs: &[Expr]) -> Vec<Expr> {
    exprs
        .iter()
        .flat_map(find_columns_referred_by_expr)
        .map(Expr::Column)
        .collect()
}

pub fn agg_cols(agg: &Aggregate) -> Vec<Column> {
    agg.aggr_expr
        .iter()
        .chain(&agg.group_expr)
        .flat_map(find_columns_referred_by_expr)
        .collect()
}
