use sqlparser::ast::Ident;
use crate::expr::{expr::Expr, logical_plan::LogicalPlan, expr_rewriter::clone_with_replacement, utils::find_column_exprs};
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use crate::common::column::Column;

pub fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) =>id.value,
        None=>id.value.to_ascii_lowercase(),
    }
}

pub fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    match expr {
        Expr::Column(col)=>{
            let s = plan.output_schema();
            let field =s.field_from_column(col)?;
            Ok(Expr::Column(field.qualified_column()))
        }
        _=>Ok(Expr::Column(Column { relation: None, name: expr.display_name()? }))
    }
}

pub fn rebase_expr(expr: &Expr, base_exprs: &[Expr], plan: &LogicalPlan) -> Result<Expr> {
    clone_with_replacement(expr, &|e|{
        if base_exprs.contains(e) {
            Ok(Some(expr_as_column_expr(e, plan)?))
        }else{
            Ok(None)
        }
    })
}

pub fn check_columns_satisfy_exprs(columns: &[Expr], exprs: &[Expr], message_prefix: &str) -> Result<()> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_)=>Ok(()),
        _=>Err(anyhow!("Expr::Column required"))
    })?;
    let column_exprs = find_column_exprs(exprs);
    for e in &column_exprs {
        check_columns_satisfy_expr(columns, e, message_prefix)?;
    }
    Ok(())
}

fn check_columns_satisfy_expr(columns: &[Expr], expr: &Expr, message_prefix: &str) -> Result<()> {
    if !columns.contains(expr) {
        return Err(anyhow!(format!("{}: Expression {} could not be resolved from avaialble columns: {:?}", message_prefix, expr, columns)))
    }
    Ok(())
}