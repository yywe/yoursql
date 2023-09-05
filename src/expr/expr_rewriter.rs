use super::expr_schema::ExprToSchema;
use super::logical_plan::builder::LogicalPlanBuilder;
use crate::common::column::Column;
use crate::common::tree_node::Transformed;
use crate::common::tree_node::TreeNode;
use crate::expr::expr::Expr;
use crate::expr::expr::Sort;
use crate::expr::logical_plan::LogicalPlan;
use anyhow::Result;
use crate::common::schema::Schema;
use std::collections::HashSet;

pub fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.transform(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col = LogicalPlanBuilder::normalize(plan, c)?;
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
}

pub fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}

/// rewrite the sort expression that has aggregation to use existing aggregation output column if any
/// e.g, select a, max(b) from t group by a order by  max(b)
/// will rewrite to: select a, max(b) from t group by a order by  col(max(b))
/// however, if the order by max(b) does not exist in the projection list
/// like select a from t group by a order by max(b), the rewrite will not be able to rewrite

pub fn rewrite_sort_cols_by_aggs(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| {
            let expr = e.into();
            match expr {
                Expr::Sort(Sort {
                    expr,
                    asc,
                    nulls_first,
                }) => {
                    let sort = Expr::Sort(Sort {
                        expr: Box::new(rewrite_sort_col_by_aggs(*expr, plan)?),
                        asc: asc,
                        nulls_first: nulls_first,
                    });
                    Ok(sort)
                }
                expr => Ok(expr),
            }
        })
        .collect()
}

/// func for a single expr used in a single sort expr
/// plan is current plan, sort is on top of that, plan_inputs is bottom layer
fn rewrite_sort_col_by_aggs(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    let plan_inputs = plan.inputs();
    // requires input plan is 1
    if plan_inputs.len() == 1 {
        let proj_exprs = plan.expressions();
        rewrite_in_terms_of_projection(expr, proj_exprs, plan_inputs[0])
    } else {
        Ok(expr)
    }
}

fn rewrite_in_terms_of_projection(
    expr: Expr,
    proj_exprs: Vec<Expr>,
    input: &LogicalPlan,
) -> Result<Expr> {
    expr.transform(&|expr| {
        // first treat expr as unqualified and see if we can find it in the current proj list
        // if found, return a column expression
        if let Some(found_expr) = proj_exprs.iter().find(|item| (**item) == expr) {
            let new_col = Expr::Column(
                found_expr
                    .to_field(input.output_schema().as_ref())
                    .map(|f| f.qualified_column())?,
            );
            return Ok(Transformed::Yes(new_col));
        }
        // if not found, then convert expr to normalized and see
        let normalized_expr = if let Ok(e) = normalize_col(expr.clone(), input) {
            e
        } else {
            return Ok(Transformed::No(expr)); // actually will not reach here since normalize_col always return Ok
        };
        let name = normalized_expr.display_name()?;
        let search_col = Expr::Column(Column {
            relation: None,
            name,
        });
        if let Some(found_expr) = proj_exprs
            .iter()
            .find(|&item| expr_match(&search_col, item))
        {
            let found_expr = found_expr.clone();
            return Ok(Transformed::Yes(found_expr));
        }
        // if still not found, return
        Ok(Transformed::No(expr))
    })
}

fn expr_match(needle: &Expr, haystack: &Expr) -> bool {
    if let Expr::Alias(haystack, _) = &haystack {
        haystack.as_ref() == needle
    } else {
        haystack == needle
    }
}


pub fn normalize_col_with_schemas_and_ambiguity_check(expr: Expr, schemas: &[&[&Schema]], using_columns: &[HashSet<Column>]) -> Result<Expr> {
    expr.transform(&|expr|{
        Ok({
            if let Expr::Column(c) = expr {
                let col = c.normalize_with_schemas_and_ambiguity_check(schemas, using_columns)?;
                Transformed::Yes(Expr::Column(col))
            }else{
                Transformed::No(expr)
            }
        })
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::schema::Field;
    use crate::common::schema::Schema;
    use crate::common::types::DataType;
    use crate::expr::utils::{col, min};
    use crate::storage::empty::EmptyTable;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_input() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(
            vec![
                Field::new("c1", DataType::Int32, true, None),
                Field::new("c2", DataType::Int32, true, None),
                Field::new("c3", DataType::Float64, true, None),
            ],
            HashMap::new(),
        ));
        LogicalPlanBuilder::scan("t", Arc::new(EmptyTable::new(schema)), None).unwrap()
    }

    fn sort(expr: Expr) -> Expr {
        let asc = true;
        let nulls_first = true;
        expr.sort(asc, nulls_first)
    }

    #[test]
    fn test_rewrite_sort_cols_by_agg() {
        let agg = make_input()
            .aggregate(vec![col("c1")], vec![min(col("c2"))])
            .unwrap()
            .project(vec![col("c1"), min(col("c2"))])
            .unwrap()
            .build()
            .unwrap();
        //println!("the initial plan: \n{:?}", agg);
        let init_sort_expr = sort(min(col("c2")));
        let ans_sort_expr = rewrite_sort_cols_by_aggs(vec![init_sort_expr], &agg).unwrap();
        //println!("the result expr: \n{:?}", ans_sort_expr[0]);
        match ans_sort_expr[0].clone() {
            Expr::Sort(Sort { expr, .. }) => {
                assert_eq!("MIN(t.c2)".to_owned(), expr.display_name().unwrap());
            }
            _ => {}
        }
    }
}
