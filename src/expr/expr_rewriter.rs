use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::expr::{AggregateFunction, Between, BinaryExpr, Like, Sort};
use super::expr_schema::ExprToSchema;
use super::logical_plan::builder::LogicalPlanBuilder;
use crate::common::column::Column;
use crate::common::schema::Schema;
use crate::common::tree_node::{Transformed, TreeNode};
use crate::expr::expr::Expr;
use crate::expr::logical_plan::LogicalPlan;

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

/// rewrite the sort expression that has aggregation to use existing aggregation output column if
/// any e.g, select a, max(b) from t group by a order by  max(b)
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
            return Ok(Transformed::No(expr)); // actually will not reach here since normalize_col
                                              // always return Ok
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

pub fn normalize_col_with_schemas_and_ambiguity_check(
    expr: Expr,
    schemas: &[&[&Schema]],
    using_columns: &[HashSet<Column>],
) -> Result<Expr> {
    expr.transform(&|expr| {
        Ok({
            if let Expr::Column(c) = expr {
                let col = c.normalize_with_schemas_and_ambiguity_check(schemas, using_columns)?;
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(expr)
            }
        })
    })
}

/// clone the expr, but rewrite it if condition satsify, i.e, if replacement_fn return some expr
/// recursive implementation
pub fn clone_with_replacement<F>(expr: &Expr, replacement_fn: &F) -> Result<Expr>
where
    F: Fn(&Expr) -> Result<Option<Expr>>,
{
    let replacement_opt = replacement_fn(expr)?;
    match replacement_opt {
        // recursion break case, where the replacement_fn already replaced. use the replacement
        Some(replacement) => Ok(replacement),
        // not replaced yet, look nested expr and recursion
        None => match expr {
            Expr::AggregateFunction(AggregateFunction {
                fun,
                args,
                distinct,
                filter,
                order_by,
            }) => Ok(Expr::AggregateFunction(AggregateFunction {
                fun: fun.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expr>>>()?,
                distinct: *distinct,
                filter: filter.clone(),
                order_by: order_by.clone(),
            })),
            Expr::Alias(e, alias_name) => Ok(Expr::Alias(
                Box::new(clone_with_replacement(e, replacement_fn)?),
                alias_name.clone(),
            )),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Ok(Expr::Between(Between {
                expr: Box::new(clone_with_replacement(expr, replacement_fn)?),
                negated: *negated,
                low: Box::new(clone_with_replacement(low, replacement_fn)?),
                high: Box::new(clone_with_replacement(high, replacement_fn)?),
            })),
            Expr::BinaryExpr(BinaryExpr { left, right, op }) => Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(clone_with_replacement(left, replacement_fn)?),
                op: *op,
                right: Box::new(clone_with_replacement(right, replacement_fn)?),
            })),
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Ok(Expr::Like(Like {
                negated: *negated,
                expr: Box::new(clone_with_replacement(expr, replacement_fn)?),
                pattern: Box::new(clone_with_replacement(pattern, replacement_fn)?),
                escape_char: *escape_char,
            })),
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Ok(Expr::ILike(Like {
                negated: *negated,
                expr: Box::new(clone_with_replacement(expr, replacement_fn)?),
                pattern: Box::new(clone_with_replacement(pattern, replacement_fn)?),
                escape_char: *escape_char,
            })),
            Expr::Not(e) => Ok(Expr::Not(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsTrue(e) => Ok(Expr::IsTrue(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsNotTrue(e) => Ok(Expr::IsNotTrue(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsFalse(e) => Ok(Expr::IsFalse(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::IsNotFalse(e) => Ok(Expr::IsNotFalse(Box::new(clone_with_replacement(
                e,
                replacement_fn,
            )?))),
            Expr::Sort(Sort {
                expr: nested_expr,
                asc,
                nulls_first,
            }) => Ok(Expr::Sort(Sort {
                expr: Box::new(clone_with_replacement(nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first,
            })),
            Expr::Column { .. } | Expr::Literal(_) => Ok(expr.clone()),
            Expr::Wildcard => Ok(Expr::Wildcard),
            Expr::QualifiedWildcard { .. } => Ok(expr.clone()),
        },
    }
}

/// rebuild an Expr with columns that refer to aliases replace by underlying expr
pub fn resolve_alias_to_exprs(expr: &Expr, aliases: &HashMap<String, Expr>) -> Result<Expr> {
    clone_with_replacement(expr, &|e| match e {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Some(aliased_expr.clone()))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    })
}

pub fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    clone_with_replacement(expr, &|e| match e {
        Expr::Column(col) => {
            let s = plan.output_schema();
            let field = s.field_from_column(col)?;
            Ok(Some(Expr::Column(field.qualified_column())))
        }
        _ => Ok(None),
    })
}

pub fn unnormalize_col(expr: Expr) -> Expr {
    expr.transform(&|e| {
        Ok({
            if let Expr::Column(c) = e {
                let col = Column {
                    relation: None,
                    name: c.name,
                };
                Transformed::Yes(Expr::Column(col))
            } else {
                Transformed::No(e)
            }
        })
    })
    .expect("unnormalize is infallable")
}

pub fn unnormlize_cols(exprs: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
    exprs.into_iter().map(unnormalize_col).collect()
}

pub fn unalias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(sub_expr, _) => unalias(*sub_expr),
        _ => expr,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::common::schema::{Field, Schema};
    use crate::common::types::DataType;
    use crate::expr::utils::{col, min};
    use crate::storage::empty::EmptyTable;

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
        // println!("the initial plan: \n{:?}", agg);
        let init_sort_expr = sort(min(col("c2")));
        let ans_sort_expr = rewrite_sort_cols_by_aggs(vec![init_sort_expr], &agg).unwrap();
        // println!("the result expr: \n{:?}", ans_sort_expr[0]);
        match ans_sort_expr[0].clone() {
            Expr::Sort(Sort { expr, .. }) => {
                assert_eq!("MIN(t.c2)".to_owned(), expr.display_name().unwrap());
            }
            _ => {}
        }
    }
}
