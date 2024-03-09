use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures::future::BoxFuture;
use futures::FutureExt;

use super::aggregate::PhysicalGroupBy;
use super::limit::LimitExec;
use super::nested_loop_join::NestedLoopJoinExec;
use super::projection::ProjectionExec;
use super::values::ValuesExec;
use super::{ExecutionPlan, PhysicalPlanner};
use crate::common::schema::Schema;
use crate::expr::expr;
use crate::expr::expr::{AggregateFunction, Between, BinaryExpr, Expr, Like};
use crate::expr::expr_rewriter::{unalias, unnormlize_cols};
use crate::expr::logical_plan::builder::wrap_projection_for_join;
use crate::expr::logical_plan::{
    Aggregate, CreateTable, CrossJoin, Insert, Join, Limit, LogicalPlan, Projection, Sort,
    TableScan, Values,
};
use crate::physical_expr::aggregate::{create_aggregate_expr_impl, AggregateExpr};
use crate::physical_expr::planner::create_physical_expr;
use crate::physical_expr::sort::PhysicalSortExpr;
use crate::physical_expr::PhysicalExpr;
use crate::physical_planner::aggregate::AggregateExec;
use crate::physical_planner::create_table::CreateTableExec;
use crate::physical_planner::cross_join::CrossJoinExec;
use crate::physical_planner::filter::FilterExec;
use crate::physical_planner::insert::InsertExec;
use crate::physical_planner::sort::SortExec;
use crate::physical_planner::DefaultPhysicalPlanner;
use crate::session::SessionState;

impl DefaultPhysicalPlanner {
    /// this ia a async func without async fn. cause it returns box future
    /// Note: Logical Schema is the schema in logical Plan, in datafusion, it is DFSchema
    /// Logical Schema Field include the Optional Table Reference 
    /// Physical Schema is the schema (output) of physical Plan, it is Schema
    /// Physical Schema do not have the Table Reference, always None
    pub fn create_initial_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
                LogicalPlan::TableScan(TableScan {
                    table_name: _,
                    source,
                    projection,
                    projected_schema: _,
                    filters,
                    fetch: _,
                }) => {
                    let filters = unnormlize_cols(filters.iter().cloned());
                    let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source
                        .scan(session_state, projection.as_ref(), &unaliased)
                        .await
                }
                LogicalPlan::Projection(Projection { input, exprs, .. }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let input_logischema = input.as_ref().output_schema();
                    let physical_exprs = exprs
                        .iter()
                        .map(|e| {
                            let physical_name = if let Expr::Column(col) = e {
                                let index_of_column = input_logischema
                                    .index_of_column_by_name(col.relation.as_ref(), &col.name)?
                                    .context(format!("failed to find column {:?}", col));
                                match index_of_column {
                                    Ok(idx) => {
                                        // here index physical schema field using logical field
                                        // index
                                        Ok(input_exec.schema().field(idx).name().to_string())
                                    }
                                    Err(_) => physical_name(e),
                                }
                            } else {
                                physical_name(e)
                            };
                            tuple_err((
                                self.create_physical_expr(
                                    e,
                                    input_logischema.as_ref(),
                                    session_state,
                                ),
                                physical_name,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(ProjectionExec::try_new(
                        physical_exprs,
                        input_exec,
                    )?))
                }
                LogicalPlan::Filter(filter) => {
                    let physical_input = self
                        .create_initial_plan(&filter.input, session_state)
                        .await?;
                    let input_logischema = filter.input.output_schema();
                    let physical_filter_expr = self.create_physical_expr(
                        &filter.predicate,
                        &input_logischema,
                        session_state,
                    )?;
                    Ok(Arc::new(FilterExec::try_new(
                        physical_filter_expr,
                        physical_input,
                    )?))
                }
                LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                    let left_plan = self.create_initial_plan(left, session_state).await?;
                    let right_plan = self.create_initial_plan(right, session_state).await?;
                    Ok(Arc::new(CrossJoinExec::new(left_plan, right_plan)?))
                }
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: keys,
                    filter,
                    join_type,
                    null_equals_null,
                    schema: join_logischema,
                    ..
                }) => {
                    let _null_equals_null = *null_equals_null; // not used yet, used for hash join and sort merge

                    // check if there is expr equal join like where 3*columnA=2*columnB
                    // i.e, any pair does not satisfy Expr::Column
                    let has_expr_equal_join = keys.iter().any(|(l, r)| {
                        !(matches!(l, Expr::Column(_)) && matches!(r, Expr::Column(_)))
                    });
                    // if has expr equal join, we do a projection, like project 3 * column A to
                    // "3*columnA" as a new column
                    if has_expr_equal_join {
                        // get all left and right join keys in sep vector
                        let left_keys = keys.iter().map(|(l, _r)| l).cloned().collect::<Vec<_>>();
                        let right_keys = keys.iter().map(|(_l, r)| r).cloned().collect::<Vec<_>>();
                        let (left, right, column_on, added_project) = {
                            let (left, left_col_keys, left_projected) = wrap_projection_for_join(
                                left_keys.as_slice(),
                                left.as_ref().clone(),
                            )?;
                            let (right, right_col_keys, right_projected) =
                                wrap_projection_for_join(&right_keys, right.as_ref().clone())?;
                            (
                                left,
                                right,
                                (left_col_keys, right_col_keys),
                                left_projected || right_projected,
                            )
                        };
                        let join_plan = LogicalPlan::Join(Join::try_new_with_project_input(
                            logical_plan,
                            Arc::new(left),
                            Arc::new(right),
                            column_on,
                        )?);
                        // if we have projection, after the join with projection, we need to remove
                        // it
                        let join_plan = if added_project {
                            // extract original columns from original join_schema
                            let final_join_result = join_logischema
                                .fields()
                                .iter()
                                .map(|f| Expr::Column(f.qualified_column()))
                                .collect::<Vec<_>>();
                            let projection = Projection::try_new_with_schema(
                                final_join_result,
                                Arc::new(join_plan),
                                join_logischema.clone(),
                            )?;
                            LogicalPlan::Projection(projection)
                        } else {
                            join_plan
                        };
                        return self.create_initial_plan(&join_plan, session_state).await;
                    }
                    // all equivilent join are columns now
                    let physical_left = self.create_initial_plan(&left, session_state).await?;
                    let physical_right = self.create_initial_plan(&right, session_state).await?;

                    let join_filter = match filter {
                        Some(expr) => Some(self.create_physical_expr(
                            expr,
                            join_logischema,
                            session_state,
                        )?),
                        _ => None,
                    };
                    Ok(Arc::new(NestedLoopJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_filter,
                        join_type,
                    )?))
                }
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    ..
                }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let input_physchema = input_exec.schema();
                    let input_logischema = input.output_schema();
                    let g_expr = group_expr
                        .iter()
                        .map(|e| {
                            tuple_err((
                                self.create_physical_expr(
                                    e,
                                    input_logischema.as_ref(),
                                    session_state,
                                ),
                                physical_name(e),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let physical_group_by = PhysicalGroupBy::new(g_expr);
                    let physical_agg_exprs = aggr_expr
                        .iter()
                        .map(|e| {
                            create_aggregate_expr(
                                e,
                                input_physchema.as_ref(),
                                input_logischema.as_ref(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(AggregateExec::try_new(
                        physical_group_by,
                        physical_agg_exprs,
                        input_exec,
                    )?))
                }
                LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_logischema = input.output_schema();
                    let sort_expr = expr
                        .iter()
                        .map(|e| {
                            create_physical_sort_expr(
                                e,
                                input_logischema.as_ref(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let new_sort = SortExec::new(sort_expr, physical_input, *fetch);
                    Ok(Arc::new(new_sort))
                }
                LogicalPlan::Limit(Limit { skip, fetch, input }) => {
                    let input = self.create_initial_plan(input, session_state).await?;
                    Ok(Arc::new(LimitExec::new(input, *skip, *fetch)))
                }
                LogicalPlan::CreateTable(CreateTable { name, fields }) => {
                    let resolved_reference = name
                        .clone()
                        .resolve(&session_state.config().catalog.default_database);
                    let dbname = resolved_reference.database;
                    let tblname = resolved_reference.table;
                    match session_state.catalogs().database(&dbname) {
                        Some(db) => {
                            if db.table_exist(&tblname) {
                                return Err(anyhow!(format!("table {} already exist", tblname)));
                            }
                            Ok(Arc::new(CreateTableExec::new(
                                dbname.to_string(),
                                tblname.to_string(),
                                fields.clone(),
                            )))
                        }
                        None => Err(anyhow!(format!("database {} does not exist", dbname))),
                    }
                }
                LogicalPlan::Values(Values { schema, values }) => {
                    let exprs = values
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|expr| {
                                    // TODO: confirm: for values, it does not have child input
                                    // physical schema
                                    // let input_logischema = input.output_schema();
                                    // NOTE: the values schema is empty schema??

                                    self.create_physical_expr(expr, &schema, session_state)
                                })
                                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let value_exec = ValuesExec::try_new(schema.clone(), exprs)?;
                    Ok(Arc::new(value_exec))
                }
                LogicalPlan::Insert(Insert {
                    table_name,
                    table_schema,
                    projected_schema,
                    input,
                }) => {
                    let resolved_reference = table_name
                        .clone()
                        .resolve(&session_state.config().catalog.default_database);
                    let dbname = resolved_reference.database;
                    let tblname = resolved_reference.table;
                    match session_state.catalogs().database(&dbname) {
                        Some(db) => {
                            if !db.table_exist(&tblname) {
                                return Err(anyhow!(format!("table {} does not exist", tblname)));
                            }
                            let input = self.create_initial_plan(input, session_state).await?;
                            let table = db
                                .get_table(&tblname)
                                .await
                                .context("failed to get table")?;
                            Ok(Arc::new(InsertExec::new(
                                input,
                                table_schema.clone(),
                                projected_schema.clone(),
                                table,
                            )))
                        }
                        None => Err(anyhow!(format!("database {} does not exist", dbname))),
                    }
                }
                other => Err(anyhow!(format!(
                    "unsupported logical plan {:?} to physical plan yet",
                    other
                ))),
            };
            exec_plan
        }
        .boxed()
    }
}

fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

/// given the logical expression, create a name for its physical expression
fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            ..
        }) => create_function_physical_name(&fun.to_string(), *distinct, args),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} and {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} and {high}"))
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT LIKE {pattern}{escape}"))
            } else {
                Ok(format!("{expr} LIKE {pattern}{escape}"))
            }
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT ILIKE {pattern}{escape}"))
            } else {
                Ok(format!("{expr} ILIKE {pattern}{escape}"))
            }
        }
        Expr::Sort { .. } => Err(anyhow!(
            "create physical name does not support sort expression"
        )),
        Expr::Wildcard => Err(anyhow!("create physical name does not support wildcard")),
        Expr::QualifiedWildcard { .. } => Err(anyhow!(
            "create physical name does not support qualified wildcard"
        )),
    }
}

/// handy function to combine result
fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(v1), Ok(v2)) => Ok((v1, v2)),
        (Err(e1), Ok(_)) => Err(e1),
        (Ok(_), Err(e2)) => Err(e2),
        (Err(e1), Err(_)) => Err(e1),
    }
}

fn create_function_physical_name(fun: &str, distinct: bool, args: &[Expr]) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_physical_name(e, false))
        .collect::<Result<Vec<_>>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

fn create_aggregate_expr(
    e: &Expr,
    input_schema: &Schema,
    input_logischema: &Schema,
) -> Result<Arc<dyn AggregateExpr>> {
    let (name, e) = match e {
        Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
        _ => (physical_name(e)?, e),
    };
    match e {
        Expr::AggregateFunction(AggregateFunction {
            fun,
            args,
            distinct,
            filter,
            order_by,
        }) => {
            if *distinct != false || filter.is_some() || order_by.is_some() {
                // todo
                return Err(anyhow!(format!(
                    "unsupported input for aggregate function:{fun:?}"
                )));
            }
            let args = args
                .iter()
                .map(|expr| create_physical_expr(expr, input_logischema))
                .collect::<Result<Vec<_>>>()?;
            let agg_expr = create_aggregate_expr_impl(fun, *distinct, &args, input_schema, name)?;
            Ok(agg_expr)
        }
        other => Err(anyhow!(
            format!("unsupported aggregate function:{other:?}",)
        )),
    }
}

pub fn create_physical_sort_expr(
    e: &Expr,
    input_logischema: &Schema,
) -> Result<PhysicalSortExpr> {
    if let Expr::Sort(expr::Sort {
        expr,
        asc,
        nulls_first,
    }) = e
    {
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(expr, input_logischema)?,
            descending: !(*asc),
            nulls_first: *nulls_first,
        })
    } else {
        Err(anyhow!("Expect sort expression"))
    }
}
