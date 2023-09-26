use super::projection::ProjectionExec;
use super::ExecutionPlan;
use super::PhysicalPlanner;
use crate::expr::expr::Expr;
use crate::expr::expr_rewriter::unalias;
use crate::expr::expr_rewriter::unnormlize_cols;
use crate::expr::logical_plan::TableScan;
use crate::expr::logical_plan::{CrossJoin, Projection};
use crate::physical_planner::cross_join::CrossJoinExec;
use crate::physical_planner::filter::FilterExec;
use crate::physical_planner::LogicalPlan;
use crate::{physical_planner::DefaultPhysicalPlanner, session::SessionState};
use anyhow::Context;
use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

impl DefaultPhysicalPlanner {
    /// this ia a async func without async fn. cause it returns box future
    pub fn create_initial_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
                LogicalPlan::TableScan(TableScan {
                    table_name,
                    source,
                    projection,
                    projected_schema,
                    filters,
                    fetch,
                }) => {
                    let filters = unnormlize_cols(filters.iter().cloned());
                    let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source
                        .scan(session_state, projection.as_ref(), &unaliased)
                        .await
                }
                LogicalPlan::Projection(Projection { input, exprs, .. }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let input_schema = input.as_ref().output_schema();
                    let physical_exprs = exprs
                        .iter()
                        .map(|e| {
                            let physical_name = if let Expr::Column(col) = e {
                                let index_of_column = input_schema
                                    .index_of_column_by_name(col.relation.as_ref(), &col.name)?
                                    .context(format!("failed to find column {:?}", col));
                                match index_of_column {
                                    Ok(idx) => {
                                        Ok(input_exec.schema().field(idx).name().to_string())
                                    }
                                    Err(_) => physical_name(e),
                                }
                            } else {
                                physical_name(e)
                            };
                            tuple_err((
                                self.create_physical_expr(e, input_schema.as_ref(), session_state),
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
                    let input_schema = filter.input.output_schema();
                    let physical_filter_expr =
                        self.create_physical_expr(&filter.predicate, &input_schema, session_state)?;
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
        _ => Err(anyhow!("todo")),
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
