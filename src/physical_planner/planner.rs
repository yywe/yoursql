use super::nested_loop_join::NestedLoopJoinExec;
use super::projection::ProjectionExec;
use super::ExecutionPlan;
use super::PhysicalPlanner;
use crate::expr::expr::Expr;
use crate::expr::expr_rewriter::unalias;
use crate::expr::expr_rewriter::unnormlize_cols;
use crate::expr::logical_plan::TableScan;
use crate::expr::logical_plan::builder::wrap_projection_for_join;
use crate::expr::logical_plan::{CrossJoin, Projection, Join};
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
                LogicalPlan::Join(Join{
                    left,
                    right,
                    on: keys,
                    filter,
                    join_type,
                    null_equals_null,
                    schema: join_schema,
                    ..
                })=>{
                    let _null_equals_null = *null_equals_null; //not used yet, used for hash join and sort merge
                    
                    // check if there is expr equal join like where 3*columnA=2*columnB
                    // i.e, any pair does not satisfy Expr::Column
                    let has_expr_equal_join = keys.iter().any(|(l, r)|{
                        !(matches!(l, Expr::Column(_)) && matches!(r, Expr::Column(_)))
                    });
                    // if has expr equal join, we do a projection, like project 3 * column A to "3*columnA"
                    // as a new column
                    if has_expr_equal_join {
                        // get all left and right join keys in sep vector
                        let left_keys = keys.iter().map(|(l, _r)|l).cloned().collect::<Vec<_>>();
                        let right_keys = keys.iter().map(|(_l, r)|r).cloned().collect::<Vec<_>>();
                        let (left, right, column_on, added_project) = {
                            let (left, left_col_keys, left_projected) = wrap_projection_for_join(left_keys.as_slice(), left.as_ref().clone())?;
                            let (right, right_col_keys, right_projected) = wrap_projection_for_join(&right_keys, right.as_ref().clone())?;
                            (left, right, (left_col_keys, right_col_keys), left_projected || right_projected)
                        };
                        let join_plan = LogicalPlan::Join(Join::try_new_with_project_input(logical_plan, Arc::new(left), Arc::new(right), column_on)?);
                        // if we have projection, after the join with projection, we need to remove it
                        let join_plan = if added_project {
                            //extract original columns from original join_schema
                            let final_join_result = join_schema.fields().iter().map(|f|Expr::Column(f.qualified_column())).collect::<Vec<_>>();
                            let projection = Projection::try_new_with_schema(final_join_result, Arc::new(join_plan), join_schema.clone())?;
                            LogicalPlan::Projection(projection)
                        }else{
                            join_plan
                        };
                        return self.create_initial_plan(&join_plan, session_state).await;
                    }
                    //all equivilent join are columns now
                    let physical_left = self.create_initial_plan(&left, session_state).await?;
                    let physical_right = self.create_initial_plan(&right, session_state).await?;
                    let join_filter = match filter {
                        Some(expr)=>{
                            Some(self.create_physical_expr(expr, join_schema, session_state)?)
                        }
                        _=>None,
                    };
                    Ok(Arc::new(NestedLoopJoinExec::try_new(physical_left, physical_right, join_filter, join_type)?))
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
