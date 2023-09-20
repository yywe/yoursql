use super::ExecutionPlan;
use crate::expr::expr_rewriter::unnormlize_cols;
use crate::expr::expr_rewriter::unalias;
use crate::expr::logical_plan::TableScan;
use crate::physical_planner::LogicalPlan;
use crate::{physical_planner::DefaultPhysicalPlanner, session::SessionState};
use anyhow::{anyhow, Result};
use crate::expr::expr::Expr;
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
            }else{
                Ok(c.flat_name())
            }
        }
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        _=>Err(anyhow!("todo"))
    }
}