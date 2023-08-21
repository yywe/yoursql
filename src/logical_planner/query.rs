use super::{LogicalPlanner,PlannerContext};
use sqlparser::ast::Query;
use anyhow::{Result,anyhow};
use crate::expr::logical_plan::LogicalPlan;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn plan_query(query: Query) -> Result<LogicalPlan> {
        return Err(anyhow::anyhow!("unsupported statement"));
    }
}