use super::{LogicalPlanner,PlannerContext};
use sqlparser::ast::Query;
use anyhow::{Result,anyhow};
use crate::expr::logical_plan::LogicalPlan;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn plan_query(&self, query: Query) -> Result<LogicalPlan> {
        //println!("plan query {:?}", query);
        return Err(anyhow!("unsupported statement"));
    }
}