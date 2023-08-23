use super::{LogicalPlanner,PlannerContext};
use sqlparser::ast::Statement;
use anyhow::{Result,anyhow};
use crate::expr::logical_plan::LogicalPlan;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let sql = Some(statement.to_string());
        match statement {
            Statement::Query(query) => self.plan_query(*query),
            _=> Err(anyhow!("Unsupported SQL statement yet: {sql:?}"))
        }
    }
}