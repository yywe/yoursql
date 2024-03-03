use super::{LogicalPlanner, PlannerContext};
use crate::expr::logical_plan::LogicalPlan;
use anyhow::{anyhow, Result};
use sqlparser::ast::Statement;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let sql = Some(statement.to_string());
        match statement {
            Statement::Query(query) => self.plan_query(*query),
            Statement::CreateTable {
                name,
                columns,
                ..
            } => self.plan_create_table(name, columns),
            _ => Err(anyhow!("Unsupported SQL statement yet: {sql:?}")),
        }
    }
}
