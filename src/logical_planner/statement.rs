use super::{LogicalPlanner, PlannerContext};
use crate::{expr::logical_plan::LogicalPlan, logical_planner::object_name_to_table_refernce};
use anyhow::{anyhow, Result};
use sqlparser::ast::Statement;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let sql = Some(statement.to_string());
        match statement {
            Statement::Query(query) => self.plan_query(*query),
            Statement::CreateTable { name, columns, .. } => self.plan_create_table(name, columns),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table_reference = object_name_to_table_refernce(table_name, true)?;
                let table_provider = self.context.get_table_provider(table_reference.clone())?;
                self.plan_insert(
                    table_reference,
                    table_provider.get_table(),
                    columns,
                    *source,
                )
            }
            _ => Err(anyhow!("Unsupported SQL statement yet: {sql:?}")),
        }
    }
}
