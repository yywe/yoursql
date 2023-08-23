use super::{LogicalPlanner, PlannerContext};
use crate::expr::logical_plan::LogicalPlan;
use anyhow::{anyhow, Result};
use sqlparser::ast::{Query, Select, SetExpr, TableWithJoins};

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn plan_query(&self, query: Query) -> Result<LogicalPlan> {
        println!("plan query {:#?}", query);
        let set_expr = query.body;
        let plan = self.plan_set_expr(*set_expr)?;
        //todo: add order_by and limit plan
        Ok(plan)
    }

    pub fn plan_set_expr(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.plan_select(*s),
            _ => Err(anyhow!(format!("set expr {set_expr} not supported yet"))),
        }
    }

    pub fn plan_select(&self, select: Select) -> Result<LogicalPlan> {
        if !select.cluster_by.is_empty()
            || !select.lateral_views.is_empty()
            || !select.qualify.is_some()
            || !select.top.is_some()
            || !select.sort_by.is_empty()
        {
            return Err(anyhow!("not supported select feature"));
        }
        let plan = self.plan_from_tables(select.from)?;
        return Err(anyhow!("not supported select feature"));
    }

    pub fn plan_from_tables(&self, from: Vec<TableWithJoins>) -> Result<LogicalPlan> {
        return Err(anyhow!("not supported select feature"));
    }
}
