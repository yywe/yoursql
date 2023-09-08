use super::{LogicalPlanner, PlannerContext, object_name_to_table_refernce};
use crate::{expr::logical_plan::LogicalPlan, common::table_reference::OwnedTableReference};
use anyhow::{anyhow, Result};
use sqlparser::ast::{Query, Ident,Select, SetExpr, TableWithJoins, TableFactor, ObjectName, TableAlias};
use crate::expr::logical_plan::builder::LogicalPlanBuilder;
use crate::expr::logical_plan::SubqueryAlias;
use crate::expr::utils::col;

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

    pub fn plan_from_tables(&self, mut from: Vec<TableWithJoins>) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let from = from.remove(0);
                self.plan_table_with_joins(from)
            }
            _=>{
                let mut plans = from.into_iter().map(|t|self.plan_table_with_joins(t));
                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);
                for right in plans {
                    left = left.cross_join(right?)?;
                }
                Ok(left.build()?)
            }
        }
    }

    pub fn plan_table_with_joins(&self, t: TableWithJoins) -> Result<LogicalPlan> {
        return Err(anyhow!(format!("todo")))
    }

    pub fn create_relation(&self, relation: TableFactor) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. }=>{
                let table_ref = self.object_name_to_table_refernce(name)?;
                let table_provider = self.context.get_table_provider(table_ref.clone())?;
                let table_scan = LogicalPlanBuilder::scan(table_ref, table_provider, None)?.build()?;
                (table_scan, alias)
            }
            _=>{
                return Err(anyhow!(format!("unsupported TableFactor type:{relation:?}")))
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        }else{
            Ok(plan)
        }
    }

    pub fn object_name_to_table_refernce(&self, object_name: ObjectName) -> Result<OwnedTableReference> {
        object_name_to_table_refernce(object_name, self.options.enable_ident_normalization)
    }

    pub fn apply_table_alias(&self, plan: LogicalPlan, alias: TableAlias) -> Result<LogicalPlan> {
        let apply_name_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(plan, self.normalizer.normalize(alias.name))?);
        self.apply_expr_alias(apply_name_plan, alias.columns)
    }

    pub fn apply_expr_alias(&self, plan: LogicalPlan, idents: Vec<Ident>) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        }else if idents.len() != plan.output_schema().fields().len() {
            return Err(anyhow!(format!("source table has {} columns, but only {} names given alias", plan.output_schema().fields().len(), idents.len())))
        }else{
            let fields = plan.output_schema().fields().clone();
            LogicalPlanBuilder::from(plan).project(fields.iter().zip(idents.into_iter()).map(|(field, ident)|{
                col(field.name()).alias(self.normalizer.normalize(ident))
            }))?.build()
        }
    }


}


#[cfg(test)]
mod test {

}