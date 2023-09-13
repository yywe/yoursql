use super::{object_name_to_table_refernce, LogicalPlanner, PlannerContext};
use crate::common::column::Column;
use crate::expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use crate::expr::logical_plan::builder::LogicalPlanBuilder;
use crate::expr::logical_plan::Filter;
use crate::expr::logical_plan::JoinType;
use crate::expr::logical_plan::SubqueryAlias;
use crate::expr::utils::col;
use crate::expr::utils::extract_columns_from_expr;
use crate::{common::table_reference::OwnedTableReference, expr::logical_plan::LogicalPlan};
use anyhow::{anyhow, Result};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::ast::{
    Ident, ObjectName, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::ast::{Join, JoinConstraint};
use std::collections::HashSet;
use std::sync::Arc;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn plan_query(&self, query: Query) -> Result<LogicalPlan> {
        //println!("plan query {:#?}", query);
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
            || select.qualify.is_some()
            || select.top.is_some()
            || !select.sort_by.is_empty()
        {
            return Err(anyhow!("not supported select feature"));
        }
        let plan = self.plan_from_tables(select.from)?;
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
        let plan = self.plan_selection(select.selection, plan)?;
        Ok(plan)
    }

    pub fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate) => {
                let fallback_schemas = plan.fallback_normalize_schemas();
                let fallback_schemas = fallback_schemas.iter().collect::<Vec<_>>(); // convert to vec of borrow for later use
                let filter_expr =
                    self.sql_expr_to_logical_expr(predicate, plan.output_schema().as_ref())?;
                // normalize the Column in filter expr
                // however, why here extract all columns as using columns
                let mut using_columns = HashSet::new();
                extract_columns_from_expr(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[plan.output_schema().as_ref()], &fallback_schemas],
                    &[using_columns],
                )?;
                // now create filter plan using normalized filter expr
                Ok(LogicalPlan::Filter(Filter::try_new(
                    filter_expr,
                    Arc::new(plan),
                )?))
            }
            None => Ok(plan),
        }
    }

    /// plan from clause
    pub fn plan_from_tables(&self, mut from: Vec<TableWithJoins>) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let from = from.remove(0);
                self.plan_table_with_joins(from)
            }
            _ => {
                let mut plans = from.into_iter().map(|t| self.plan_table_with_joins(t));
                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);
                for right in plans {
                    left = left.cross_join(right?)?;
                }
                Ok(left.build()?)
            }
        }
    }

    pub fn plan_table_with_joins(&self, t: TableWithJoins) -> Result<LogicalPlan> {
        let left = self.create_relation(t.relation)?;
        match t.joins.len() {
            0 => Ok(left),
            _ => {
                let mut joins = t.joins.into_iter();
                let mut left = self.parse_relation_join(left, joins.next().unwrap())?;
                for join in joins {
                    left = self.parse_relation_join(left, join)?;
                }
                Ok(left)
            }
        }
    }

    pub fn create_relation(&self, relation: TableFactor) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                let table_ref = self.object_name_to_table_refernce(name)?;
                let table_provider = self.context.get_table_provider(table_ref.clone())?;
                let table_scan =
                    LogicalPlanBuilder::scan(table_ref, table_provider, None)?.build()?;
                (table_scan, alias)
            }
            _ => {
                return Err(anyhow!(format!(
                    "unsupported TableFactor type:{relation:?}"
                )))
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    fn parse_relation_join(&self, left: LogicalPlan, join: Join) -> Result<LogicalPlan> {
        let right = self.create_relation(join.relation)?;
        match join.join_operator {
            sqlparser::ast::JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left)
            }
            sqlparser::ast::JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right)
            }
            sqlparser::ast::JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner)
            }
            sqlparser::ast::JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full)
            }
            _ => {
                return Err(anyhow!(
                    "unsupported join operator {:?}",
                    join.join_operator
                ))
            }
        }
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(condition) => {
                let join_schema = left.output_schema().join(right.output_schema().as_ref())?;
                let condition = self.ast_expr_to_expr(condition, &join_schema)?;
                LogicalPlanBuilder::from(left)
                    .join(
                        right,
                        join_type,
                        (Vec::<Column>::new(), Vec::<Column>::new()),
                        Some(condition),
                    )?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<Column> = idents
                    .into_iter()
                    .map(|id| Column {
                        relation: None,
                        name: self.normalizer.normalize(id),
                    })
                    .collect();
                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                let left_schema = left.output_schema();
                let left_cols: HashSet<&String> =
                    left_schema.fields().iter().map(|f| f.name()).collect();
                let keys: Vec<Column> = right
                    .output_schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .filter(|n| left_cols.contains(n))
                    .map(|n| Column {
                        relation: None,
                        name: n.into(),
                    })
                    .collect();
                if keys.is_empty() {
                    LogicalPlanBuilder::from(left).cross_join(right)?.build()
                } else {
                    LogicalPlanBuilder::from(left)
                        .join_using(right, join_type, keys)?
                        .build()
                }
            }
            _ => return Err(anyhow!(format!("join constrait NONE is not supported"))),
        }
    }

    pub fn object_name_to_table_refernce(
        &self,
        object_name: ObjectName,
    ) -> Result<OwnedTableReference> {
        object_name_to_table_refernce(object_name, self.options.enable_ident_normalization)
    }

    pub fn apply_table_alias(&self, plan: LogicalPlan, alias: TableAlias) -> Result<LogicalPlan> {
        let apply_name_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            plan,
            self.normalizer.normalize(alias.name),
        )?);
        self.apply_expr_alias(apply_name_plan, alias.columns)
    }

    pub fn apply_expr_alias(&self, plan: LogicalPlan, idents: Vec<Ident>) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.output_schema().fields().len() {
            return Err(anyhow!(format!(
                "source table has {} columns, but only {} names given alias",
                plan.output_schema().fields().len(),
                idents.len()
            )));
        } else {
            let fields = plan.output_schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(self.normalizer.normalize(ident))
                }))?
                .build()
        }
    }


}

#[cfg(test)]
mod test {
    use super::*;
    use crate::parser::parse;
    use crate::session::test::init_mem_testdb;
    use crate::session::SessionContext;
    #[tokio::test]
    async fn test_plan_select() -> Result<()> {
        let mut session = SessionContext::default();
        init_mem_testdb(&mut session)?;
        let sql = "SELECT A.id, A.name, B.score from testdb.student A inner join testdb.enroll B on A.id=B.student_id inner join testdb.course C on A.id=C.id where B.score > 99 order by B.score";
        let statement = parse(sql).unwrap();
        let _plan = session.state.read().make_logical_plan(statement).await?;
        //println!("{:?}", _plan);
        Ok(())
    }
}
