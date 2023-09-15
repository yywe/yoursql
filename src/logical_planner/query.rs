use super::{object_name_to_table_refernce, LogicalPlanner, PlannerContext};
use crate::common::column::Column;
use crate::common::schema::Schema;
use crate::common::utils::extract_aliases;
use crate::common::table_reference::TableReference;
use crate::expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use crate::expr::logical_plan::builder::LogicalPlanBuilder;
use crate::expr::logical_plan::Filter;
use crate::expr::expr::Expr;
use crate::expr::logical_plan::JoinType;
use crate::expr::logical_plan::SubqueryAlias;
use crate::expr::utils::col;
use crate::expr::utils::extract_columns_from_expr;
use crate::expr::utils::find_column_exprs;
use crate::expr::expr_rewriter::resolve_alias_to_exprs;
use crate::expr::expr_rewriter::normalize_col;

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
        // first get data, i.e, table scan
        let plan = self.plan_from_tables(select.from)?;
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));

        // next do filter
        let plan = self.plan_selection(select.selection, plan)?;

        // now get the original select expressions (projection, may contain aggregation expressions)
        let select_exprs = self.select_items_to_exprs(&plan, select.projection, empty_from)?;

        // next do a projection, note this is just a temp projection, in order to extract the output schema, which may be
        // used in having or group by, the combined schema will have all the exprs, including alias
        let projected_plan = self.project(plan.clone(), select_exprs.clone())?;
        let mut combined_schema = (*projected_plan.output_schema()).clone();
        combined_schema=combined_schema.merge(plan.output_schema().as_ref())?;

        // extract alias map, later used to convert alias back to expr in having expression
        let alias_map  = extract_aliases(&select_exprs);
        
        // next we convert the having from AST expr to logical expr
        let having_expr = select.having.map(|expr|{
            // first we use combined schema as the having may have alias from projection
            // after this step, the expr may refer alias
            let expr = self.sql_expr_to_logical_expr(expr, &combined_schema)?;
            // now we rewrite the expr by convert alias back to original expr based on alias map
            let expr = resolve_alias_to_exprs(&expr, &alias_map)?;
            // lastly, normlize the expression based on projected plan
            normalize_col(expr, &projected_plan)
        }).transpose()?;


        // next should do aggegration
        // when do 



        // lastly do projection
        
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

    /// convert the select items into a vec of exprs
    fn select_items_to_exprs(&self, plan: &LogicalPlan, projection: Vec<SelectItem>, empty_from: bool) -> Result<Vec<Expr>> {
        projection.into_iter().map(|select_item|self.select_item_to_expr(select_item, plan, empty_from)).flat_map(|result|{
            match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)]
            }
        }).collect::<Result<Vec<Expr>>>()
    }

    /// convert a single select item into expr(s), include wildcard expansion
    fn select_item_to_expr(&self,  select_item: SelectItem, plan: &LogicalPlan, empty_from: bool) -> Result<Vec<Expr>> {
        let schema = plan.output_schema();
        match select_item {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_expr_to_logical_expr(expr, plan.output_schema().as_ref())?;
                let col = normalize_col_with_schemas_and_ambiguity_check(expr, &[&[plan.output_schema().as_ref()]], &plan.using_columns()?)?;
                Ok(vec![col])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.sql_expr_to_logical_expr(expr, plan.output_schema().as_ref())?;
                let col = normalize_col_with_schemas_and_ambiguity_check(expr, &[&[plan.output_schema().as_ref()]], &plan.using_columns()?)?;
                let expr = Expr::Alias(Box::new(col), self.normalizer.normalize(alias));
                Ok(vec![expr])
            }
            // wildcard expansion, here options of wildcard will not be supported
            SelectItem::Wildcard(_options) => {
                if empty_from {
                    return Err(anyhow!("select * with no tables specified is not valid"))
                }
                let using_columns = plan.using_columns()?;
                let columns_to_skip = using_columns.into_iter().flat_map(|cols|{
                    let mut cols = cols.into_iter().collect::<Vec<_>>();
                    cols.sort();
                    let mut out_column_names: HashSet<String> = HashSet::new();
                    cols.into_iter().filter_map(|c|{
                        if out_column_names.contains(&c.name) {
                            Some(c)
                        }else{
                            out_column_names.insert(c.name);
                            None
                        }
                    }).collect::<Vec<_>>()
                }).collect::<HashSet<_>>();
                
                let exprs = if columns_to_skip.is_empty() {
                    schema.fields().iter().map(|f|Expr::Column(f.qualified_column())).collect::<Vec<Expr>>()
                }else{
                    schema.fields().iter().filter_map(|f|{
                        let col = f.qualified_column();
                        if !columns_to_skip.contains(&col) {
                            Some(Expr::Column(col))
                        }else{
                            None
                        }
                    }).collect::<Vec<Expr>>()
                };
                Ok(exprs)
            }
            SelectItem::QualifiedWildcard(ref object_name, _options)=>{
                let qualifier = format!("{object_name}");
                let qualifier = TableReference::from(qualifier);
                let qualified_fields = schema.fields_with_qualifed(&qualifier).into_iter().cloned().collect::<Vec<_>>();
                if qualified_fields.is_empty() {
                    return Err(anyhow!(format!("Invalid qualifier {qualifier}")))
                }
                let qualified_schema = Schema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
                Ok(qualified_schema.fields().iter().map(|f|Expr::Column(f.qualified_column())).collect::<Vec<Expr>>())
            }
        }
    }

    fn project(&self, input: LogicalPlan, exprs: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.output_schema().as_ref(), &exprs)?;
        LogicalPlanBuilder::from(input).project(exprs)?.build()
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
