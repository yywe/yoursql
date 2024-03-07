use super::{object_name_to_table_refernce, LogicalPlanner, PlannerContext};
use crate::common::column::Column;
use crate::common::schema::Field;
use crate::common::schema::Schema;
use crate::common::table_reference::TableReference;
use crate::common::utils::extract_aliases;
use crate::expr::expr::Expr;
use crate::expr::expr::Sort;
use crate::expr::expr_rewriter::normalize_col;
use crate::expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use crate::expr::expr_rewriter::resolve_alias_to_exprs;
use crate::expr::expr_rewriter::resolve_columns;
use crate::expr::logical_plan::builder::project;
use crate::expr::logical_plan::builder::LogicalPlanBuilder;
use crate::expr::logical_plan::CreateTable;
use crate::expr::logical_plan::Filter;
use crate::expr::logical_plan::JoinType;
use crate::expr::logical_plan::SubqueryAlias;
use crate::expr::logical_plan::Values;
use crate::expr::utils::col;
use crate::expr::utils::extract_columns_from_expr;
use crate::expr::utils::find_aggregate_exprs;
use crate::logical_planner::utils::check_columns_satisfy_exprs;
use crate::logical_planner::utils::expr_as_column_expr;
use crate::logical_planner::utils::rebase_expr;
use crate::{common::table_reference::OwnedTableReference, expr::logical_plan::LogicalPlan};
use anyhow::{anyhow, Result};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::ast::Offset as SQLOffset;
use sqlparser::ast::{ColumnDef, OrderByExpr};
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
        let plan = self.order_by(plan, query.order_by)?;
        let plan = self.limit(plan, query.offset, query.limit)?;
        Ok(plan)
    }

    pub fn plan_create_table(
        &self,
        table_name: ObjectName,
        columns: Vec<ColumnDef>,
    ) -> Result<LogicalPlan> {
        let table_reference = object_name_to_table_refernce(table_name, true)?;
        let fields = columns
            .into_iter()
            .map(|cdf| {
                let not_null = cdf
                    .options
                    .iter()
                    .find(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull))
                    .is_some();
                Ok(Field::new(
                    cdf.name.value.clone(),
                    (&cdf.data_type).try_into()?,
                    !not_null,
                    None,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(LogicalPlan::CreateTable(CreateTable {
            name: table_reference,
            fields: fields,
        }))
    }

    pub fn plan_set_expr(&self, set_expr: SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.plan_select(*s),
            SetExpr::Values(values) => self.plan_values(values),
            _ => Err(anyhow!(format!("set expr {set_expr} not supported yet"))),
        }
    }

    ///here simply convert to ast::Expr to logical Expr based on empty schema
    /// the datafusion impl also infer schema from values, refer LogicalPlanBuilder::values
    /// here in plan_insert, we will project the schema based on columns_ident
    /// TODO: remove to mutation
    pub fn plan_values(&self, values: sqlparser::ast::Values) -> Result<LogicalPlan> {
        let schema = Schema::empty();
        let values = values
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| self.sql_expr_to_logical_expr(v, &schema))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        //values should not be based on any other schema, thus just empty
        Ok(LogicalPlan::Values(Values {
            schema: Arc::new(schema),
            values: values,
        }))
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
        combined_schema = combined_schema.merge(plan.output_schema().as_ref())?;

        // extract alias map, later used to convert alias back to expr in having expression
        let alias_map = extract_aliases(&select_exprs);

        // next we convert the having from AST expr to logical expr
        let having_expr = select
            .having
            .map(|expr| {
                // first we use combined schema as the having may have alias from projection
                // after this step, the expr may refer alias
                let expr = self.sql_expr_to_logical_expr(expr, &combined_schema)?;
                // now we rewrite the expr by convert alias back to original expr based on alias map
                let expr = resolve_alias_to_exprs(&expr, &alias_map)?;
                // lastly, normlize the expression based on projected plan
                normalize_col(expr, &projected_plan)
            })
            .transpose()?;

        // next we search/extract aggregate expressions (note just extract sub-expr of aggregation)
        // they may imbeded/included in select_exprs or having exprs
        let mut aggr_expr_haystack = select_exprs.clone();
        if let Some(he) = &having_expr {
            aggr_expr_haystack.push(he.clone());
        }
        // now we extracted the aggregation expressions, they maybe sub-expressions of select items
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // next we convert group by AST expressions to logical expressions
        let group_by_exprs = select
            .group_by
            .into_iter()
            .map(|e| {
                // note the group by may have alias, thus here use combined schema
                let group_by_expr = self.sql_expr_to_logical_expr(e, &combined_schema)?;
                // alias from the projection may conflict with same name exprs in the input schema
                // remove it first
                let mut alias_map = alias_map.clone();
                for f in plan.output_schema().all_fields() {
                    alias_map.remove(f.name());
                }
                //now convert back the alias
                let group_by_expr = resolve_alias_to_exprs(&group_by_expr, &alias_map)?;

                // in arrow datafusion, also support position (integer) exprs. here ommitted.

                //normalize
                let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                self.validate_schema_satisfies_exprs(
                    plan.output_schema().as_ref(),
                    &[group_by_expr.clone()],
                )?;
                Ok(group_by_expr)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // finally we are ready to do aggregation based on group by, aggregate expr, and having
        let (plan, select_exprs_post_aggr, having_expr_post_aggr) =
            if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
                // note the input plan of aggregation is the one filtered, not the projected before
                self.aggregate(
                    plan,
                    &select_exprs,
                    having_expr.as_ref(),
                    group_by_exprs,
                    aggr_exprs,
                )?
            } else {
                match having_expr {
                    Some(having_expr) => {
                        return Err(anyhow!(
                            "having clause {} must be used in aggregation",
                            having_expr
                        ))
                    }
                    None => (plan, select_exprs, having_expr),
                }
            };

        // next we need to replace having to use aggregate output result columns

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        //finally, we do the projection. then all done.

        let plan = project(plan, select_exprs_post_aggr)?;
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
            sqlparser::ast::JoinOperator::CrossJoin => self.parse_cross_join(left, right),
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

    fn parse_cross_join(&self, left: LogicalPlan, right: LogicalPlan) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
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
    fn select_items_to_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
    ) -> Result<Vec<Expr>> {
        projection
            .into_iter()
            .map(|select_item| self.select_item_to_expr(select_item, plan, empty_from))
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expr>>>()
    }

    /// convert a single select item into expr(s), include wildcard expansion
    fn select_item_to_expr(
        &self,
        select_item: SelectItem,
        plan: &LogicalPlan,
        empty_from: bool,
    ) -> Result<Vec<Expr>> {
        let schema = plan.output_schema();
        match select_item {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_expr_to_logical_expr(expr, plan.output_schema().as_ref())?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[plan.output_schema().as_ref()]],
                    &plan.using_columns()?,
                )?;
                Ok(vec![col])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.sql_expr_to_logical_expr(expr, plan.output_schema().as_ref())?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[plan.output_schema().as_ref()]],
                    &plan.using_columns()?,
                )?;
                let expr = Expr::Alias(Box::new(col), self.normalizer.normalize(alias));
                Ok(vec![expr])
            }
            // wildcard expansion, here options of wildcard will not be supported
            SelectItem::Wildcard(_options) => {
                if empty_from {
                    return Err(anyhow!("select * with no tables specified is not valid"));
                }
                let using_columns = plan.using_columns()?;
                let columns_to_skip = using_columns
                    .into_iter()
                    .flat_map(|cols| {
                        let mut cols = cols.into_iter().collect::<Vec<_>>();
                        cols.sort();
                        let mut out_column_names: HashSet<String> = HashSet::new();
                        cols.into_iter()
                            .filter_map(|c| {
                                if out_column_names.contains(&c.name) {
                                    Some(c)
                                } else {
                                    out_column_names.insert(c.name);
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<HashSet<_>>();

                let exprs = if columns_to_skip.is_empty() {
                    schema
                        .fields()
                        .iter()
                        .map(|f| Expr::Column(f.qualified_column()))
                        .collect::<Vec<Expr>>()
                } else {
                    schema
                        .fields()
                        .iter()
                        .filter_map(|f| {
                            let col = f.qualified_column();
                            if !columns_to_skip.contains(&col) {
                                Some(Expr::Column(col))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<Expr>>()
                };
                Ok(exprs)
            }
            SelectItem::QualifiedWildcard(ref object_name, _options) => {
                let qualifier = format!("{object_name}");
                let qualifier = TableReference::from(qualifier);
                let qualified_fields = schema
                    .fields_with_qualifed(&qualifier)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                if qualified_fields.is_empty() {
                    return Err(anyhow!(format!("Invalid qualifier {qualifier}")));
                }
                let qualified_schema =
                    Schema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
                Ok(qualified_schema
                    .fields()
                    .iter()
                    .map(|f| Expr::Column(f.qualified_column()))
                    .collect::<Vec<Expr>>())
            }
        }
    }

    fn project(&self, input: LogicalPlan, exprs: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.output_schema().as_ref(), &exprs)?;
        LogicalPlanBuilder::from(input).project(exprs)?.build()
    }

    /// do aggregation, return the new select and having exprs where the aggregate func will use aggregate output columns
    fn aggregate(
        &self,
        input: LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        group_by_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        // first create a pure aggregation plan node, not include other algorithmic ops
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_by_exprs.clone(), aggr_exprs.clone())?
            .build()?;

        // now put the group by and aggregate ops together;
        let mut groupby_and_agg = group_by_exprs.clone();
        groupby_and_agg.extend_from_slice(&aggr_exprs.clone());

        // furthure, make columns full qualified, this step does not do any calculation actually.
        let groupby_and_agg = groupby_and_agg
            .iter()
            .map(|expr| resolve_columns(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // now what we do is, create output columns from aggregation result, later used for just
        //  validation, no have actual effect
        let column_exprs = groupby_and_agg
            .iter()
            .map(|expr| expr_as_column_expr(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // now we rewrite the select exprs  using ouput of aggregation, call the rebase function
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|e| rebase_expr(e, &groupby_and_agg, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // check, projection cannot use non-aggregate values
        check_columns_satisfy_exprs(
            &column_exprs,
            &select_exprs_post_aggr,
            "Projetion references non-aggregate values",
        )?;

        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr = rebase_expr(having_expr, &groupby_and_agg, &input)?;
            check_columns_satisfy_exprs(
                &column_exprs,
                &[having_expr_post_aggr.clone()],
                "Having references non-aggregate values",
            )?;
            Some(having_expr_post_aggr)
        } else {
            None
        };
        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
    }

    fn order_by(&self, plan: LogicalPlan, order_by: Vec<OrderByExpr>) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        let mut order_by_expr = vec![];
        for e in order_by.iter() {
            let OrderByExpr {
                asc,
                expr,
                nulls_first,
            } = e;
            // not the input expr is ast expr
            let expr =
                self.sql_expr_to_logical_expr(expr.clone(), plan.output_schema().as_ref())?;
            let asc = asc.unwrap_or(true);
            order_by_expr.push(Expr::Sort(Sort {
                expr: Box::new(expr),
                asc: asc,
                nulls_first: nulls_first.unwrap_or(!asc),
            }))
        }
        LogicalPlanBuilder::from(plan).sort(order_by_expr)?.build()
    }
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        use crate::common::types::DataValue;
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }
        let skip = match skip {
            Some(skip_expr) => {
                match self.ast_expr_to_expr(skip_expr.value, input.output_schema().as_ref())? {
                    Expr::Literal(DataValue::Int64(Some(s))) => {
                        if s < 0 {
                            return Err(anyhow!(format!("offset must >=0, got: {s}")));
                        }
                        Ok(s as usize)
                    }
                    _ => Err(anyhow!(format!("unexpected expression in offset clause"))),
                }
            }?,
            _ => 0,
        };

        let fetch = match fetch {
            Some(limit_expr)
                if limit_expr != sqlparser::ast::Expr::Value(sqlparser::ast::Value::Null) =>
            {
                let n = match self.ast_expr_to_expr(limit_expr, input.output_schema().as_ref())? {
                    Expr::Literal(DataValue::Int64(Some(n))) if n >= 0 => Ok(n as usize),
                    _ => Err(anyhow!(format!("limit cannot be negative"))),
                }?;
                Some(n)
            }
            _ => None,
        };
        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
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
        // the case of join
        let sql = "SELECT A.id, A.name, B.score from testdb.student A inner join testdb.enroll B on A.id=B.student_id inner join testdb.course C on A.id=C.id where B.score > 99 order by B.score";
        let statement = parse(sql).unwrap();
        let _plan = session.state.read().make_logical_plan(statement).await?;
        //println!("{:?}", _plan);

        // the case of aggregation
        let sql = "SELECT max(age)+100, address from testdb.student where id<100 group by address having max(age)<20";
        let statement = parse(sql).unwrap();
        let _plan = session.state.read().make_logical_plan(statement).await?;
        //println!("{:?}", _plan);
        Ok(())
    }
}
