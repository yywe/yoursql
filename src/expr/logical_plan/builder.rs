use crate::{
    common::{schema::SchemaRef, table_reference::OwnedTableReference},
    expr::{
        expr_rewriter::{
            normalize_col, normalize_col_with_schemas_and_ambiguity_check, normalize_cols,
            rewrite_sort_cols_by_aggs,
        },
        utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard},
    },
    storage::Table,
};

use super::LogicalPlan;
use crate::{
    common::{column::Column, schema::Field},
    expr::{
        expr_schema::exprlist_to_fields,
        logical_plan::{
            Aggregate, EmptyRelation, Expr, Filter, JoinType, Projection, Schema, TableScan,
        },
    },
};
use anyhow::{anyhow, Result};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.output_schema()
    }

    pub fn build(self) -> Result<LogicalPlan> {
        Ok(self.plan)
    }

    pub fn empty(produce_one_row: bool) -> Self {
        Self::from(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: SchemaRef::new(Schema::empty()),
        }))
    }

    pub fn scan_with_filters(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn Table>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<Self> {
        let table_name = table_name.into();
        if table_name.table_name().is_empty() {
            return Err(anyhow!("table name cannot be empty"));
        }
        let schema = table_source.get_table();
        // in case projection is none, default will copy the fields and set qualifier
        // in case projection is not None, note to add the qualifer here
        let projected_schema = projection
            .as_ref()
            .map(|p| {
                Schema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            let mut new_field = schema.field(*i).clone();
                            new_field.set_qualifier(Some(table_name.clone()));
                            new_field
                        })
                        .collect(),
                    schema.metadata().clone(),
                )
            })
            .unwrap_or_else(|| Schema::try_from_qualified_schema(table_name.clone(), &schema))?;
        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name,
            source: table_source,
            projection: projection,
            projected_schema: Arc::new(projected_schema),
            filters: filters,
            fetch: None,
        });
        Ok(Self::from(table_scan))
    }

    pub fn scan(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn Table>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Self::scan_with_filters(table_name, table_source, projection, vec![])
    }

    pub fn normalize(plan: &LogicalPlan, column: impl Into<Column> + Clone) -> Result<Column> {
        let schema = plan.output_schema();
        let fallback_schemas = plan.fallback_normalize_schemas();
        let fallback_schemas_ref: Vec<&Schema> = fallback_schemas.iter().collect();
        let using_columns = plan.using_columns()?;
        column.into().normalize_with_schemas_and_ambiguity_check(
            &[&[schema.as_ref()], &fallback_schemas_ref],
            &using_columns,
        )
    }

    pub fn project(self, expr: impl IntoIterator<Item = impl Into<Expr>>) -> Result<Self> {
        Ok(Self::from(project(self.plan, expr)?))
    }

    pub fn filter(self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Ok(Self::from(LogicalPlan::Filter(Filter::try_new(
            expr,
            Arc::new(self.plan),
        )?)))
    }

    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Limit(super::Limit {
            skip,
            fetch,
            input: Arc::new(self.plan),
        })))
    }

    pub fn aggregate(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;
        Ok(Self::from(LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(self.plan),
            group_expr,
            aggr_expr,
        )?)))
    }

    /// apply sort plan given the input, first check and insert missing columns, then do a projection
    /// if missing columns are added. when adding missing, will recursively add for all downstream plans
    ///
    /// another thing, if the sort op has aggregation, it needs to use the aggregation output. at the time
    /// of sort, the aggregation should have already by done. so just needs a rewrite, i.e. rewrite
    /// the aggregation expression using the output of the aggregation node name

    /// in the case of toydb, this is done by insert separate placeholder columns 1st for aggregation
    /// result, then all the other operation is based on the result column
    /// the aggregation is done at logical plan level, not expression level.
    ///
    ///
    /// while here, aggregation is part of expression (also order by is expression)

    pub fn sort(self, exprs: impl IntoIterator<Item = impl Into<Expr>> + Clone) -> Result<Self> {
        let exprs = rewrite_sort_cols_by_aggs(exprs, &self.plan)?;
        let schema = self.plan.output_schema();
        let mut missing_cols: Vec<Column> = vec![];
        exprs
            .clone()
            .into_iter()
            .try_for_each::<_, Result<()>>(|expr| {
                // first get all the used columns in the expr
                let columns = expr.used_columns()?;

                // check if the input schema has the column (field), if not, add to missing
                columns.into_iter().for_each(|c| {
                    if schema.field_from_column(&c).is_err() {
                        missing_cols.push(c)
                    }
                });
                Ok(())
            })?;

        if missing_cols.is_empty() {
            return Ok(Self::from(LogicalPlan::Sort(super::Sort {
                expr: normalize_cols(exprs, &self.plan)?,
                input: Arc::new(self.plan),
                fetch: None,
            })));
        }

        // the strategy is, we will first add the missing columns to the current logical plan
        // and then do a projection to recover the original projection structure
        // thus, we need to new projections based on the output of sorted plan
        // so the new projection will diretly refer the column name

        // it is just the column names based on original schema
        let new_exprs = schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        // first do a sort plan
        let plan = Self::add_missing_columns(self.plan, &missing_cols)?;
        let plan_sorted = LogicalPlan::Sort(super::Sort {
            expr: normalize_cols(exprs, &plan)?,
            input: Arc::new(plan),
            fetch: None,
        });

        // now do a projection
        Ok(Self::from(LogicalPlan::Projection(Projection::try_new(
            new_exprs,
            Arc::new(plan_sorted),
        )?)))
    }

    /// add missing sort columns to all downstream projection (recursively call itself)
    /// very smart recursive implementation
    fn add_missing_columns(curr_plan: LogicalPlan, missing_cols: &[Column]) -> Result<LogicalPlan> {
        match curr_plan {
            LogicalPlan::Projection(Projection {
                input,
                mut exprs,
                schema: _,
            }) if missing_cols
                .iter()
                .all(|c| input.output_schema().has_column(c)) =>
            {
                // note above condition, this is a recursion break out case, where
                // we find a projection node, whose input schema (input plan's output) has
                // all the missing columns, so we add it from here (this projection)
                let mut missing_exprs = missing_cols
                    .iter()
                    .map(|c| normalize_col(Expr::Column(c.clone()), &input))
                    .collect::<Result<Vec<_>>>()?;
                // deduplication
                missing_exprs.retain(|e| !exprs.contains(e));
                exprs.extend(missing_exprs);
                Ok(project((*input).clone(), exprs)?) // return a projection which has all missing columns
            }

            _ => {
                // recursion case, either not projection, or the projection does not have all missing columns
                // continue look in bottom layers recursively

                // first process all child plan nodes (i.e, add missing columns for them)
                let new_inputs = curr_plan
                    .inputs()
                    .into_iter()
                    .map(|p| Self::add_missing_columns((*p).clone(), missing_cols))
                    .collect::<Result<Vec<_>>>()?;
                // all children has been updated, now update current project node itself
                curr_plan.with_new_inputs(&new_inputs)
            }
        }
    }

    pub fn join(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        self.join_detailed(right, join_type, join_keys, filter, false)
    }

    pub fn join_detailed(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
        null_equals_null: bool,
    ) -> Result<Self> {
        if join_keys.0.len() != join_keys.1.len() {
            return Err(anyhow!(
                "join left keys are not the same length as the right keys"
            ));
        }
        let filter = if let Some(expr) = filter {
            let filter = normalize_col_with_schemas_and_ambiguity_check(
                expr,
                &[&[
                    self.plan.output_schema().as_ref(),
                    right.output_schema().as_ref(),
                ]],
                &[],
            )?;
            Some(filter)
        } else {
            None
        };

        // in where A join B on x = y, it is unkown x is from A or B, we need to resolve the qualifier first
        let (left_keys, right_keys): (Vec<Result<Column>>, Vec<Result<Column>>) = join_keys
            .0
            .into_iter()
            .zip(join_keys.1.into_iter())
            .map(|(l, r)| {
                let l = l.into();
                let r = r.into();
                let left_schema = self.plan.output_schema();
                let right_schema = right.output_schema();
                match (&l.relation, &r.relation) {
                    (Some(lr), Some(rr)) => {
                        let l_is_left = left_schema.field_with_qualified_name(lr, &l.name);
                        let l_is_right = right_schema.field_with_qualified_name(lr, &l.name);
                        let r_is_left = left_schema.field_with_qualified_name(rr, &r.name);
                        let r_is_right = right_schema.field_with_qualified_name(rr, &r.name);
                        match (l_is_left, l_is_right, r_is_left, r_is_right) {
                            (_, Ok(_), Ok(_), _) => (Ok(r), Ok(l)),
                            (Ok(_), _, _, Ok(_)) => (Ok(l), Ok(r)),
                            _ => (Self::normalize(&self.plan, l), Self::normalize(&right, r)),
                        }
                    }
                    (Some(lr), None) => {
                        let l_is_left = left_schema.field_with_qualified_name(lr, &l.name);
                        let l_is_right = right_schema.field_with_qualified_name(lr, &l.name);
                        match (l_is_left, l_is_right) {
                            (Ok(_), _) => (Ok(l), Self::normalize(&right, r)),
                            (_, Ok(_)) => (Self::normalize(&self.plan, r), Ok(l)),
                            _ => (Self::normalize(&self.plan, l), Self::normalize(&right, r)),
                        }
                    }
                    (None, Some(rr)) => {
                        let r_is_left = left_schema.field_with_qualified_name(rr, &r.name);
                        let r_is_right = right_schema.field_with_qualified_name(rr, &r.name);

                        match (r_is_left, r_is_right) {
                            (Ok(_), _) => (Ok(r), Self::normalize(&right, l)),
                            (_, Ok(_)) => (Self::normalize(&self.plan, l), Ok(r)),
                            _ => (Self::normalize(&self.plan, l), Self::normalize(&right, r)),
                        }
                    }
                    (None, None) => {
                        let mut swap = false;
                        let left_key = Self::normalize(&self.plan, l.clone()).or_else(|_| {
                            swap = true;
                            Self::normalize(&right, l)
                        });
                        if swap {
                            (Self::normalize(&self.plan, r), left_key)
                        } else {
                            (left_key, Self::normalize(&right, r))
                        }
                    }
                }
            })
            .unzip();

        let left_keys = left_keys.into_iter().collect::<Result<Vec<Column>>>()?;
        let right_keys = right_keys.into_iter().collect::<Result<Vec<Column>>>()?;

        let on = left_keys
            .into_iter()
            .zip(right_keys.into_iter())
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();

        let join_schema = build_join_schema(
            self.plan.output_schema().as_ref(),
            right.output_schema().as_ref(),
            &join_type,
        )?;

        Ok(Self::from(LogicalPlan::Join(super::Join {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            on: on,
            filter: filter,
            join_type: join_type,
            join_constraint: super::JoinConstraint::On,
            schema: Arc::new(join_schema),
            null_equals_null: null_equals_null,
        })))
    }

    pub fn join_using(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        using_keys: Vec<impl Into<Column> + Clone>,
    ) -> Result<Self> {
        let left_keys: Vec<Column> = using_keys
            .clone()
            .into_iter()
            .map(|c| Self::normalize(&self.plan, c))
            .collect::<Result<_>>()?;
        let right_keys: Vec<Column> = using_keys
            .clone()
            .into_iter()
            .map(|c| Self::normalize(&right, c))
            .collect::<Result<_>>()?;
        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();
        let join_schema = build_join_schema(
            self.plan.output_schema().as_ref(),
            right.output_schema().as_ref(),
            &join_type,
        )?;
        let mut join_on: Vec<(Expr, Expr)> = vec![];
        for (l, r) in &on {
            if !self.plan.output_schema().has_column(l) || !right.output_schema().has_column(r) {
                return Err(anyhow!(format!(
                    "join left column {} or right column {} is missing in respective schema",
                    l, r
                )));
            } else {
                join_on.push((Expr::Column(l.clone()), Expr::Column(r.clone())))
            }
        }
        Ok(Self::from(LogicalPlan::Join(super::Join {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            on: join_on,
            filter: None,
            join_type: join_type,
            join_constraint: super::JoinConstraint::Using,
            schema: Arc::new(join_schema),
            null_equals_null: false,
        })))
    }

    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let schema = self.plan.output_schema().join(&right.output_schema())?;
        Ok(Self::from(LogicalPlan::CrossJoin(super::CrossJoin {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            schema: Arc::new(schema),
        })))
    }
}

pub fn project(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<Expr>>,
) -> Result<LogicalPlan> {
    let input_schema = plan.output_schema();
    let mut projected_expr = vec![];
    for e in expr {
        let e = e.into();
        match e {
            Expr::Wildcard => projected_expr.extend(expand_wildcard(&input_schema, &plan)?),
            Expr::QualifiedWildcard { ref qualifier } => {
                projected_expr.extend(expand_qualified_wildcard(qualifier, &input_schema)?)
            }
            _ => projected_expr.push(columnize_expr(
                normalize_col(e, &plan)?,
                input_schema.as_ref(),
            )),
        }
    }
    validate_unique_names("Projections", projected_expr.iter())?;
    let projected_schema = Schema::new_with_metadata(
        exprlist_to_fields(&projected_expr, &plan)?,
        plan.output_schema().metadata().clone(),
    )?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        projected_expr,
        Arc::new(plan.clone()),
        Arc::new(projected_schema),
    )?))
}

pub fn build_join_schema(left: &Schema, right: &Schema, join_type: &JoinType) -> Result<Schema> {
    fn nullify_fields(fields: &[Field]) -> Vec<Field> {
        fields
            .iter()
            .map(|f| f.clone().with_nullable(true))
            .collect()
    }
    let right_fields = right
        .fields()
        .to_field_vec()
        .iter()
        .map(|&e| e.clone())
        .collect::<Vec<_>>();
    let left_fields = left
        .fields()
        .to_field_vec()
        .iter()
        .map(|&e| e.clone())
        .collect::<Vec<_>>();
    let fields: Vec<Field> = match join_type {
        JoinType::Inner => left_fields
            .iter()
            .chain(right_fields.iter())
            .cloned()
            .collect(),
        JoinType::Left => left_fields
            .iter()
            .chain(&nullify_fields(&right_fields))
            .cloned()
            .collect(),
        JoinType::Right => nullify_fields(&left_fields)
            .iter()
            .chain(right_fields.iter())
            .cloned()
            .collect(),
        JoinType::Full => nullify_fields(&left_fields)
            .iter()
            .chain(&nullify_fields(&right_fields))
            .cloned()
            .collect(),
    };
    let mut metadata = left.metadata().clone();
    metadata.extend(right.metadata().clone());
    Schema::new_with_metadata(fields, metadata)
}

pub fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.into_iter().enumerate().try_for_each(|(position, expr)|{
        let name = expr.display_name()?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            }
            Some((_existing_position, existing_expr))=>{
                return Err(anyhow!(format!(
                    "{node_name} requires unique expression names but expression {existing_expr} and {expr} as the same name"
                )))
            }
        }
    })
}

pub fn wrap_projection_for_join(
    join_keys: &[Expr],
    input: LogicalPlan,
) -> Result<(LogicalPlan, Vec<Column>, bool)> {
    let input_schema = input.output_schema();
    let cloned_join_keys = join_keys.iter().map(|key| key.clone()).collect::<Vec<_>>();
    let need_project = join_keys.iter().any(|key| !matches!(key, Expr::Column(_)));
    let plan = if need_project {
        // first convert the schema fields to Expr, (cause later we need Expr to project)
        let mut projection = expand_wildcard(input_schema.as_ref(), &input)?;
        // then get join item  that is not a Column, i.e, if try_into_col() is error then get its expr
        let join_key_items = cloned_join_keys
            .iter()
            .flat_map(|expr| expr.try_into_col().is_err().then_some(expr))
            .cloned()
            .collect::<HashSet<Expr>>();
        projection.extend(join_key_items);
        LogicalPlanBuilder::from(input)
            .project(projection)?
            .build()?
    } else {
        input
    };

    // if try_into_col fail, i.e, if join item is not a column, create a column
    let join_on = cloned_join_keys
        .into_iter()
        .map(|key| {
            key.try_into_col()
                .or_else(|_| Ok(Column::from_name(key.display_name()?)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((plan, join_on, need_project))
}
