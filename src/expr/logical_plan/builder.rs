use crate::common::schema::SchemaRef;
use crate::common::table_reference::OwnedTableReference;
use crate::expr::utils::columnize_expr;
use crate::expr::utils::expand_qualified_wildcard;
use crate::expr::utils::expand_wildcard;
use crate::expr::expr_rewriter::normalize_col;
use crate::storage::Table;

use super::LogicalPlan;
use crate::expr::logical_plan::EmptyRelation;
use crate::common::column::Column;
use crate::expr::logical_plan::Expr;
use crate::expr::logical_plan::{JoinType,Schema};
use crate::expr::logical_plan::{TableScan,Projection};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use crate::common::schema::Field;
use std::collections::HashMap;

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
        let projected_schema = projection
            .as_ref()
            .map(|p| {
                Schema::new_with_metadata(
                    p.iter().map(|i| schema.field(*i).clone()).collect(),
                    schema.metadata().clone(),
                )
            })
            .unwrap_or_else(|| Err(anyhow!("failed to project schema")))?;
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

    pub fn scan(table_name: impl Into<OwnedTableReference>, table_source: Arc<dyn Table>, projection: Option<Vec<usize>>)->Result<Self> {
        Self::scan_with_filters(table_name, table_source, projection, vec![])
    }

    pub fn normalize(plan: &LogicalPlan, column: impl Into<Column> + Clone) -> Result<Column> {
        let schema = plan.output_schema();
        let fallback_schemas = plan.fallback_normalize_schemas();
        let fallback_schemas_ref:Vec<&Schema> = fallback_schemas.iter().collect();
        let using_columns = plan.using_columns()?;
        column.into().normalize_with_schemas_and_ambiguity_check(
            &[&[schema.as_ref()], &fallback_schemas_ref],
            &using_columns,
        )
    }

    pub fn project(self,expr: impl IntoIterator<Item = impl Into<Expr>>) -> Result<Self> {
        Ok(Self::from(project(self.plan, expr)?))
    }
}


pub fn project(plan: LogicalPlan, expr: impl IntoIterator<Item = impl Into<Expr>>) -> Result<LogicalPlan> {
    let input_schema = plan.output_schema();
    let mut projected_expr = vec![];
    for e in expr {
        let e = e.into();
        match e {
            Expr::Wildcard => {
                projected_expr.extend(expand_wildcard(&input_schema, &plan)?)
            }
            Expr::QualifiedWildcard { ref qualifier } => {
                projected_expr.extend(expand_qualified_wildcard(qualifier, &input_schema)?)
            }
            _=>projected_expr.push(columnize_expr(normalize_col(e, &plan)?, input_schema.as_ref()))
        }
    }
    validate_unique_names("Projections", projected_expr.iter())?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(projected_expr, Arc::new(plan.clone()), input_schema)?))
}

pub fn build_join_schema(left: &Schema, right: &Schema, join_type: &JoinType) -> Result<Schema> {
    fn nullify_fields(fields: &[Field])->Vec<Field> {
        fields.iter().map(|f|f.clone().with_nullable(true)).collect()
    }
    let right_fields = right.fields().to_field_vec().iter().map(|&e|e.clone()).collect::<Vec<_>>();
    let left_fields = left.fields().to_field_vec().iter().map(|&e|e.clone()).collect::<Vec<_>>();
    let fields: Vec<Field> = match join_type {
        JoinType::Inner => {
            left_fields.iter().chain(right_fields.iter()).cloned().collect()
        }
        JoinType::Left=>{
            left_fields.iter().chain(&nullify_fields(&right_fields)).cloned().collect()
        }
        JoinType::Right=>{
            nullify_fields(&left_fields).iter().chain(right_fields.iter()).cloned().collect()
        }
        JoinType::Full => {
            nullify_fields(&left_fields).iter().chain(&nullify_fields(&right_fields)).cloned().collect()
        }
    };
    let mut metadata = left.metadata().clone();
    metadata.extend(right.metadata().clone());
    Schema::new_with_metadata(fields, metadata) 
}

pub fn validate_unique_names<'a>(node_name: &str, expressions: impl IntoIterator<Item = &'a Expr>)->Result<()> {
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