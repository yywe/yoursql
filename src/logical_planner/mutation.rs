use super::{LogicalPlanner, PlannerContext};
use crate::common::schema::SchemaRef;
use crate::expr::logical_plan::Insert;
use crate::expr::logical_plan::Values;
use crate::{common::table_reference::OwnedTableReference, expr::logical_plan::LogicalPlan};
use anyhow::{anyhow, Result};
use sqlparser::ast::{Ident, Query};
use std::sync::Arc;

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn plan_insert(
        &self,
        table_reference: OwnedTableReference,
        schema: SchemaRef,
        columns_ident: Vec<Ident>,
        source: Query,
    ) -> Result<LogicalPlan> {
        //let table_reference = object_name_to_table_refernce(table_name, true)?;
        let input = self.plan_set_expr(*source.body)?;
        if let LogicalPlan::Values(Values {
            schema: _schema,
            values,
        }) = input.clone()
        {
            let projected_schema = if columns_ident.is_empty() {
                schema.clone()
            } else {
                let columns: Vec<String> = columns_ident
                    .into_iter()
                    .map(|id| id.value.clone())
                    .collect();
                let indices = columns
                    .into_iter()
                    .map(|cn| schema.index_of_column_by_name(None, &cn))
                    .collect::<Result<Vec<_>>>()?;
                let checked_indices = indices
                    .into_iter()
                    .map(|e| match e {
                        Some(index) => Ok(index),
                        None => Err(anyhow!("failed to find some column name")),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(schema.project(&checked_indices)?)
            };
            //IMPORTANT: fix the schema of values
            let fixed_plan = LogicalPlan::Values(Values {
                schema: projected_schema.clone(),
                values: values,
            });
            Ok(LogicalPlan::Insert(Insert {
                table_name: table_reference,
                table_schema: schema,
                projected_schema: projected_schema,
                input: Arc::new(fixed_plan),
            }))
        } else {
            Err(anyhow!(format!("input plan of insert must be values")))
        }
    }
}
