use anyhow::{anyhow, Result};

use super::type_coercion::get_result_type;
use crate::common::schema::{ColumnMeta, Field, Schema};
use crate::common::types::DataType;
use crate::expr::expr::{AggregateFunction, AggregateFunctionType, BinaryExpr, Expr, Sort};
use crate::expr::logical_plan::{Aggregate, LogicalPlan};
use crate::expr::utils::agg_cols;

/// we need to be able to compose a schema based on expressions
/// each time for a plan node, the output is a schema based on expressions
pub trait ExprToSchema {
    fn get_type<M: ColumnMeta>(&self, schema: &M) -> Result<DataType>;
    fn nullable<M: ColumnMeta>(&self, schema: &M) -> Result<bool>;
    fn to_field(&self, input_schema: &Schema) -> Result<Field>;
    fn cast_to<M: ColumnMeta>(self, cast_to_type: &DataType, schema: &M) -> Result<Expr>;
}

/// Trait so we can convert Expressions to a Schema structure
impl ExprToSchema for Expr {
    fn get_type<M: ColumnMeta>(&self, schema: &M) -> Result<DataType> {
        match self {
            Expr::Alias(expr, ..) => expr.get_type(schema),
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::Between { .. }
            | Expr::IsNotNull(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::Like(_)
            | Expr::ILike(_)
            | Expr::IsTrue(_)
            | Expr::IsNotFalse(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ref op,
            }) => get_result_type(&left.get_type(schema)?, op, &right.get_type(schema)?),
            Expr::Wildcard => Ok(DataType::Null),
            Expr::Sort(Sort { expr, .. }) => expr.get_type(schema),
            Expr::QualifiedWildcard { .. } => Err(anyhow!(
                "qualified wildcard should not exist in logical query plan"
            )),
            Expr::AggregateFunction(AggregateFunction { fun, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                AggregateFunctionType::return_type(fun, &data_types)
            }
        }
    }

    fn nullable<M: ColumnMeta>(&self, schema: &M) -> Result<bool> {
        use crate::expr::expr::{Between, Like};
        match self {
            Expr::Alias(expr, _) | Expr::Not(expr) | Expr::Sort(Sort { expr, .. }) => {
                expr.nullable(schema)
            }
            Expr::Column(c) => schema.nullable(c),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_) => Ok(false),
            Expr::Like(Like { expr, .. }) => expr.nullable(schema),
            Expr::ILike(Like { expr, .. }) => expr.nullable(schema),
            Expr::Between(Between { expr, .. }) => expr.nullable(schema),
            Expr::Wildcard => Err(anyhow!("wildcard is not valid in logical query plan")),
            Expr::QualifiedWildcard { .. } => Err(anyhow!(
                "qualified wildcard are not valid in logical query plan"
            )),
            Expr::AggregateFunction { .. } => Ok(true),
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ..
            }) => Ok(left.nullable(schema)? || right.nullable(schema)?),
        }
    }

    fn to_field(&self, input_schema: &Schema) -> Result<Field> {
        match self {
            Expr::Column(c) => Ok(Field::new(
                &c.name,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
                c.relation.clone(),
            )),
            _ => Ok(Field::new(
                &self.display_name()?,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
                None,
            )),
        }
    }

    fn cast_to<M: ColumnMeta>(self, cast_to_type: &DataType, schema: &M) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }
        return Err(anyhow!("data type cast not supported yet"));
    }
}

/// given a list of expressions and the relevant input logical plan, generate a list of fields
/// plan is the input plan node
pub fn exprlist_to_fields<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    plan: &LogicalPlan,
) -> Result<Vec<Field>> {
    let exprs = exprs.into_iter().cloned().collect::<Vec<Expr>>();
    let fields = match plan {
        LogicalPlan::Aggregate(agg) => Some(expr_list_to_fields_aggregate(&exprs, plan, agg)),
        _ => None,
    };
    if let Some(fields) = fields {
        fields
    } else {
        let res_schema = plan.output_schema();
        exprs.iter().map(|e| e.to_field(&res_schema)).collect()
    }
}

/// given expressions and a aggregate plan, generate the list of fields
fn expr_list_to_fields_aggregate(
    exprs: &[Expr],
    plan: &LogicalPlan,
    agg: &Aggregate,
) -> Result<Vec<Field>> {
    let agg_cols = agg_cols(agg);
    let mut fields = vec![];
    for expr in exprs {
        match expr {
            Expr::Column(c) if agg_cols.iter().any(|x| x == c) => {
                fields.push(expr.to_field(agg.input.output_schema().as_ref())?);
            }
            _ => fields.push(expr.to_field(plan.output_schema().as_ref())?),
        }
    }
    Ok(fields)
}
