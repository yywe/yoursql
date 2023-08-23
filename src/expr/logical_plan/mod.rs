pub mod builder;

use crate::{
    common::{schema::SchemaRef, table_reference::OwnedTableReference},
    expr::expr::Expr,
    storage::Table,
};
use anyhow::{anyhow, Result};
use std::sync::Arc;

pub enum LogicalPlan {
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    Sort(Sort),
    Join(Join),
    TableScan(TableScan),
    EmptyRelation(EmptyRelation),
    Limit(Limit),
}

pub struct Projection {
    pub exprs: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
}

pub struct Filter {
    pub predicate: Expr,
    pub input: Arc<LogicalPlan>,
}

pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggr_expr: Vec<Expr>,
    pub schema: SchemaRef,
}

pub struct Sort {
    pub expr: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub fetch: Option<usize>,
}

pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub on: Vec<(Expr, Expr)>,
    pub filter: Option<Expr>,
    pub join_type: JoinType,
    pub join_constraint: JoinConstraint,
    pub schema: SchemaRef,
    pub null_equals_null: bool,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub enum JoinConstraint {
    On,
    Using,
}

pub struct TableScan {
    pub table_name: OwnedTableReference,
    pub source: Arc<dyn Table>,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: SchemaRef,
    pub filters: Vec<Expr>,
    pub fetch: Option<usize>,
}

pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

pub struct Limit {
    pub skip: usize,
    pub fetch: Option<usize>,
    pub input: Arc<LogicalPlan>,
}

impl Projection {
    pub fn try_new_with_schema(
        exprs: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: SchemaRef,
    ) -> Result<Self> {
        if exprs.len() != schema.fields().len() {
            return Err(anyhow!(format!(
                "number of exprs {} mismatch with schema fields num {}",
                exprs.len(),
                schema.fields().len()
            )));
        }
        Ok(Self {
            exprs,
            input,
            schema,
        })
    }
    pub fn try_new(exprs: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self> {
        //todo: to be continued
        return Err(anyhow!("not implemented yet"));
    }
}
