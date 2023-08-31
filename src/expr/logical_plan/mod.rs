pub mod builder;

use crate::common::column::Column;
use crate::common::schema::Schema;
use crate::common::tree_node::TreeNodeVisitor;
use crate::common::tree_node::{TreeNode, VisitRecursion};
use crate::common::types::DataType;
use crate::expr::utils::from_plan;
use crate::{
    common::{schema::SchemaRef, table_reference::OwnedTableReference},
    expr::expr::Expr,
    storage::Table,
};
use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use super::expr_schema::{exprlist_to_fields, ExprToSchema};
use crate::expr_vec_fmt;

#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Projection {
    pub exprs: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Filter {
    pub predicate: Expr,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggr_expr: Vec<Expr>,
    pub schema: SchemaRef,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Sort {
    pub expr: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub fetch: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
        };
        write!(f, "{join_type}")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    On,
    Using,
}

#[derive(Clone)]
pub struct TableScan {
    pub table_name: OwnedTableReference,
    pub source: Arc<dyn Table>,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: SchemaRef,
    pub filters: Vec<Expr>,
    pub fetch: Option<usize>,
}

/// manual impl for PartialEq, omit source
impl PartialEq for TableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.projection == other.projection
            && self.projected_schema == other.projected_schema
            && self.filters == other.filters
            && self.fetch == other.fetch
    }
}
impl Eq for TableScan {}

impl Hash for TableScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.projection.hash(state);
        self.projected_schema.hash(state);
        self.filters.hash(state);
        self.fetch.hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
    pub schema: SchemaRef,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Limit {
    pub skip: usize,
    pub fetch: Option<usize>,
    pub input: Arc<LogicalPlan>,
}

impl LogicalPlan {
    /// get the output schema from this logical plan node
    pub fn output_schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema.clone(),
            LogicalPlan::TableScan(TableScan {
                projected_schema, ..
            }) => projected_schema.clone(),
            LogicalPlan::Filter(Filter { input, .. }) => input.output_schema(),
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema.clone(),
            LogicalPlan::Sort(Sort { input, .. }) => input.output_schema(),
            LogicalPlan::Join(Join { schema, .. }) => schema.clone(),
            LogicalPlan::Limit(Limit { input, .. }) => input.output_schema(),
            LogicalPlan::Projection(Projection { schema, .. }) => schema.clone(),
        }
    }

    /// get all the input node
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input],
            LogicalPlan::Sort(Sort { input, .. }) => vec![input],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => vec![],
        }
    }

    /// wrapper of self to display, display node itself, not include children
    pub fn display(&self) -> impl std::fmt::Display + '_ {
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> std::fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self.0 {
                    LogicalPlan::EmptyRelation(_) => write!(f, "EmptyRelation"),
                    LogicalPlan::TableScan(TableScan {
                        ref source,
                        ref table_name,
                        ref projection,
                        ref fetch,
                        ..
                    }) => {
                        let projected_fields = match projection {
                            Some(indices) => {
                                let schema = source.get_table();
                                let names: Vec<&str> = indices
                                    .iter()
                                    .map(|i| schema.field(*i).name().as_str())
                                    .collect();
                                format!(" projection = [{}]", names.join(", "))
                            }
                            _ => "".to_string(),
                        };
                        write!(f, "TableScan: {table_name}{projected_fields}")?;
                        if let Some(n) = fetch {
                            write!(f, ", fetch={n}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Projection(Projection { ref exprs, .. }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in exprs.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter(Filter {
                        predicate: ref expr,
                        ..
                    }) => write!(f, "Filter: {expr}"),
                    LogicalPlan::Aggregate(Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    }) => {
                        write!(
                            f,
                            "Aggregate: groupBy=[[{}]], aggr=[[{}]]",
                            expr_vec_fmt!(group_expr),
                            expr_vec_fmt!(aggr_expr)
                        )
                    }
                    LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        if let Some(a) = fetch {
                            write!(f, ", fetch={a}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Join(Join {
                        on: ref keys,
                        filter,
                        join_constraint,
                        join_type,
                        ..
                    }) => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{l}={r}")).collect();
                        let filter_expr = filter
                            .as_ref()
                            .map(|expr| format!(" Filter:{expr}"))
                            .unwrap_or_else(|| "".to_string());
                        match join_constraint {
                            JoinConstraint::On => {
                                write!(
                                    f,
                                    "{} Join: {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                            JoinConstraint::Using => {
                                write!(
                                    f,
                                    "{} Join: Using{}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                        }
                    }
                    LogicalPlan::Limit(Limit {
                        ref skip,
                        ref fetch,
                        ..
                    }) => {
                        write!(
                            f,
                            "Limit: skip={}, fetch={}",
                            skip,
                            fetch.map_or_else(|| "None".to_string(), |x| x.to_string())
                        )
                    }
                }
            }
        }
        Wrapper(self)
    }

    pub fn display_indent(&self) -> impl std::fmt::Display + '_ {
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> std::fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                match self.0.visit(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(std::fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    pub fn using_columns(&self) -> Result<Vec<HashSet<Column>>> {
        let mut using_columns: Vec<HashSet<Column>> = vec![];
        self.apply(&mut |plan| {
            if let LogicalPlan::Join(Join {
                join_constraint: JoinConstraint::Using,
                on,
                ..
            }) = plan
            {
                let columns = on.iter().try_fold(HashSet::new(), |mut acc, (l, r)| {
                    acc.insert(l.try_into_col()?);
                    acc.insert(r.try_into_col()?);
                    Result::<_, anyhow::Error>::Ok(acc)
                })?;
                using_columns.push(columns);
            }
            Ok(VisitRecursion::Continue)
        })?;
        Ok(using_columns)
    }

    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.inspect_expressions(|e| {
            exprs.push(e.clone());
            Ok(())
        }).unwrap();
        exprs
    }

    /// call f on exprs belonging to current plan node
    pub fn inspect_expressions<F>(self: &LogicalPlan, mut f: F) -> Result<()>
    where
        F: FnMut(&Expr) -> Result<()>,
    {
        match self {
            LogicalPlan::Projection(Projection { exprs, .. }) => exprs.iter().try_for_each(f),
            LogicalPlan::Filter(Filter { predicate, .. }) => f(predicate),
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => group_expr.iter().chain(aggr_expr.iter()).try_for_each(f),
            LogicalPlan::Join(Join { on, filter, .. }) => {
                // here we concat the (l,r) as a equal expression and then export
                // the reason is that later, we will recover to the form of (l,r), ref
                // the function in from_plan, where we update plan based on expressions in this form
                on.iter()
                    .map(|(l, r)| Expr::eq(l.clone(), r.clone()))
                    .try_for_each(|e| f(&e))?;
                if let Some(filter) = filter.as_ref() {
                    f(filter)
                } else {
                    Ok(())
                }
            }
            LogicalPlan::Sort(Sort { expr, .. }) => expr.iter().try_for_each(f),
            LogicalPlan::TableScan(TableScan { filters, .. }) => filters.iter().try_for_each(f),
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Limit(_) => Ok(()),
        }
    }

    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> Result<LogicalPlan> {
        from_plan(self, &self.expressions(), inputs)
    }

    pub fn fallback_normalize_schemas(&self) -> Vec<Schema> {
        match self {
            LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Join(_) => self
                .inputs()
                .iter()
                .map(|input| input.output_schema())
                .map(|e|(*e).clone())
                .collect(),
            _ => vec![],
        }
    }
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
        let schema = Arc::new(Schema::new_with_metadata(
            exprlist_to_fields(&exprs, &input)?,
            input.output_schema().metadata().clone(),
        )?);
        Self::try_new_with_schema(exprs, input, schema)
    }
}

impl Filter {
    pub fn try_new(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self> {
        if let Ok(predicate_type) = predicate.get_type(input.output_schema().as_ref()) {
            if predicate_type != DataType::Boolean {
                return Err(anyhow!(format!(
                    "cannot create filter with non-boolean type {} ",
                    predicate_type
                )));
            }
        }
        Ok(Self { predicate, input })
    }
}

impl Aggregate {
    /// create aggregate node if the output schema of aggregation is already known
    pub fn try_new_with_schema(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: SchemaRef,
    ) -> Result<Self> {
        if group_expr.is_empty() && aggr_expr.is_empty() {
            return Err(anyhow!(
                "aggregate requires at least one grouping or aggregate operation"
            ));
        }
        let group_expr_count = group_expr.len();
        if schema.fields().len() != group_expr_count + aggr_expr.len() {
            return Err(anyhow!(format!(
                "Aggregate schema has wrong num of fields, expect {}, get {}",
                group_expr_count + aggr_expr.len(),
                schema.fields().len()
            )));
        }
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        let all_exprs = group_expr.iter().chain(aggr_expr.iter());
        let schema = Schema::new_with_metadata(
            exprlist_to_fields(all_exprs, &input)?,
            input.output_schema().metadata().clone(),
        )?;
        Self::try_new_with_schema(input, group_expr, aggr_expr, Arc::new(schema))
    }
}

pub struct IndentVisitor<'a, 'b> {
    f: &'a mut std::fmt::Formatter<'b>,
    with_schema: bool,
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    pub fn new(f: &'a mut std::fmt::Formatter<'b>, with_schema: bool) -> Self {
        Self {
            f,
            with_schema,
            indent: 0,
        }
    }
}

impl<'a, 'b> TreeNodeVisitor for IndentVisitor<'a, 'b> {
    type N = LogicalPlan;
    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
        if self.indent > 0 {
            writeln!(self.f)?; //just write a new line
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?; // write some tabs
        write!(self.f, "{}", plan.display())?;
        if self.with_schema {
            write!(
                self.f,
                " {}",
                display_schema(&plan.output_schema().as_ref().to_owned().into())
            )?;
        }
        self.indent += 1;
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion> {
        self.indent -= 1;
        Ok(VisitRecursion::Continue)
    }
}

pub fn display_schema(schema: &Schema) -> impl std::fmt::Display + '_ {
    struct Wrapper<'a>(&'a Schema);
    impl<'a> std::fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "[")?;
            for (idx, field) in self.0.fields().iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                let nullable_str = if field.is_nullable() { ";N" } else { "" };
                write!(
                    f,
                    "{}:{:?}{}",
                    field.name(),
                    field.data_type(),
                    nullable_str
                )?;
            }
            write!(f, "]")
        }
    }
    Wrapper(schema)
}
