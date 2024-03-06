pub mod builder;

use crate::common::column::Column;
use crate::common::schema::Field;
use crate::common::schema::Schema;
use crate::common::schema::EMPTY_SCHEMA_REF;
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
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

use self::builder::build_join_schema;

use super::expr_schema::{exprlist_to_fields, ExprToSchema};
use crate::expr_vec_fmt;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    Sort(Sort),
    Join(Join),
    CrossJoin(CrossJoin),
    TableScan(TableScan),
    EmptyRelation(EmptyRelation),
    Limit(Limit),
    SubqueryAlias(SubqueryAlias),
    CreateTable(CreateTable),
    Values(Values),
    Insert(Insert),
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateTable {
    pub name: OwnedTableReference,
    pub fields: Vec<Field>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Values {
    pub schema: SchemaRef,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Insert {
    pub table_name: OwnedTableReference,
    pub table_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    pub input: Arc<LogicalPlan>,
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

impl Join {
    ///create new join based on original join, left and right has projection columns
    pub fn try_new_with_project_input(
        original: &LogicalPlan,
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        column_on: (Vec<Column>, Vec<Column>),
    ) -> Result<Self> {
        let original_join = match original {
            LogicalPlan::Join(join) => join,
            _ => return Err(anyhow::anyhow!("could not create join with project input")),
        };
        let on: Vec<(Expr, Expr)> = column_on
            .0
            .into_iter()
            .zip(column_on.1.into_iter())
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();
        let join_schema = build_join_schema(
            left.output_schema().as_ref(),
            right.output_schema().as_ref(),
            &original_join.join_type,
        )?;
        Ok(Join {
            left: left,
            right: right,
            on: on,
            filter: original_join.filter.clone(),
            join_type: original_join.join_type,
            join_constraint: original_join.join_constraint,
            schema: Arc::new(join_schema),
            null_equals_null: original_join.null_equals_null,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CrossJoin {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub schema: SchemaRef,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SubqueryAlias {
    pub input: Arc<LogicalPlan>,
    pub alias: OwnedTableReference,
    pub schema: SchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(plan: LogicalPlan, alias: impl Into<OwnedTableReference>) -> Result<Self> {
        let alias = alias.into();
        let schema: Schema = plan.output_schema().as_ref().clone().into();
        let schema = SchemaRef::new(Schema::try_from_qualified_schema(&alias, &schema)?);
        Ok(SubqueryAlias {
            input: Arc::new(plan),
            alias: alias,
            schema: schema,
        })
    }
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
            LogicalPlan::CrossJoin(CrossJoin { schema, .. }) => schema.clone(),
            LogicalPlan::Limit(Limit { input, .. }) => input.output_schema(),
            LogicalPlan::Projection(Projection { schema, .. }) => schema.clone(),
            LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. }) => schema.clone(),
            LogicalPlan::CreateTable(_) => Arc::clone(&EMPTY_SCHEMA_REF),
            LogicalPlan::Values(Values { schema, .. }) => schema.clone(),
            LogicalPlan::Insert(Insert { table_schema, .. }) => table_schema.clone(),
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
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::TableScan { .. } | LogicalPlan::EmptyRelation { .. } => vec![],
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => vec![input],
            LogicalPlan::CreateTable(_) | LogicalPlan::Values(_) => vec![],
            LogicalPlan::Insert(Insert { input, .. }) => vec![input],
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
                                    "{} Join: Using {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                        }
                    }
                    LogicalPlan::CrossJoin(_) => {
                        write!(f, "CrossJoin:")
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
                    LogicalPlan::SubqueryAlias(SubqueryAlias { ref alias, .. }) => {
                        write!(f, "SubqueryAlias: {alias}")
                    }
                    LogicalPlan::CreateTable(CreateTable {
                        ref name,
                        ref fields,
                    }) => {
                        write!(
                            f,
                            "Create Table: {}, Columns:({})",
                            name,
                            fields
                                .iter()
                                .map(|c| c.name().to_owned())
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    }
                    LogicalPlan::Values(Values { ref values, .. }) => {
                        let valuestr: Vec<_> = values
                            .iter()
                            .take(5)
                            .map(|row| {
                                let rowstr = row
                                    .iter()
                                    .map(|e| e.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({rowstr})")
                            })
                            .collect();
                        let hasmore = if values.len() > 5 { "..." } else { "" };
                        write!(f, "Values:{}{}", valuestr.join(", "), hasmore)
                    }
                    LogicalPlan::Insert(Insert {
                        table_name, input, ..
                    }) => {
                        write!(f, "Insert into {}, values {}", table_name, input.display())
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
                let with_schema = true; // will output schema structure
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
        })
        .unwrap();
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
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::CreateTable(_)
            | LogicalPlan::Insert(_) => Ok(()),
            LogicalPlan::Values(Values { values, .. }) => values.iter().flatten().try_for_each(f),
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
                .map(|e| (*e).clone())
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
                    "cannot create filter with non-boolean type {}, predicate expr:{:?}",
                    predicate_type, predicate
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
        let ans_fields = exprlist_to_fields(all_exprs, &input)?;
        let schema =
            Schema::new_with_metadata(ans_fields, input.output_schema().metadata().clone())?;
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
                let qualifier_str = if let Some(q) = field.qualifier() {
                    q.to_string() + "."
                } else {
                    "".to_string()
                };
                write!(
                    f,
                    "{}{}:{:?}{}",
                    qualifier_str,
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

impl std::fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display_indent().fmt(f)
    }
}

/*
#[cfg(test)]
mod test {
    use super::*;
    use crate::common::schema::Field;
    use crate::common::schema::Schema;
    use crate::common::table_reference::TableReference;
    use crate::common::types::DataType;
    use crate::common::types::DataValue;
    use crate::expr::expr::Sort;
    use crate::expr::logical_plan::builder::LogicalPlanBuilder;
    use crate::expr::utils::{col, min};
    use crate::storage::empty::EmptyTable;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_table_scan(
        name: impl Into<OwnedTableReference>,
        schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        LogicalPlanBuilder::scan(
            name,
            Arc::new(EmptyTable::new(Arc::new(schema.clone()))),
            projection,
        )
    }

    fn employee_schema() -> Schema {
        Schema::new(
            vec![
                Field::new("id", DataType::Int32, true, None),
                Field::new("first_name", DataType::Utf8, true, None),
                Field::new("last_name", DataType::Utf8, true, None),
                Field::new("state", DataType::Utf8, true, None),
                Field::new("salary", DataType::Int32, true, None),
            ],
            HashMap::new(),
        )
    }

    #[test]
    fn build_basic_plan() -> Result<()> {
        let mut builder = test_table_scan("employee", &employee_schema(), Some(vec![0, 3]))?;
        builder = builder
            .filter(col("state").eq(Expr::Literal(DataValue::Utf8(Some("CO".to_string())))))?;
        builder = builder.project(vec![col("id")])?;
        let plan = builder.build().unwrap();
        let _visplan = format!("{plan:?}");
        //println!("{}",_visplan);
        Ok(())
    }

    /// ensure the builder schema is qualifed
    #[test]
    fn builder_schema() -> Result<()> {
        // lower case
        let builder = test_table_scan("employee", &employee_schema(), None).unwrap();
        let expected_schema = Schema::try_from_qualified_schema(
            TableReference::Bare {
                table: "employee".into(),
            },
            &employee_schema(),
        )
        .unwrap();
        assert_eq!(
            &expected_schema,
            builder.build().unwrap().output_schema().as_ref()
        );

        // upper case
        let builder = test_table_scan("EMPLOYEE", &employee_schema(), None).unwrap();
        // when make table reference from string, it will normalize the ident, parse_identifiers_normalized
        let expected_schema = Schema::try_from_qualified_schema(
            TableReference::Bare {
                table: "employee".into(),
            },
            &employee_schema(),
        )
        .unwrap();
        assert_eq!(
            &expected_schema,
            builder.build().unwrap().output_schema().as_ref()
        );

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let mut builder =
            test_table_scan("employee", &employee_schema(), Some(vec![3, 4])).unwrap();
        builder = builder.aggregate(
            vec![col("state")],
            vec![min(col("salary")).alias("min_salary")],
        )?;
        builder = builder
            .project(vec![col("state"), col("min_salary")])?
            .limit(2, Some(10))?;
        let _plan = builder.build()?;
        //println!("{:?}", _plan);
        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let mut builder =
            test_table_scan("employee", &employee_schema(), Some(vec![3, 4])).unwrap();
        builder = builder.sort(vec![
            Expr::Sort(Sort {
                expr: Box::new(col("state")),
                asc: true,
                nulls_first: true,
            }),
            Expr::Sort(Sort {
                expr: Box::new(col("salary")),
                asc: false,
                nulls_first: false,
            }),
        ])?;
        let _plan = builder.build()?;
        //println!("{:?}", _plan);
        Ok(())
    }

    #[test]
    fn plan_builder_join() -> Result<()> {
        let t2 = test_table_scan("t2", &employee_schema(), None)?.build()?;

        // after the join op, the output schema will have both t1.id and t2.id
        // after do projection with Expr::Wildcard, it will only pick t1.id
        // this is implemented in expand_wildcard, which will detect using clause
        let _plan = test_table_scan("t1", &employee_schema(), None)?
            .join_using(t2, JoinType::Inner, vec!["id"])?
            .project(vec![Expr::Wildcard])?
            .build()?;

        //println!("{:?}", _plan);
        Ok(())
    }
}
*/
