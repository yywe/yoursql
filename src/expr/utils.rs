use crate::common::column::Column;
use crate::common::schema::{Schema, SchemaRef};
use crate::common::table_reference::TableReference;
use crate::common::tree_node::{VisitRecursion, TreeNodeRewriter};
use crate::{common::tree_node::TreeNode, expr::expr::Expr};
use anyhow::{Result,anyhow};
use std::collections::HashSet;
use crate::expr::logical_plan::LogicalPlan;
use crate::common::schema::Field;
use crate::expr::logical_plan::{Projection,Filter, SubqueryAlias,Aggregate, Sort,Join,Limit, TableScan};
use std::sync::Arc;
use crate::common::tree_node::RewriteRecursion;
use crate::expr::expr::{BinaryExpr, Operator};

use super::expr::{AggregateFunction, AggregateFunctionType};
use super::logical_plan::builder::build_join_schema;
use crate::expr::logical_plan::builder::LogicalPlanBuilder;


pub fn inspect_expr_pre<F>(expr: &Expr, mut f: F) -> Result<()>
where
    F: FnMut(&Expr) -> Result<()>,
{
    let mut err = Ok(());
    expr.apply(&mut |expr| {
        if let Err(e) = f(expr) {
            err = Err(e);
            Ok(VisitRecursion::Stop)
        } else {
            Ok(VisitRecursion::Continue)
        }
    })
    .expect("cannot happen here");
    err
}

pub fn find_columns_referred_by_expr(e: &Expr) -> Vec<Column> {
    let mut exprs = vec![];
    inspect_expr_pre(e, |expr| {
        if let Expr::Column(c) = expr {
            exprs.push(c.clone())
        }
        Ok(())
    })
    .expect("cannot happen");
    exprs
}

pub fn find_column_exprs(exprs: &[Expr]) -> Vec<Expr> {
    exprs
        .iter()
        .flat_map(find_columns_referred_by_expr)
        .map(Expr::Column)
        .collect()
}

pub fn agg_cols(agg: &Aggregate) -> Vec<Column> {
    agg.aggr_expr
        .iter()
        .chain(&agg.group_expr)
        .flat_map(find_columns_referred_by_expr)
        .collect()
}

pub fn expand_wildcard(schema: &Schema, plan: &LogicalPlan)-> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let columns_to_skip = using_columns.into_iter().flat_map(
        |cols|{
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
    Ok(get_columns_with_skipped(schema, columns_to_skip))
}

pub fn expand_qualified_wildcard(qualifier: &str, schema: &Schema)->Result<Vec<Expr>> {
    let qualifier = TableReference::from(qualifier);
    let qualified_fields: Vec<Field> = schema.fields_with_qualifed(&qualifier)
        .into_iter().cloned().collect();
    if qualified_fields.is_empty() {
        return Err(anyhow!("Invalid qualifier {}", qualifier))
    }
    let qualified_schema = Schema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    Ok(get_columns_with_skipped(&qualified_schema, HashSet::new()))
}

/// convert all fields in schema into Expr::Column (except skipped) and return Vec<Expr>
fn get_columns_with_skipped(schema: &Schema, columns_to_skip: HashSet<Column>) -> Vec<Expr> {
    if columns_to_skip.is_empty() {
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
    }
}

/// replace old logical plan base on new expr and inputs
/// the expr and inputs has the same structure like in the old logical plan
pub fn from_plan(plan: &LogicalPlan, expr: &[Expr], inputs: &[LogicalPlan]) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(_) => Ok(LogicalPlan::Projection(Projection::try_new(expr.to_vec(), Arc::new(inputs[0].clone()))?)),
        LogicalPlan::Filter{..}=>{
            assert_eq!(1, expr.len());
            let predicate = expr[0].clone();
            struct RemoveAliases {}
            impl TreeNodeRewriter for RemoveAliases {
                type N = Expr;
                fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
                    match expr {
                        Expr::Alias(_,_) => Ok(RewriteRecursion::Mutate),
                        _=>Ok(RewriteRecursion::Continue)
                    }   
                }
                fn mutate(&mut self, expr: Expr) -> Result<Expr> {
                    Ok(expr.unalias())
                }
            }
            let mut remove_aliases = RemoveAliases {};
            let predicate = predicate.rewrite(&mut remove_aliases)?;
            Ok(LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(inputs[0].clone()))?))
        }
        LogicalPlan::Aggregate(Aggregate{group_expr, schema,..})=>{
            Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(Arc::new(inputs[0].clone()), expr[0..group_expr.len()].to_vec(), expr[group_expr.len()..].to_vec(),schema.clone())?))
        }
        LogicalPlan::Sort(Sort{fetch,..})=>Ok(LogicalPlan::Sort(Sort { expr: expr.to_vec(), input: Arc::new(inputs[0].clone()), fetch: *fetch })),
        LogicalPlan::Join(Join{
            join_type,
            join_constraint,
            on,
            null_equals_null,
            ..
        })=>{
            let schema = build_join_schema(inputs[0].output_schema().as_ref(), inputs[1].output_schema().as_ref(), join_type)?;
            let equi_expr_count = on.len();
            assert!(expr.len() >= equi_expr_count);
            let new_on: Vec<(Expr, Expr)> = expr.iter().take(equi_expr_count).map(|equi_expr|{
                let unalias_expr = equi_expr.clone().unalias();
                if let Expr::BinaryExpr(BinaryExpr{left, op: Operator::Eq, right}) = unalias_expr {
                    Ok((*left, *right))
                }else{
                    return Err(anyhow!(format!("invalid expression found when construct join conditions, expr:{equi_expr}")))
                }   
             }).collect::<Result<Vec<(Expr, Expr)>>>()?;
             let filter_expr = (expr.len() > equi_expr_count).then(||expr[expr.len()-1].clone());
             Ok(LogicalPlan::Join(Join { left: Arc::new(inputs[0].clone()), right: Arc::new(inputs[1].clone()), on: new_on, filter: filter_expr, join_type: *join_type, join_constraint: *join_constraint, schema: SchemaRef::new(schema), null_equals_null: *null_equals_null }))
        }
        LogicalPlan::CrossJoin(_) => {
            let left = inputs[0].clone();
            let right = inputs[1].clone();
            LogicalPlanBuilder::from(left).cross_join(right)?.build()
        }
        LogicalPlan::Limit(Limit{skip, fetch,..}) => Ok(LogicalPlan::Limit(
            super::logical_plan::Limit { skip: *skip, fetch: *fetch, input: Arc::new(inputs[0].clone()) }
        )),
        LogicalPlan::TableScan(ts) => {
            Ok(LogicalPlan::TableScan(TableScan{
                filters: expr.to_vec(),
                ..ts.clone()
            }))
        }
        LogicalPlan::EmptyRelation(_)=>Ok(plan.clone()),
        LogicalPlan::SubqueryAlias(SubqueryAlias{alias,..})=>{
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(inputs[0].clone(), alias.clone())?))
        }
    }
}


/// convert an expression into Column expression (so will save the effort to evalute the expression)
/// if the the expression is already in input
/// example:
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))] #not sum
/// .project(vec![col("c1")], sum(col("c2")))
/// 
/// 
/// 
/// will be write to
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))]
/// .project(vec![col("c1")], col("SUM(c2)"))) 
/// note here "SUM(c2)" is a column name in the input node
/// 
/// if errors happend, then just return original expr
pub fn columnize_expr(e: Expr, input_schema: &Schema) -> Expr {
    match e {
        Expr::Column(_) => e,
        Expr::Alias(inner_expr, name)=> {
            columnize_expr(*inner_expr, input_schema).alias(name)
        }
        _=>match e.display_name() {
            Ok(name)=>match input_schema.field_with_unqualified_name(&name){
                Ok(field)=>Expr::Column(field.qualified_column()),
                Err(_)=>e,
            }
            Err(_)=>e,
        }
    }
}

pub fn col(ident: impl Into<Column>) -> Expr {
    Expr::Column(ident.into())
}

pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction{fun: AggregateFunctionType::Min, args: vec![expr], distinct: false, filter: None, order_by: None }
    )
}

pub fn extract_columns_from_expr(expr: &Expr, acc: &mut HashSet<Column>) -> Result<()>{
    inspect_expr_pre(expr, |expr|{
        match expr {
            Expr::Column(c)=>{
                acc.insert(c.clone());
            }
            _=>{}
        }
        Ok(())
    })
}

pub fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where F: Fn(&Expr)->bool {
    let mut exprs = vec![];
    expr.apply(&mut |expr|{
        if test_fn(expr){
            if !(exprs.contains(expr)) {
                exprs.push(expr.clone())
            }
            // once find  match stop recursing down the curernt node, why???
            return Ok(VisitRecursion::Skip);
        }
        Ok(VisitRecursion::Continue)
    }).expect("no way to return error during recursion");
    exprs
}


pub fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where F: Fn(&Expr)->bool {
    exprs.iter().flat_map(|expr|find_exprs_in_expr(expr,test_fn)).fold(vec![], |mut acc, expr|{
        if !acc.contains(&expr){
            acc.push(expr)
        }
        acc
    })
}

pub fn find_aggregate_exprs(exprs: &[Expr])-> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr|{
        matches!(nested_expr, Expr::AggregateFunction{..})
    })
}