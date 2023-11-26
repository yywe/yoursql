use super::physical_expr::down_cast_any_ref;
use crate::common::schema::Field;
use crate::common::types::DataType;
use crate::expr::expr::AggregateFunctionType;
use crate::physical_expr::accumulator::{
    Accumulator, CountAccumulator, MaxAccumulator, MinAccumulator, SumAccumulator,AvgAccumulator
};
use crate::physical_expr::PhysicalExpr;
use anyhow::{Result, anyhow};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use crate::common::schema::Schema;

pub trait AggregateExpr: Send + Sync + Debug + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;
    fn field(&self) -> Result<Field>;
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }
}

#[derive(Debug, Clone)]
pub struct Sum {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Sum {
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type: data_type,
            expr: expr,
            nullable: true,
        }
    }
}

impl AggregateExpr for Sum {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
            None,
        ))
    }
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::try_new(&self.data_type)?))
    }
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Sum {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct Max {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Max {
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type: data_type,
            expr: expr,
            nullable: true,
        }
    }
}

impl AggregateExpr for Max {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
            None,
        ))
    }
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(&self.data_type)?))
    }
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Max {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct Min {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Min {
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type: data_type,
            expr: expr,
            nullable: true,
        }
    }
}

impl AggregateExpr for Min {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
            None,
        ))
    }
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(&self.data_type)?))
    }
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Min {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

// count aggregation
// return the amount of non-null values of the given expressions
#[derive(Debug, Clone)]
pub struct Count {
    name: String,
    data_type: DataType,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    nullable: bool,
}

impl Count {
    pub fn new(
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: data_type,
            exprs: exprs,
            nullable: true,
        }
    }
}

impl AggregateExpr for Count {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
            None,
        ))
    }
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()?))
    }
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Count {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.exprs.len() == x.exprs.len()
                    && self
                        .exprs
                        .iter()
                        .zip(x.exprs.iter())
                        .all(|(expr1, expr2)| expr1.eq(expr2))
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct Avg {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Avg {
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type: data_type,
            expr: expr,
            nullable: true,
        }
    }
}

impl AggregateExpr for Avg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
            None,
        ))
    }
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(&self.data_type)?))
    }
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Avg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

pub fn create_aggregate_expr_impl(fun: &AggregateFunctionType, distinct: bool, input_phy_exprs: &[Arc<dyn PhysicalExpr>], input_schema: &Schema, name: impl Into<String>) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    let input_phy_types = input_phy_exprs.iter().map(|e|e.data_type(input_schema)).collect::<Result<Vec<_>>>()?;
    let input_phy_exprs = input_phy_exprs.to_vec();
    let rt_type = return_type(fun, &input_phy_types)?;
    if distinct == true {
        return Err(anyhow!("Unsupported distinct in aggregate expression"));
    }
    Ok(match fun {
        AggregateFunctionType::Avg => Arc::new(Avg::new(input_phy_exprs[0].clone(), name, rt_type)),
        AggregateFunctionType::Count => Arc::new(Count::new(input_phy_exprs, name, rt_type)),
        AggregateFunctionType::Sum => Arc::new(Sum::new(input_phy_exprs[0].clone(), name, rt_type)),
        AggregateFunctionType::Max => Arc::new(Max::new(input_phy_exprs[0].clone(), name, rt_type)),
        AggregateFunctionType::Min => Arc::new(Min::new(input_phy_exprs[0].clone(), name, rt_type)),
    })
}

///very simple implementation here, should consider input type and use max type
///todo
pub fn return_type(fun: &AggregateFunctionType, _inpu_expr_types: &[DataType]) -> Result<DataType> {
    Ok(match fun {
        AggregateFunctionType::Avg => DataType::Float64,
        AggregateFunctionType::Count => DataType::Int64,
        AggregateFunctionType::Sum => DataType::Float64,
        AggregateFunctionType::Max => DataType::Float64,
        AggregateFunctionType::Min => DataType::Float64,
    })
}