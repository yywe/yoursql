use crate::common::record_batch::RecordBatch;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use crate::common::types::DataValue;
use crate::expr::expr::Operator;
use crate::expr::type_coercion::get_result_type;
use anyhow::Context;
use anyhow::{anyhow, Result};
use regex::Regex;
use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::PhysicalExpr;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Literal {
    value: DataValue,
}

impl Literal {
    pub fn new(value: DataValue) -> Self {
        Self { value }
    }
    pub fn value(&self) -> &DataValue {
        &self.value
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PhysicalExpr for Literal {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.get_datatype())
    }
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.value.is_null())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let dim = batch.rows.len();
        Ok(vec![self.value.clone(); dim])
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for Literal {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Column {
    pub name: String,
    pub index: usize,
}

impl Column {
    pub fn new(name: &str, index: usize) -> Self {
        Self {
            name: name.to_owned(),
            index,
        }
    }
    fn bounds_check(&self, input_schema: &Schema) -> Result<()> {
        if self.index < input_schema.fields().len() {
            Ok(())
        } else {
            Err(anyhow!(format!(
                "Physical Column {} refered index {}, but input schema only has {} columns",
                self.name,
                self.index,
                input_schema.fields().len()
            )))
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.index)
    }
}

impl PhysicalExpr for Column {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).data_type().clone())
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).is_nullable())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        self.bounds_check(&batch.schema)?;
        Ok(batch.column(self.index))
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}
impl PartialEq<dyn Any> for Column {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct BinaryExpr {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        Self { left, op, right }
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_child(
            f: &mut std::fmt::Formatter<'_>,
            expr: &dyn PhysicalExpr,
            precedence: u8,
        ) -> std::fmt::Result {
            if let Some(child) = expr.as_any().downcast_ref::<BinaryExpr>() {
                let p = child.op.precedence();
                if p == 0 || p < precedence {
                    write!(f, "({child})")?;
                } else {
                    write!(f, "{child}")?;
                }
            } else {
                write!(f, "{expr}")?;
            }
            Ok(())
        }
        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

impl PhysicalExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        get_result_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.left.clone(), self.right.clone()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(BinaryExpr::new(
            children[0].clone(),
            self.op,
            children[1].clone(),
        )))
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let left_values = self.left.evaluate(batch)?;
        let right_values = self.right.evaluate(batch)?;
        let ret = left_values
            .iter()
            .zip(right_values.iter())
            .map(|(l, r)| match self.op {
                Operator::And => match (l, r) {
                    (DataValue::Boolean(Some(v1)), DataValue::Boolean(Some(v2))) => {
                        Ok(DataValue::Boolean(Some(*v1 && *v2)))
                    }
                    (DataValue::Null, _) | (_, DataValue::Null) => Ok(DataValue::Null),
                    (DataValue::Boolean(None), _) | (_, DataValue::Boolean(None)) => {
                        Ok(DataValue::Boolean(None))
                    }
                    (_, _) => Err(anyhow!(format!("cannot AND {} and {}", l, r))),
                },
                Operator::IsDistinctFrom => Ok(DataValue::Boolean(Some(!l.eq(r)))),
                Operator::IsNotDistinctFrom => Ok(DataValue::Boolean(Some(l.eq(r)))),
                Operator::Or => match (l, r) {
                    (DataValue::Boolean(Some(v1)), DataValue::Boolean(Some(v2))) => {
                        Ok(DataValue::Boolean(Some(*v1 || *v2)))
                    }
                    (DataValue::Boolean(Some(v1)), _) => Ok(DataValue::Boolean(Some(*v1))),
                    (_, DataValue::Boolean(Some(v2))) => Ok(DataValue::Boolean(Some(*v2))),
                    (DataValue::Null, _) | (_, DataValue::Null) => Ok(DataValue::Null),
                    (DataValue::Boolean(None), _) | (_, DataValue::Boolean(None)) => {
                        Ok(DataValue::Boolean(None))
                    }
                    (_, _) => Err(anyhow!(format!("cannot Or {} and {}", l, r))),
                },
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo => eval_binary_value_pair(l, r, self.op),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ret)
    }
}

impl PartialEq<dyn Any> for BinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.left.eq(&x.left) && self.op == x.op && self.right.eq(&x.right))
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct IsNullExpr {
    arg: Arc<dyn PhysicalExpr>,
}
impl IsNullExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl std::fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} IS NULL", self.arg)
    }
}
impl PhysicalExpr for IsNullExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IsNullExpr::new(children[0].clone())))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let arg = self.arg.evaluate(batch)?;
        Ok(arg
            .into_iter()
            .map(|x| DataValue::Boolean(Some(x.is_null())))
            .collect::<Vec<_>>())
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}
impl PartialEq<dyn Any> for IsNullExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct IsNotNullExpr {
    arg: Arc<dyn PhysicalExpr>,
}
impl IsNotNullExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl std::fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} IS NOT NULL", self.arg)
    }
}
impl PhysicalExpr for IsNotNullExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IsNotNullExpr::new(children[0].clone())))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let arg = self.arg.evaluate(batch)?;
        Ok(arg
            .into_iter()
            .map(|x| DataValue::Boolean(Some(!x.is_null())))
            .collect::<Vec<_>>())
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}
impl PartialEq<dyn Any> for IsNotNullExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct NotExpr {
    arg: Arc<dyn PhysicalExpr>,
}
impl NotExpr {
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }
}

impl std::fmt::Display for NotExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NOT {}", self.arg)
    }
}
impl PhysicalExpr for NotExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NotExpr::new(children[0].clone())))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let arg = self.arg.evaluate(batch)?;
        arg.into_iter()
            .map(|x| {
                if x.is_null() {
                    return Ok(DataValue::Boolean(None));
                }
                match x {
                    DataValue::Boolean(Some(b)) => Ok(DataValue::Boolean(Some(!b))),
                    _ => Err(anyhow!(format!("Not cannot be applied to value:{:?}", x))),
                }
            })
            .collect::<Result<Vec<_>>>()
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}
impl PartialEq<dyn Any> for NotExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct LikeExpr {
    negated: bool,
    case_sensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl LikeExpr {
    pub fn new(
        negated: bool,
        case_sensitive: bool,
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            negated,
            case_sensitive,
            expr,
            pattern,
        }
    }
    fn op_name(&self) -> &str {
        match (self.negated, self.case_sensitive) {
            (false, false) => "LIKE",
            (true, false) => "NOT LIKE",
            (false, true) => "ILIKE",
            (true, true) => "NOT ILIKE",
        }
    }
}
impl std::fmt::Display for LikeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.op_name(), self.pattern)
    }
}
impl PhysicalExpr for LikeExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.expr.nullable(input_schema)? || self.pattern.nullable(input_schema)?)
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone(), self.pattern.clone()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(LikeExpr::new(
            self.negated,
            self.case_sensitive,
            children[0].clone(),
            children[1].clone(),
        )))
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<DataValue>> {
        let expr_value = self.expr.evaluate(batch)?;
        let pattern_value = self.pattern.evaluate(batch)?;
        expr_value
            .into_iter()
            .zip(pattern_value.into_iter())
            .map(|(value, pattern)| match (value, pattern) {
                (DataValue::Utf8(Some(v)), DataValue::Utf8(Some(p))) => {
                    return simple_like(&v, &p, self.case_sensitive);
                }
                (v1, v2) => {
                    return Err(anyhow!(format!(
                        "Invalid like for value of {:?} using pattern {:?}",
                        v1, v2
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()
    }
}
impl PartialEq<dyn Any> for LikeExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.negated == x.negated
                    && self.case_sensitive == x.case_sensitive
                    && self.expr.eq(&x.expr)
                    && self.pattern.eq(&x.pattern)
            })
            .unwrap_or(false)
    }
}

/// simple like implemenatation based on regular expression (solution comes from toydb)
/// in the case of apache datafusion, like is based on
/// https://github.com/apache/arrow-rs/blob/master/arrow-string/src/like.rs
///
fn simple_like(value: &String, pattern: &String, case_sensitive: bool) -> Result<DataValue> {
    let (value, pattern) = if case_sensitive == false {
        (value.to_lowercase(), pattern.to_lowercase())
    } else {
        (value.clone(), pattern.clone())
    };
    let regexp = Regex::new(&format!(
        "^{}$",
        regex::escape(&pattern)
            .replace("%", ".*")
            .replace(".*.*", "%")
            .replace("_", ".")
            .replace("..", "_")
    ))?;
    Ok(DataValue::Boolean(Some(regexp.is_match(&value))))
}

/// at least one value must be string
pub fn eval_binary_value_pair(l: &DataValue, r: &DataValue, op: Operator) -> Result<DataValue> {
    let ltype = l.get_datatype();
    let rtype = r.get_datatype();
    if ltype == DataType::Utf8 || rtype == DataType::Utf8 {
        eval_binary_string_value_pair(l, r, op)
    } else {
        eval_binary_numeric_value_pair(l, r, op)
    }
}

/// at least one value must be string
pub fn eval_binary_string_value_pair(
    l: &DataValue,
    r: &DataValue,
    op: Operator,
) -> Result<DataValue> {
    let ltype = l.get_datatype();
    let rtype = r.get_datatype();
    if ltype != DataType::Utf8 && rtype != DataType::Utf8 {
        return Err(anyhow!(format!(
            "expect utf8 (string) type, got {} and {}",
            ltype, rtype
        )));
    }
    Ok(match op {
        Operator::Eq => match (l, r) {
            (DataValue::Utf8(Some(v1)), DataValue::Utf8(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (_, _) => DataValue::Boolean(Some(false)),
        },
        Operator::NotEq => match (l, r) {
            (DataValue::Utf8(Some(v1)), DataValue::Utf8(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (_, _) => DataValue::Boolean(Some(true)),
        },
        _ => {
            return Err(anyhow!(format!(
                "unsupported operation {} for string type",
                op
            )))
        }
    })
}
/// evaluation of expressions involes different types is a pain.
/// for simplicity, here we promote the types as follows when do calculation
/// since in general unsgined can be converted to singed and furthur float
/// so below is the order to do promotion
/// u8->u16->u32->u64->i8->i16->i32->i64->f32->f64.
/// it may be problematic when convert u64->i8. that is the cost to pay here for simplicity
pub fn eval_binary_numeric_value_pair(
    l: &DataValue,
    r: &DataValue,
    op: Operator,
) -> Result<DataValue> {
    if l.get_datatype() == DataType::Null || r.get_datatype() == DataType::Null {
        return Ok(DataValue::Null);
    }
    let one_is_none = match (l, r) {
        (DataValue::Int8(None), _)
        | (_, DataValue::Int8(None))
        | (DataValue::Int16(None), _)
        | (_, DataValue::Int16(None))
        | (DataValue::Int32(None), _)
        | (_, DataValue::Int32(None))
        | (DataValue::Int64(None), _)
        | (_, DataValue::Int64(None))
        | (DataValue::Float32(None), _)
        | (_, DataValue::Float32(None))
        | (DataValue::Float64(None), _)
        | (_, DataValue::Float64(None))
        | (DataValue::UInt8(None), _)
        | (_, DataValue::UInt8(None))
        | (DataValue::UInt16(None), _)
        | (_, DataValue::UInt16(None))
        | (DataValue::UInt32(None), _)
        | (_, DataValue::UInt32(None))
        | (DataValue::UInt64(None), _)
        | (_, DataValue::UInt64(None)) => true,
        _ => false,
    };
    if one_is_none {
        return Ok(DataValue::Null);
    }
    // below both values will not be NULL or None
    let numeric_type_orders = vec![
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    let ltype = l.get_datatype();
    let rtype = r.get_datatype();
    let l_order = numeric_type_orders
        .iter()
        .position(|t| *t == ltype)
        .context(format!("Failed to find type for {}", l))?;
    let r_order = numeric_type_orders
        .iter()
        .position(|t| *t == rtype)
        .context(format!("Failed to find type for {}", r))?;
    let max_order = std::cmp::max(l_order, r_order);

    macro_rules! convert_value_type {
        ($v:expr, $vtype:ident, $t:ty) => {
            Ok(match $v {
                DataValue::UInt8(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::UInt16(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::UInt32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::UInt64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Int8(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Int16(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Int32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Int64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Float32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                DataValue::Float64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                _ => return Err(anyhow!("unsupported type conversion encouted.")),
            })
        };
    }
    let lnew: DataValue = match max_order {
        0 => convert_value_type!(l.clone(), UInt8, u8),
        1 => convert_value_type!(l.clone(), UInt16, u16),
        2 => convert_value_type!(l.clone(), UInt32, u32),
        3 => convert_value_type!(l.clone(), UInt64, u64),
        4 => convert_value_type!(l.clone(), Int8, i8),
        5 => convert_value_type!(l.clone(), Int16, i16),
        6 => convert_value_type!(l.clone(), Int32, i32),
        7 => convert_value_type!(l.clone(), Int64, i64),
        8 => convert_value_type!(l.clone(), Float32, f32),
        9 => convert_value_type!(l.clone(), Float64, f64),
        _ => Err(anyhow!("Invalid order number")),
    }?;
    let rnew: DataValue = match max_order {
        0 => convert_value_type!(r.clone(), UInt8, u8),
        1 => convert_value_type!(r.clone(), UInt16, u16),
        2 => convert_value_type!(r.clone(), UInt32, u32),
        3 => convert_value_type!(r.clone(), UInt64, u64),
        4 => convert_value_type!(r.clone(), Int8, i8),
        5 => convert_value_type!(r.clone(), Int16, i16),
        6 => convert_value_type!(r.clone(), Int32, i32),
        7 => convert_value_type!(r.clone(), Int64, i64),
        8 => convert_value_type!(r.clone(), Float32, f32),
        9 => convert_value_type!(r.clone(), Float64, f64),
        _ => Err(anyhow!("Invalid order number")),
    }?;
    if lnew.get_datatype() != rnew.get_datatype() {
        return Err(anyhow!(format!(
            "type mismatch: left {:?}, right {:?}",
            lnew, rnew
        )));
    }
    Ok(match op {
        Operator::Eq => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 == v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for Equal operation")),
        },
        Operator::NotEq => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 != v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for not Equal operation")),
        },
        Operator::GtEq => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 >= v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for >= operation")),
        },
        Operator::Gt => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 > v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for > operation")),
        },
        Operator::Lt => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 < v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for < operation")),
        },
        Operator::LtEq => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Boolean(Some(v1 <= v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for <= operation")),
        },
        Operator::Plus => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::UInt8(Some(v1 + v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::UInt16(Some(v1 + v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::UInt32(Some(v1 + v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::UInt64(Some(v1 + v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Int8(Some(v1 + v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Int16(Some(v1 + v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Int32(Some(v1 + v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Int64(Some(v1 + v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Float32(Some(v1 + v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Float64(Some(v1 + v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for + operation")),
        },
        Operator::Minus => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::UInt8(Some(v1 - v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::UInt16(Some(v1 - v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::UInt32(Some(v1 - v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::UInt64(Some(v1 - v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Int8(Some(v1 - v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Int16(Some(v1 - v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Int32(Some(v1 - v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Int64(Some(v1 - v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Float32(Some(v1 - v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Float64(Some(v1 - v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for - operation")),
        },
        Operator::Multiply => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::UInt8(Some(v1 * v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::UInt16(Some(v1 * v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::UInt32(Some(v1 * v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::UInt64(Some(v1 * v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Int8(Some(v1 * v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Int16(Some(v1 * v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Int32(Some(v1 * v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Int64(Some(v1 * v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Float32(Some(v1 * v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Float64(Some(v1 * v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for * operation")),
        },
        Operator::Divide => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::UInt8(Some(v1 / v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::UInt16(Some(v1 / v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::UInt32(Some(v1 / v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::UInt64(Some(v1 / v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Int8(Some(v1 / v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Int16(Some(v1 / v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Int32(Some(v1 / v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                if v2 == 0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Int64(Some(v1 / v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                if v2 == 0.0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Float32(Some(v1 / v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                if v2 == 0.0 {
                    return Err(anyhow!("cannot divide by 0"));
                }
                DataValue::Float64(Some(v1 / v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for * operation")),
        },
        Operator::Modulo => match (lnew, rnew) {
            (DataValue::UInt8(Some(v1)), DataValue::UInt8(Some(v2))) => {
                DataValue::UInt8(Some(v1 % v2))
            }
            (DataValue::UInt16(Some(v1)), DataValue::UInt16(Some(v2))) => {
                DataValue::UInt16(Some(v1 % v2))
            }
            (DataValue::UInt32(Some(v1)), DataValue::UInt32(Some(v2))) => {
                DataValue::UInt32(Some(v1 % v2))
            }
            (DataValue::UInt64(Some(v1)), DataValue::UInt64(Some(v2))) => {
                DataValue::UInt64(Some(v1 % v2))
            }
            (DataValue::Int8(Some(v1)), DataValue::Int8(Some(v2))) => {
                DataValue::Int8(Some(v1 % v2))
            }
            (DataValue::Int16(Some(v1)), DataValue::Int16(Some(v2))) => {
                DataValue::Int16(Some(v1 % v2))
            }
            (DataValue::Int32(Some(v1)), DataValue::Int32(Some(v2))) => {
                DataValue::Int32(Some(v1 % v2))
            }
            (DataValue::Int64(Some(v1)), DataValue::Int64(Some(v2))) => {
                DataValue::Int64(Some(v1 % v2))
            }
            (DataValue::Float32(Some(v1)), DataValue::Float32(Some(v2))) => {
                DataValue::Float32(Some(v1 % v2))
            }
            (DataValue::Float64(Some(v1)), DataValue::Float64(Some(v2))) => {
                DataValue::Float64(Some(v1 % v2))
            }
            (_, _) => return Err(anyhow!("unsupported data type for modulo operation")),
        },
        _ => {
            return Err(anyhow!(format!(
                "unsupported numrical binary operator {}",
                op
            )))
        }
    })
}

pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::schema::Field;
    use std::collections::HashMap;
    fn get_test_record_batch() -> RecordBatch {
        let schema = Schema::new(
            vec![
                Field::new("id", DataType::Int64, false, None),
                Field::new("name", DataType::Utf8, false, None),
                Field::new("age", DataType::Int8, false, None),
                Field::new("address", DataType::Utf8, false, None),
            ],
            HashMap::new(),
        );
        let schema_ref = Arc::new(schema);
        let row_batch = vec![
            vec![
                DataValue::Int64(Some(10)),
                DataValue::Utf8(Some("John".into())),
                DataValue::Int8(Some(20)),
                DataValue::Utf8(Some("100 bay street".into())),
            ],
            vec![
                DataValue::Int64(Some(20)),
                DataValue::Utf8(Some("Andy".into())),
                DataValue::Int8(Some(21)),
                DataValue::Utf8(Some("121 hunter street".into())),
            ],
        ];
        RecordBatch {
            schema: schema_ref.clone(),
            rows: row_batch.clone(),
        }
    }
    #[test]
    fn test_literal() -> Result<()> {
        let mylit = Literal::new(DataValue::Int32(Some(32)));
        let test_batch = get_test_record_batch();
        let ans = mylit.evaluate(&test_batch)?;
        assert_eq!(ans.len(), 2);
        assert_eq!(ans[0], DataValue::Int32(Some(32)));
        Ok(())
    }

    #[test]
    fn test_binary_expr() -> Result<()> {
        let test_batch = get_test_record_batch();
        let expr = BinaryExpr::new(
            Arc::new(Column {
                name: "id".into(),
                index: 0,
            }),
            Operator::Lt,
            Arc::new(Literal::new(DataValue::Int64(Some(15)))),
        );
        let ans = expr.evaluate(&test_batch)?;
        assert_eq!(
            ans,
            vec![
                DataValue::Boolean(Some(true)),
                DataValue::Boolean(Some(false))
            ]
        );

        let expr = BinaryExpr::new(
            Arc::new(Column {
                name: "id".into(),
                index: 0,
            }),
            Operator::Plus,
            Arc::new(Literal::new(DataValue::Int64(Some(15)))),
        );
        let ans = expr.evaluate(&test_batch)?;
        assert_eq!(
            ans,
            vec![DataValue::Int64(Some(25)), DataValue::Int64(Some(35))]
        );

        let expr = BinaryExpr::new(
            Arc::new(Column {
                name: "id".into(),
                index: 0,
            }),
            Operator::Multiply,
            Arc::new(Literal::new(DataValue::Int64(Some(2)))),
        );
        let ans = expr.evaluate(&test_batch)?;
        assert_eq!(
            ans,
            vec![DataValue::Int64(Some(20)), DataValue::Int64(Some(40))]
        );

        let expr = BinaryExpr::new(
            Arc::new(Column {
                name: "name".into(),
                index: 1,
            }),
            Operator::Eq,
            Arc::new(Literal::new(DataValue::Utf8(Some("Andy".to_owned())))),
        );
        let ans = expr.evaluate(&test_batch)?;
        assert_eq!(
            ans,
            vec![
                DataValue::Boolean(Some(false)),
                DataValue::Boolean(Some(true))
            ]
        );

        Ok(())
    }
    #[test]
    fn test_macro() -> Result<()> {
        macro_rules! convert_data_type {
            ($v:expr, $vtype:ident, $t:ty) => {
                Ok(match $v {
                    DataValue::UInt8(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::UInt16(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::UInt32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::UInt64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Int8(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Int16(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Int32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Int64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Float32(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    DataValue::Float64(Some(v)) => DataValue::$vtype(Some(v as $t)),
                    _ => return Err(anyhow!("unsupported type conversion encouted.")),
                })
            };
        }
        let t: Result<DataValue> = convert_data_type!(DataValue::Float64(Some(3.0)), UInt8, u8);
        assert_eq!(DataValue::UInt8(Some(3)), t?);
        Ok(())
    }
}
