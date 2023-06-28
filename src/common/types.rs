use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataValue {
    Null,
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8(String),
    Binary(Vec<u8>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(i32),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(i64),
    /// Time stored as a signed 32bit int as seconds since midnight
    Time32Second(i32),
    /// Time stored as a signed 32bit int as milliseconds since midnight
    Time32Millisecond(i32),
    /// Time stored as a signed 64bit int as microseconds since midnight
    Time64Microsecond(i64),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64Nanosecond(i64),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataType {
    Null,
    Boolean,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
    Binary,
    Date32,
    Date64,
    Time32Second,
    Time32Millisecond,
    Time64Microsecond,
    Time64Nanosecond,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: HashMap<String, String>,
}
pub type FieldRef = Arc<Field>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Fields(Arc<[FieldRef]>);


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TableDef{
    pub fields: Fields,
    pub metadata: HashMap<String, String>,
}

pub type TableRef = Arc<TableDef>;


