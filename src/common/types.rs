use anyhow::Context;
use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use super::table_reference::OwnedTableReference;

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

impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                DataValue::Null => "NULL".to_string(),
                DataValue::Boolean(b) => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
                DataValue::Float32(n) => n.to_string(),
                DataValue::Float64(n) => n.to_string(),
                DataValue::Int8(n) => n.to_string(),
                DataValue::Int16(n) => n.to_string(),
                DataValue::Int32(n) => n.to_string(),
                DataValue::Int64(n) => n.to_string(),
                DataValue::UInt8(n) => n.to_string(),
                DataValue::UInt16(n) => n.to_string(),
                DataValue::UInt32(n) => n.to_string(),
                DataValue::UInt64(n) => n.to_string(),
                DataValue::Utf8(s) => s.to_owned(),
                DataValue::Binary(b) => b
                    .iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                DataValue::Date32(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d").to_string()
                }
                DataValue::Date64(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d").to_string()
                }
                DataValue::Time32Second(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
                DataValue::Time32Millisecond(d) => {
                    let t = UNIX_EPOCH + Duration::from_millis(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
                DataValue::Time64Microsecond(d) => {
                    let t = UNIX_EPOCH + Duration::from_micros(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
                DataValue::Time64Nanosecond(d) => {
                    let t = UNIX_EPOCH + Duration::from_nanos(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                }
            }
            .as_ref(),
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
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
