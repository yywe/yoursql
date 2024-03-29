use std::time::{Duration, UNIX_EPOCH};

use anyhow::anyhow;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    Binary(Option<Vec<u8>>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Time stored as a signed 32bit int as seconds since midnight
    Time32Second(Option<i32>),
    /// Time stored as a signed 32bit int as milliseconds since midnight
    Time32Millisecond(Option<i32>),
    /// Time stored as a signed 64bit int as microseconds since midnight
    Time64Microsecond(Option<i64>),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64Nanosecond(Option<i64>),
}

/// note Eq cannot be derived due to f32, f64, only PartialEq can
impl Eq for DataValue {}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {
        match $EXPR {
            Some(e) => write!($F, "{e}"),
            None => write!($F, "NULL"),
        }
    };
}

impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => write!(f, "NULL")?,
            DataValue::Boolean(e) => format_option!(f, e)?,
            DataValue::Float32(e) => format_option!(f, e)?,
            DataValue::Float64(e) => format_option!(f, e)?,
            DataValue::Int8(e) => format_option!(f, e)?,
            DataValue::Int16(e) => format_option!(f, e)?,
            DataValue::Int32(e) => format_option!(f, e)?,
            DataValue::Int64(e) => format_option!(f, e)?,
            DataValue::UInt8(e) => format_option!(f, e)?,
            DataValue::UInt16(e) => format_option!(f, e)?,
            DataValue::UInt32(e) => format_option!(f, e)?,
            DataValue::UInt64(e) => format_option!(f, e)?,
            DataValue::Utf8(e) => format_option!(f, e)?,
            DataValue::Binary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            DataValue::Date32(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d").to_string())?;
                }
                None => write!(f, "NULL")?,
            },
            DataValue::Date64(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d").to_string())?
                }
                None => write!(f, "NULL")?,
            },
            DataValue::Time32Second(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_secs(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string())?
                }
                None => write!(f, "NULL")?,
            },
            DataValue::Time32Millisecond(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_millis(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string())?
                }
                None => write!(f, "NULL")?,
            },

            DataValue::Time64Microsecond(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_micros(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string())?
                }
                None => write!(f, "NULL")?,
            },
            DataValue::Time64Nanosecond(e) => match e {
                Some(d) => {
                    let t = UNIX_EPOCH + Duration::from_nanos(*d as u64);
                    let datetime = DateTime::<Utc>::from(t);
                    write!(f, "{}", datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string())?
                }
                None => write!(f, "NULL")?,
            },
        }
        Ok(())
    }
}

impl std::fmt::Debug for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(_) => write!(f, "Boolean({self})"),
            DataValue::Float32(_) => write!(f, "Float32({self})"),
            DataValue::Float64(_) => write!(f, "Float64({self})"),
            DataValue::Int8(_) => write!(f, "Int8({self})"),
            DataValue::Int16(_) => write!(f, "Int16({self})"),
            DataValue::Int32(_) => write!(f, "Int32({self})"),
            DataValue::Int64(_) => write!(f, "Int64({self})"),
            DataValue::UInt8(_) => write!(f, "Uint8({self})"),
            DataValue::UInt16(_) => write!(f, "Uint16({self})"),
            DataValue::UInt32(_) => write!(f, "Uint32({self})"),
            DataValue::UInt64(_) => write!(f, "Uint64({self})"),
            DataValue::Utf8(_) => write!(f, "Utf8({self})"),
            DataValue::Binary(e) => match e {
                Some(_) => write!(f, "Binary(\"{self}\")"),
                None => write!(f, "Binary{self}"),
            },
            DataValue::Date32(_) => write!(f, "Date32(\"{self}\")"),
            DataValue::Date64(_) => write!(f, "Date64(\"{self}\")"),
            DataValue::Time32Second(_) => write!(f, "Time32Second(\"{self}\")"),
            DataValue::Time32Millisecond(_) => write!(f, "Time32Millisecond(\"{self}\")"),

            DataValue::Time64Microsecond(_) => write!(f, "Time64Microsecond(\"{self}\")"),

            DataValue::Time64Nanosecond(_) => write!(f, "Time64Nanosecond(\"{self}\")"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
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

impl TryFrom<&DataType> for DataValue {
    type Error = anyhow::Error;
    fn try_from(datatype: &DataType) -> Result<Self, Self::Error> {
        Ok(match datatype {
            DataType::Boolean => DataValue::Boolean(None),
            DataType::Null => DataValue::Null,
            DataType::Float32 => DataValue::Float32(None),
            DataType::Float64 => DataValue::Float64(None),
            DataType::Int8 => DataValue::Int8(None),
            DataType::Int16 => DataValue::Int16(None),
            DataType::Int32 => DataValue::Int32(None),
            DataType::Int64 => DataValue::Int64(None),
            DataType::UInt8 => DataValue::UInt8(None),
            DataType::UInt16 => DataValue::UInt16(None),
            DataType::UInt32 => DataValue::UInt32(None),
            DataType::UInt64 => DataValue::UInt64(None),
            DataType::Utf8 => DataValue::Utf8(None),
            DataType::Binary => DataValue::Binary(None),
            DataType::Date32 => DataValue::Date32(None),
            DataType::Date64 => DataValue::Date64(None),
            DataType::Time32Second => DataValue::Time32Second(None),
            DataType::Time32Millisecond => DataValue::Time32Millisecond(None),
            DataType::Time64Microsecond => DataValue::Time64Microsecond(None),
            DataType::Time64Nanosecond => DataValue::Time64Nanosecond(None),
        })
    }
}

impl DataValue {
    pub fn get_datatype(&self) -> DataType {
        match self {
            DataValue::Null => DataType::Null,
            DataValue::Boolean(_) => DataType::Boolean,
            DataValue::Float32(_) => DataType::Float32,
            DataValue::Float64(_) => DataType::Float64,
            DataValue::Int8(_) => DataType::Int8,
            DataValue::Int16(_) => DataType::Int16,
            DataValue::Int32(_) => DataType::Int32,
            DataValue::Int64(_) => DataType::Int64,
            DataValue::UInt8(_) => DataType::UInt8,
            DataValue::UInt16(_) => DataType::UInt16,
            DataValue::UInt32(_) => DataType::UInt32,
            DataValue::UInt64(_) => DataType::UInt64,
            DataValue::Utf8(_) => DataType::Utf8,
            DataValue::Binary(_) => DataType::Binary,
            DataValue::Date32(_) => DataType::Date32,
            DataValue::Date64(_) => DataType::Date64,
            DataValue::Time32Second(_) => DataType::Time32Second,
            DataValue::Time32Millisecond(_) => DataType::Time32Millisecond,
            DataValue::Time64Microsecond(_) => DataType::Time64Microsecond,
            DataValue::Time64Nanosecond(_) => DataType::Time64Nanosecond,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            DataValue::Null => true,
            DataValue::Boolean(v) => v.is_none(),
            DataValue::Float32(v) => v.is_none(),
            DataValue::Float64(v) => v.is_none(),
            DataValue::Int8(v) => v.is_none(),
            DataValue::Int16(v) => v.is_none(),
            DataValue::Int32(v) => v.is_none(),
            DataValue::Int64(v) => v.is_none(),
            DataValue::UInt8(v) => v.is_none(),
            DataValue::UInt16(v) => v.is_none(),
            DataValue::UInt32(v) => v.is_none(),
            DataValue::UInt64(v) => v.is_none(),
            DataValue::Utf8(v) => v.is_none(),
            DataValue::Binary(v) => v.is_none(),
            DataValue::Date32(v) => v.is_none(),
            DataValue::Date64(v) => v.is_none(),
            DataValue::Time32Second(v) => v.is_none(),
            DataValue::Time32Millisecond(v) => v.is_none(),
            DataValue::Time64Microsecond(v) => v.is_none(),
            DataValue::Time64Nanosecond(v) => v.is_none(),
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            DataValue::Null => false,
            DataValue::Boolean(v) => v.is_none(),
            DataValue::Float32(v) => v.is_none(),
            DataValue::Float64(v) => v.is_none(),
            DataValue::Int8(v) => v.is_none(),
            DataValue::Int16(v) => v.is_none(),
            DataValue::Int32(v) => v.is_none(),
            DataValue::Int64(v) => v.is_none(),
            DataValue::UInt8(v) => v.is_none(),
            DataValue::UInt16(v) => v.is_none(),
            DataValue::UInt32(v) => v.is_none(),
            DataValue::UInt64(v) => v.is_none(),
            DataValue::Utf8(v) => v.is_none(),
            DataValue::Binary(v) => v.is_none(),
            DataValue::Date32(v) => v.is_none(),
            DataValue::Date64(v) => v.is_none(),
            DataValue::Time32Second(v) => v.is_none(),
            DataValue::Time32Millisecond(v) => v.is_none(),
            DataValue::Time64Microsecond(v) => v.is_none(),
            DataValue::Time64Nanosecond(v) => v.is_none(),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// wrapper to hash f32, f64
/// in rust, you cannot impl foreign trait for foreign type
/// need to use own type. the hash for f32 and f64 is to recognize
/// its binary represenation, i.e, build u32 from f32 bits, that is used
/// as its hash
struct Fl<T>(T);
macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(
            impl std::hash::Hash for Fl<$t> {
                #[inline]
                fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                    state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
                }
            }
        )+
    };
}

hash_float_value!((f64, u64), (f32, u32));

impl std::hash::Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use DataValue::*;
        match self {
            Null => 1.hash(state),
            Boolean(v) => v.hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Binary(v) => v.hash(state),
            Time32Millisecond(v) => v.hash(state),
            Time32Second(v) => v.hash(state),
            Time64Microsecond(v) => v.hash(state),
            Time64Nanosecond(v) => v.hash(state),
        }
    }
}

impl TryFrom<&sqlparser::ast::DataType> for DataType {
    type Error = anyhow::Error;
    fn try_from(value: &sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        use sqlparser::ast::DataType::*;
        match value {
            Boolean => Ok(DataType::Boolean),
            Char(_) | Nvarchar(_) | Varchar(_) | Text | String => Ok(DataType::Utf8),
            Real => Ok(DataType::Float32),
            Double | Numeric(_) => Ok(DataType::Float64),
            TinyInt(_) => Ok(DataType::Int8),
            UnsignedTinyInt(_) => Ok(DataType::UInt8),
            SmallInt(_) => Ok(DataType::Int16),
            UnsignedSmallInt(_) => Ok(DataType::UInt16),
            Int(_) => Ok(DataType::Int32),
            UnsignedInt(_) => Ok(DataType::UInt32),
            BigInt(_) => Ok(DataType::Int64),
            UnsignedBigInt(_) => Ok(DataType::UInt64),
            Binary(_) => Ok(DataType::Binary),
            Date => Ok(DataType::Date64),
            Datetime(_) | Time(..) | Timestamp(..) => Ok(DataType::Time32Millisecond),
            _ => Err(anyhow!(format!("Cannot convert datatype {}", value))),
        }
    }
}
