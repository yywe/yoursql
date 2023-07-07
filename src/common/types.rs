use anyhow::Context;
use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

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

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: HashMap<String, String>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::default(),
        }
    }
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

pub type FieldRef = Arc<Field>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Fields(Arc<[FieldRef]>);

impl Fields {
    pub fn find(&self, name: &str) -> Option<(usize, &FieldRef)> {
        self.0.iter().enumerate().find(|(_, f)| f.name() == name)
    }
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
    pub fn project(&self, indices: &[usize]) -> Result<Fields> {
        let new_fields = indices
            .iter()
            .map(|i| {
                self.0.get(*i).cloned().context(format!(
                    "project index {} out of bounds, max {}",
                    i,
                    self.0.len()
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Fields(new_fields.into()))
    }

    pub fn equal(&self, other: &Fields) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b) || *(*a) == *(*b))
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(value: Vec<FieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(value: Vec<Field>) -> Self {
        value.into_iter().collect()
    }
}
impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}
impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::empty()
    }
}

impl Deref for Fields {
    type Target = [FieldRef];
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TableDef {
    pub fields: Fields,
    pub metadata: HashMap<String, String>,
}

pub type TableRef = Arc<TableDef>;

impl TableDef {
    pub fn empty() -> Self {
        Self {
            fields: Default::default(),
            metadata: HashMap::new(),
        }
    }
    pub fn new(fields: impl Into<Fields>, metadata: HashMap<String, String>) -> Self {
        Self {
            fields: fields.into(),
            metadata: metadata,
        }
    }
    pub fn fields(&self) -> &Fields {
        &self.fields
    }
    pub fn project(&self, indices: &[usize]) -> Result<TableDef> {
        Ok(Self {
            fields: self.fields.project(indices)?,
            metadata: self.metadata.clone(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_field_equal() {
        let f1 = Field::new("name_a", DataType::Binary, false);
        let f2 = Field::new("name_a", DataType::Binary, false);
        let f3 = Field::new("name_b", DataType::Binary, false);
        let f4 = Field::new("name_b", DataType::Binary, true);
        assert_eq!(f1, f2);
        assert_ne!(f1, f3);
        assert_ne!(f3, f4);
    }

    #[test]
    fn test_fields_equal() {
        let f1 = Field::new("name_a", DataType::Int16, false);
        let f2 = Field::new("name_b", DataType::Binary, false);
        let fields_1: Fields = vec![f1, f2].into();
        let f3 = Field::new("name_a", DataType::Int16, false);
        let f4 = Field::new("name_b", DataType::Binary, false);
        let fields_2: Fields = vec![f3, f4].into();
        let fields_3 = Fields(fields_2.0.clone());
        assert_eq!(fields_1, fields_2);
        assert_eq!(fields_1, fields_3);
    }
}
