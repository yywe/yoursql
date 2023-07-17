use anyhow::Context;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use crate::common::types::DataType;
use super::table_reference::OwnedTableReference;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: HashMap<String, String>,
    qualifier: Option<OwnedTableReference>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool, qualifier: Option<OwnedTableReference>) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::default(),
            qualifier: qualifier,
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
pub struct Schema {
    pub fields: Fields,
    pub metadata: HashMap<String, String>,
}

pub type SchemaRef = Arc<Schema>;

impl Schema {
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
    pub fn project(&self, indices: &[usize]) -> Result<Schema> {
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
        let qualifier = OwnedTableReference::Full {
            database: "testdb".to_string().into(),
            table: "testtable".to_string().into(),
        };
        let f1 = Field::new("name_a", DataType::Binary, false,Some(qualifier.clone()));
        let f2 = Field::new("name_a", DataType::Binary, false,Some(qualifier.clone()));
        let f3 = Field::new("name_b", DataType::Binary, false,Some(qualifier.clone()));
        let f4 = Field::new("name_b", DataType::Binary, true,Some(qualifier.clone()));
        assert_eq!(f1, f2);
        assert_ne!(f1, f3);
        assert_ne!(f3, f4);
    }

    #[test]
    fn test_fields_equal() {
        let qualifier = OwnedTableReference::Full {
            database: "testdb".to_string().into(),
            table: "testtable".to_string().into(),
        };
        let f1 = Field::new("name_a", DataType::Int16, false,Some(qualifier.clone()));
        let f2 = Field::new("name_b", DataType::Binary, false,Some(qualifier.clone()));
        let fields_1: Fields = vec![f1, f2].into();
        let f3 = Field::new("name_a", DataType::Int16, false,Some(qualifier.clone()));
        let f4 = Field::new("name_b", DataType::Binary, false,Some(qualifier));
        let fields_2: Fields = vec![f3, f4].into();
        let fields_3 = Fields(fields_2.0.clone());
        assert_eq!(fields_1, fields_2);
        assert_eq!(fields_1, fields_3);
    }
}
