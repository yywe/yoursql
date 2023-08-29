use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap,HashSet};
use std::ops::Deref;
use std::sync::Arc;
use crate::common::types::DataType;
use super::column::Column;
use super::table_reference::OwnedTableReference;
use super::table_reference::TableReference;

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
    pub fn qualifier(&self) -> Option<&OwnedTableReference> {
        self.qualifier.as_ref()
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

    /// before creating Schema, also check the name, ensure a unique name is not 
    /// both unqualified and qualified, see impl
    pub fn new_with_metadata(fields: Vec<Field>, metadata: HashMap<String, String>) -> Result<Self> {
        let mut qualified_names = HashSet::new();
        let mut unqualified_names = HashSet::new();
        for field in &fields {
            if let Some(qualifier) = field.qualifier() {
                qualified_names.insert((qualifier, field.name()));
            }else if unqualified_names.insert(field.name()) {
                return Err(anyhow!(format!("duplicate unqualified field {}", field.name())))
            }
        }
        let mut qualified_names =  qualified_names.iter().map(|(l, r)|(l.to_owned(), r.to_owned())).collect::<Vec<(&OwnedTableReference, &String)>>();
        qualified_names.sort();
        for (qualifer, name) in &qualified_names {
            if unqualified_names.contains(name) {
                return Err(anyhow!(format!("ambigious  field: {}, qualifed version:{}", name, qualifer)))
            }
        };
        Ok(Self {fields: fields.into(), metadata:metadata})
    }

    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// cause we implemented Deref for fields, so we can use &self.fields[i]
    /// although fields is: Fields(Arc<[FieldRef]>)
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    pub fn project(&self, indices: &[usize]) -> Result<Schema> {
        Ok(Self {
            fields: self.fields.project(indices)?,
            metadata: self.metadata.clone(),
        })
    }
    /// given column name (optional table reference), return the index of field in this schema
    /// one thing to note is that when the table qualifier (either in the input params or in the
    /// schema fields) is missing, the equal comparison will omit the part 
    pub fn index_of_column_by_name(&self, qualifier: Option<&TableReference>, name: &str) -> Result<Option<usize>> {
        let mut matches = self.fields.iter().enumerate().filter(|(_, field)|{
            match (qualifier, &field.qualifier) {
                // both the input name and field name are qualified, this requires the field name = name
                // and the qualifiers are equal, when compare qualifers (relation, if database is absent, ignore)
                (Some(q), Some(ref field_q))=>{
                    let q_match = match q {
                        TableReference::Bare { table } => table == field_q.table_name(), // only compare table name, below is similar
                        TableReference::Full { database, table } => table == field_q.table_name() && field_q.database_name().map_or(true, |n|n==database)
                    };
                    field.name() == name && q_match
                }
                // query is qualified, but field is un-qualifed, check if field name has qualifier
                (Some(q), None) => {
                    let column = Column::from_qualified_name(field.name());
                    match column {
                        Column {
                            relation: Some(r),
                            name: column_name,
                        }=> &r == q && column_name == name,
                        _=>false,
                    }
                }
                // field to look up is unqualified, ignore qualifier
                (None, Some(_)) | (None, None) => field.name() == name,
            }
        }).map(|(idx,_)|idx);
        Ok(matches.next())
    }

    /// get the field in the Schema based on column (optional relation and name)
    pub fn field_from_column(&self, column: &Column) -> Result<&Field> {
        match &column.relation {
            Some(r) => self.field_with_qualified_name(r, &column.name),
            None=> self.field_with_unqualified_name(&column.name),
        }
    }

    /// get the field in the Schema based on qualifier and name
    pub fn field_with_qualified_name(&self, qualifier: &TableReference, name: &str) -> Result<&Field> {
        let idx = self.index_of_column_by_name(Some(qualifier), name)?.context(format!("failed to find field using qualifier:{}, name:{}",qualifier.to_string(), name))?;
        Ok(self.field(idx))
    }

    /// get all the fields maching the given name
    pub fn fields_with_unqualified_name(&self, name: &str) -> Vec<&Field> {
        self.fields.iter().filter(|f|f.name() == name).map(|f|&(**f)).collect()
    }

    pub fn field_with_unqualified_name(&self, name: &str) -> Result<&Field> {
        let matches = self.fields_with_unqualified_name(name);
        match matches.len() {
            0 => Err(anyhow!(format!("failed to find field using name:{}", name))),
            1 => Ok(matches[0]),
            _=> {
                // when there are multiple matches, see if we have any fields without qualifer
                // if so, that's it
                let fields_without_qualifier = matches.iter().filter(|f|f.qualifier.is_none()).collect::<Vec<_>>();
                if fields_without_qualifier.len() == 1 {
                    Ok(fields_without_qualifier[0])
                }else{
                    Err(anyhow!(format!("found multiple fields using name:{}", name)))
                }

            }
        }
    }
}


/// Given column name, retrieve relevant meta information 
pub trait ColumnMeta: std::fmt::Debug {
    fn nullable(&self, col: &Column) -> Result<bool>;
    fn data_type(&self, col: &Column) -> Result<&DataType>;
}

impl ColumnMeta for Schema {
    fn nullable(&self, col: &Column) -> Result<bool>{
        Ok(self.field_from_column(col)?.is_nullable())
    }

    fn data_type(&self, col: &Column) -> Result<&DataType>{
        Ok(self.field_from_column(col)?.data_type())
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
