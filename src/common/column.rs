use super::{table_reference::OwnedTableReference, utils::parse_identifiers_normalized};
use crate::common::schema::Schema;
use std::{collections::HashSet};
use anyhow::{Result,anyhow};

#[derive(Debug, Clone,Hash, Eq, Ord,PartialOrd, PartialEq)]
pub struct Column {
    pub relation: Option<OwnedTableReference>,
    pub name: String,
}

impl Column {
    pub fn new(relation: Option<impl Into<OwnedTableReference>>, name: impl Into<String>) -> Self {
        Self {
            relation: relation.map(|r| r.into()),
            name: name.into(),
        }
    }
    pub fn from_qualified_name(flat_name: impl Into<String>) -> Self {
        let flat_name = flat_name.into();
        let mut idents = parse_identifiers_normalized(&flat_name);
        let (relation, name) = match idents.len() {
            1 => (None, idents.remove(0)),
            2 => (Some(OwnedTableReference::Bare { table: idents.remove(0).into() }), idents.remove(0)),
            3 => (Some(OwnedTableReference::Full {database: idents.remove(0).into(), table: idents.remove(0).into()}), idents.remove(0)),
            _=> (None, flat_name),
        };
        Self {relation, name}
    }
    pub fn flat_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}",r, self.name),
            None=>self.name.clone(),
        }
    }
    pub fn normalize_with_schemas_and_ambiguity_check(
        self,
        schemas: &[&[&Schema]],
        using_columns: &[HashSet<Column>]
    )->Result<Self> {
        if self.relation.is_some() {
            return Ok(self)
        }
        for schema_level in schemas {
            let fields = schema_level.iter().flat_map(|s|s.fields_with_unqualified_name(&self.name)).collect::<Vec<_>>();
            match fields.len() {
                0 => continue,
                1 => return Ok(fields[0].qualified_column()),
                _=>{
                    for using_col in using_columns {
                        let all_matched = fields.iter().all(|f|using_col.contains(&f.qualified_column()));
                        if all_matched {
                            return Ok(fields[0].qualified_column())
                        }
                    }
                    return Err(anyhow!("ambigious columns found for {self}"))
                }
            }
        }
        return Err(anyhow!("failed to find field {self}"))
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.flat_name())
    }
}

impl From<String> for Column {
    fn from(c: String) -> Self {
        Self::from_qualified_name(c)
    }
}
impl From<&String> for Column {
    fn from(c: &String) -> Self {
        Self::from_qualified_name(c)
    }
}

impl From<&str> for Column {
    fn from(c: &str) -> Self {
        Self::from_qualified_name(c)
    }
}