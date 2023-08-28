use super::{table_reference::OwnedTableReference, utils::parse_identifiers_normalized};

#[derive(Debug)]
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
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.flat_name())
    }
}