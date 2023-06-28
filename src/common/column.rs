use super::table_reference::OwnedTableReference;

pub struct Column {
    pub relation: Option<OwnedTableReference>,
    pub name: String,
}