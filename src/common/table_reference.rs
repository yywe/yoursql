use super::utils::parse_identifiers_normalized;
use std::borrow::Cow;
use serde::{Deserialize, Serialize};

pub struct ResolvedTableReference<'a> {
    pub database: Cow<'a, str>,
    pub table: Cow<'a, str>,
}

impl<'a> std::fmt::Display for ResolvedTableReference<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub enum TableReference<'a> {
    Bare {
        table: Cow<'a, str>,
    },
    Full {
        database: Cow<'a, str>,
        table: Cow<'a, str>,
    },
}

pub type OwnedTableReference = TableReference<'static>;

impl<'a> TableReference<'a> {
    pub fn none() -> Option<TableReference<'a>> {
        None
    }

    pub fn bare(table: impl Into<Cow<'a, str>>) -> TableReference<'a> {
        TableReference::Bare {
            table: table.into(),
        }
    }
    pub fn full(
        database: impl Into<Cow<'a, str>>,
        table: impl Into<Cow<'a, str>>,
    ) -> TableReference<'a> {
        TableReference::Full {
            database: database.into(),
            table: table.into(),
        }
    }

    pub fn to_owned_reference(&self) -> OwnedTableReference {
        match self {
            Self::Full {
                database,
                table,
            } => OwnedTableReference::Full {
                database: database.to_string().into(),
                table: table.to_string().into(),
            },
            Self::Bare { table } => OwnedTableReference::Bare {
                table: table.to_string().into(),
            },
        }
    }

    pub fn table_name(&self) -> &str {
        match self {
            Self::Full { table, .. } | Self::Bare { table } => table,
        }
    }
    pub fn parse_str(s: &'a str) -> Self {
        let mut parts = parse_identifiers_normalized(s);
        match parts.len() {
            1 => Self::Bare {
                table: parts.remove(0).into(),
            },
            2 => Self::Full {
                database: parts.remove(0).into(),
                table: parts.remove(0).into(),
            },
            _ => Self::Bare { table: s.into() },
        }
    }

    pub fn resolve(
        self,
        default_database: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                database,
                table,
            } => ResolvedTableReference {
                database,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                database: default_database.into(),
                table,
            },
        }
    }
}

impl From<String> for OwnedTableReference {
    fn from(s: String) -> Self {
        TableReference::parse_str(&s).to_owned_reference()
    }
}

impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::parse_str(s)
    }
}
