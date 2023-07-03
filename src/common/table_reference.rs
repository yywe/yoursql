use std::borrow::Cow;

pub struct ResolvedTableReference<'a> {
    pub catalog: Cow<'a, str>,
    pub schema: Cow<'a, str>,
    pub table: Cow<'a, str>,
}

impl<'a> std::fmt::Display for ResolvedTableReference<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

pub enum TableReference<'a> {
    Bare {
        table: Cow<'a, str>,
    },
    Partial {
        schema: Cow<'a, str>,
        table: Cow<'a, str>,
    },
    Full {
        catalog: Cow<'a, str>,
        schema: Cow<'a, str>,
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
    pub fn partial(
        schema: impl Into<Cow<'a, str>>,
        table: impl Into<Cow<'a, str>>,
    ) -> TableReference<'a> {
        TableReference::Partial {
            schema: schema.into(),
            table: table.into(),
        }
    }
    pub fn full(
        catalog: impl Into<Cow<'a, str>>,
        schema: impl Into<Cow<'a, str>>,
        table: impl Into<Cow<'a, str>>,
    ) -> TableReference<'a> {
        TableReference::Full {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: default_catalog.into(),
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: default_catalog.into(),
                schema: default_schema.into(),
                table,
            },
        }
    }
}
