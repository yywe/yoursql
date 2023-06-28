
use std::borrow::Cow;


pub struct ResolvedTableReference<'a> {
    pub catalog: Cow<'a, str>,
    pub schema: Cow<'a, str>,
    pub table: Cow<'a, str>,
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
    }
}

pub type OwnedTableReference = TableReference<'static>;
