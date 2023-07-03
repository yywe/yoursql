use anyhow::Result;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub(crate) fn parse_identiiers(s: &str) -> Result<Vec<Ident>> {
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(s)?;
    let idents = parser.parse_multipart_identifier()?;
    Ok(idents)
}

pub(crate) fn parse_identifiers_normalized(s: &str) -> Vec<String> {
    parse_identiiers(s)
        .unwrap_or_default()
        .into_iter()
        .map(|ident| match ident.quote_style {
            Some(_) => ident.value,
            None => ident.value.to_ascii_lowercase(),
        })
        .collect::<Vec<_>>()
}
