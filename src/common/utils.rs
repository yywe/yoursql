use std::collections::HashMap;

use anyhow::Result;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::expr::expr::Expr;

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

pub fn extract_aliases(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(nested_expr, alias_name) => {
                Some((alias_name.clone(), *nested_expr.clone()))
            }
            _ => None,
        })
        .collect::<HashMap<String, Expr>>()
}
