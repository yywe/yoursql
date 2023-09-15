use sqlparser::ast::Ident;
use crate::expr::expr::Expr;
use std::collections::HashMap;
use anyhow::Result;

pub fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) =>id.value,
        None=>id.value.to_ascii_lowercase(),
    }
}

