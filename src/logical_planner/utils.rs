use sqlparser::ast::Ident;

pub fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) =>id.value,
        None=>id.value.to_ascii_lowercase(),
    }
}