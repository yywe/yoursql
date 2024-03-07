pub mod mutation;
pub mod plan_expr;
pub mod query;
pub mod statement;
pub mod utils;

use crate::{
    common::{
        config::ConfigOptions,
        table_reference::{OwnedTableReference, TableReference},
    },
    logical_planner::utils::normalize_ident,
    storage::Table,
};
use anyhow::{anyhow, Result};
use sqlparser::ast::{Ident, ObjectName};
use std::sync::Arc;

pub trait PlannerContext {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn Table>>;
    fn options(&self) -> &ConfigOptions;
}

#[derive(Debug)]
pub struct ParserOptions {
    pub parse_float_as_decimal: bool,
    pub enable_ident_normalization: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            parse_float_as_decimal: false,
            enable_ident_normalization: true,
        }
    }
}

#[derive(Debug)]
pub struct IdentNormalizer {
    normalize: bool,
}

impl Default for IdentNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }
    pub fn normalize(&self, ident: Ident) -> String {
        if self.normalize {
            normalize_ident(ident)
        } else {
            ident.value
        }
    }
}

pub struct LogicalPlanner<'a, C: PlannerContext> {
    pub options: ParserOptions,
    pub normalizer: IdentNormalizer,
    pub context: &'a C,
}

impl<'a, C: PlannerContext> LogicalPlanner<'a, C> {
    pub fn new_with_options(tables: &'a C, options: ParserOptions) -> Self {
        let normalize = options.enable_ident_normalization;
        Self {
            options: options,
            normalizer: IdentNormalizer::new(normalize),
            context: tables,
        }
    }
    pub fn new(tables: &'a C) -> Self {
        Self::new_with_options(tables, ParserOptions::default())
    }
}

pub fn object_name_to_table_refernce(
    object_name: ObjectName,
    enable_normalization: bool,
) -> Result<OwnedTableReference> {
    let ObjectName(idents) = object_name;
    idents_to_table_reference(idents, enable_normalization)
}

pub fn idents_to_table_reference(
    idents: Vec<Ident>,
    enable_normalization: bool,
) -> Result<OwnedTableReference> {
    struct IdentTaker(Vec<Ident>);
    impl IdentTaker {
        fn take(&mut self, enable_normalization: bool) -> String {
            let ident = self.0.pop().expect("no more identifiers");
            IdentNormalizer::new(enable_normalization).normalize(ident)
        }
    }
    let mut taker = IdentTaker(idents);
    match taker.0.len() {
        1 => {
            let table = taker.take(enable_normalization);
            Ok(OwnedTableReference::bare(table))
        }
        2 => {
            let table = taker.take(enable_normalization);
            let database = taker.take(enable_normalization);
            Ok(OwnedTableReference::full(database, table))
        }
        _ => Err(anyhow!("Invalid identifier")),
    }
}
