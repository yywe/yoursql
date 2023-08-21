pub mod utils;
pub mod query;
pub mod mutation;

use crate::{common::{table_reference::TableReference, config::ConfigOptions}, storage::Table};
use crate::logical_planner::utils::normalize_ident;
use anyhow::Result;
use std::sync::Arc;
use sqlparser::ast::Ident;



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
        Self {
            normalize: true
        }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self {normalize}
    }
    pub fn normalize(&self, ident: Ident) -> String {
        if self.normalize {
            normalize_ident(ident)
        } else{
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