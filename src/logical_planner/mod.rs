use crate::{common::{table_reference::TableReference, config::ConfigOptions}, storage::Table};
use anyhow::Result;
use std::sync::Arc;


pub trait Context {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn Table>>;
    fn options(&self) -> &ConfigOptions;
}

pub struct LogicalPlanner {

}