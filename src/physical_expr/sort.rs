use crate::physical_expr::PhysicalExpr;
use core::hash::Hash;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PhysicalSortExpr {
    pub expr: Arc<dyn PhysicalExpr>,
    pub descending: bool,
    pub nulls_first: bool,
}

impl PartialEq for PhysicalSortExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.descending == other.descending
            && self.nulls_first == other.nulls_first
    }
}

impl Eq for PhysicalSortExpr {}

impl Hash for PhysicalSortExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.descending.hash(state);
        self.nulls_first.hash(state);
    }
}
