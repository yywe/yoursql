use crate::physical_expr::PhysicalExpr;
use std::sync::Arc;
use std::fmt::Debug;
use crate::physical_expr::aggregate::AggregateExpr;

#[derive(Clone, Debug, Default)]
pub struct PhysicalGroupBy {
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
}

impl PhysicalGroupBy {
    pub fn new(expr: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
        Self { expr }
    }
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }
    pub fn is_empty(&self) -> bool {
        self.expr.is_empty()
    }
}

impl PartialEq for PhysicalGroupBy {
    fn eq(&self, other: &Self) -> bool {
        self.expr.len() == other.expr.len()
            && self
                .expr
                .iter()
                .zip(other.expr.iter())
                .all(|((expr1, name1), (expr2, name2))| expr1.eq(expr2) && name1 == name2)
    }
}


#[derive(Debug)]
pub struct AggregateExec {
    pub group_by: PhysicalGroupBy,
    pub aggr_expr: Vec<Arc<dyn AggregateExpr>>
}
