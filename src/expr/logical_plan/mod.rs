use crate::expr::expr::Expr;
use std::sync::Arc;
use crate::common::schema::Fields;

pub enum LogicalPlan {
    Projection(Projection)
}


pub struct Projection {
    pub expr: Vec<Expr>,
    pub input: Arc<LogicalPlan>,
    pub header: Fields,
}