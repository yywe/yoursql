use super::LogicalPlan;


pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    pub fn from(plan: LogicalPlan) -> Self {
        Self {plan}
    }
}