use crate::expr::logical_plan::{LogicalPlan};
use crate::compiler::{CompilationContextRef, OperatorTranslator};
use std::rc::Rc;
use std::cell::RefCell;
use crate::compiler::Pipeline;
use anyhow::Result;
use crate::compiler::types::Row;
use crate::compiler::ConsumerContext;
use crate::compiler::CompilationContext;

#[derive(Debug)]
pub struct ProjectionTranslator{
    pub plan: LogicalPlan,
    pub pipeline_id: usize,
}

impl ProjectionTranslator {
    pub fn new(
        plan: LogicalPlan,
        pipeline_id: usize,
    ) -> Self {
        Self { plan, pipeline_id }
    }
}


impl<'ctx> OperatorTranslator<'ctx> for ProjectionTranslator{
    fn produce(&self, compilation_context: &CompilationContext<'ctx>) -> Result<()> {
        // simply call produce on child operator
        let inputs = self.plan.inputs();
        let child = inputs.get(0).ok_or_else(|| {
            anyhow::anyhow!("Projection operator has no input")
        })?;
        compilation_context.produce(child)
    }
    fn consume<'a>(&self, context: &ConsumerContext<'a, 'ctx>, _row: &Row<'a, 'ctx>) -> Result<()> {
        // TODO: actual projection logic here, for now, nothing and simply pass the row to parent
        context.consume(_row)
    }

    fn get_plan(&self) -> &LogicalPlan {
        &self.plan
    }
}