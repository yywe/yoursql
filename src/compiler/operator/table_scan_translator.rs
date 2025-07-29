use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use log::debug;
use crate::compiler::CompilationContext;

use anyhow::Result;

use crate::compiler::lang::LoopVariable;
use crate::compiler::operator::{Accessor, AttributeInfo, TableAttributeAccessor, TableCache};
use crate::compiler::types::{Row, RowBatch};
use crate::compiler::{
    BufferingConsumer, CompilationContextRef, ConsumerContext, OperatorTranslator, Pipeline,
};
use crate::expr::logical_plan::{LogicalPlan, TableScan};
use crate::storage::memory::MemTable; // Import MemTable
use crate::compiler::lang::DoWhileLoopBuilder;


#[derive(Debug)]
pub struct TableScanTranslator {
    pub plan: LogicalPlan,
    //pub context: CompilationContextRef<'ctx>,
    pub pipeline_id: usize,
    //pub row_cache: Option<BufferingConsumer>, // Store here
}

impl<'ctx> TableScanTranslator {
    pub fn new(
        plan: LogicalPlan,
        pipeline_id: usize,
    ) -> Self {
        Self {
            plan,
            pipeline_id,
            //row_cache: None,
        }
    }

    pub fn generate_scan<'a>(&self, compilation_context: &CompilationContext<'ctx>, ctx: &mut ConsumerContext<'a, 'ctx>) -> Result<()> {
        // Generate the scan operation
        // get Arc<dyn Table> from TableScan plan
        let (mem_table, schema) = match &self.plan {
            LogicalPlan::TableScan(ts) => {
                let table_ref = ts.source.clone();
                // Get a raw pointer from the Arc<dyn Table>
                let raw_table_ptr = table_ref
                    .as_ref()
                    .as_any()
                    .downcast_ref::<MemTable>()
                    .unwrap() as *const MemTable;
                (raw_table_ptr, ts.source.get_table().clone())
            }
            _ => return Err(anyhow::anyhow!("Expected TableScan plan")),
        };

        // Build TableAttributeAccessors for each column
        let mut attribute_accessors = HashMap::new();

        let table_row_cache = Box::new(BufferingConsumer::new(schema.clone()));
        let row_cache_ptr = Box::into_raw(table_row_cache);
        //Box::leak(table_row_cache);
        // Temporarily forget the table_row_cache to prevent it from being dropped
        // TODO: This is a temporary solution - need proper memory management
   
        //self.row_cache = Some(BufferingConsumer::new(schema.clone()));
      //  let row_cache_ptr = self.row_cache.as_mut().unwrap() as *mut BufferingConsumer;

        for (idx, field) in schema.fields.iter().enumerate() {
            let ai = AttributeInfo {
                ty: field.data_type().into(),
                idx,
                name: Some(field.name().clone()),
            };
            let accessor: Box<dyn Accessor<'_>> = Box::new(TableAttributeAccessor {
                attribute: ai.clone(),
                store: mem_table,
                row_cache: row_cache_ptr,
            });
            attribute_accessors.insert(ai, accessor);
        }

        // let's create a batch that is the whole table for now
        let num_rows = unsafe { (*(mem_table)).total_rows() };
        debug!("TableScan: total_rows = {}", num_rows);
        let codegen = compilation_context.codegen.as_ref();
        let i64_type = codegen.types.i64type;

        // for now just a single large batch, TODO: fix this;
        let row_batch = RowBatch {
            tid_start: i64_type.const_int(0, false).into(),
            tid_end: i64_type.const_int((num_rows-1) as u64, false).into(),
            num_rows: i64_type.const_int(num_rows as u64, false).into(),
            attribute_accessors: attribute_accessors,
        };

        // now let's create a for loop to iterate rows and push to parent consumer
        let loop_vars = vec![
            LoopVariable {
                name: Some("idx".to_string()),
                value: i64_type.const_zero().into(),//TODO: use tid_start
                ty: i64_type.into(),
            },
        ];

        let mut loop_builder = DoWhileLoopBuilder::new(&codegen, &loop_vars);
        // Loop body, while i<num_rows
        let i_val = loop_builder.get_loop_var(0).into_int_value();
        let cond = codegen
            .builder
            .build_int_compare(
                inkwell::IntPredicate::ULT,
                i_val,
                i64_type.const_int((num_rows-1) as u64, false).into(),//TODO: use tid_end
                "loop_cond",
            )
            .unwrap();
 
        // get the row and i_val and pass to parent consumer

        let row = Row {
            batch: &row_batch,
            batch_position: i_val.into(),
        };

        // call consume on parent consumer context
        ctx.consume(&row)?;

        // next_i = i + 1
        let next_i = codegen
            .builder
            .build_int_add(i_val, i64_type.const_int(1, false), "next_i")
            .unwrap()
            .into();

        loop_builder.loop_end(&codegen, cond, &[next_i]);

        Ok(())
    }
}

impl<'ctx> OperatorTranslator<'ctx> for TableScanTranslator {
    fn produce(&self, compilation_context: &CompilationContext<'ctx>) -> Result<()> {
        // for each row in the batch, pass to parent consumer;
        let pipeline = compilation_context.pipelines.get(self.pipeline_id).unwrap();
        pipeline.compile(compilation_context, |ctx| self.generate_scan(compilation_context, ctx))?;
        Ok(())
    }

    fn consume<'a>(&self, _context: &ConsumerContext<'a, 'ctx>, _row: &Row<'a, 'ctx>) -> Result<()> {
        // create error since Table scan is the source cannot consume
        Err(anyhow::anyhow!(
            "TableScan is a source operator and cannot consume rows from upstream"
        ))
    }

    fn get_plan(&self) -> &LogicalPlan {
        &self.plan
    }
}
