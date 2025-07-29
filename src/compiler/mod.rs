pub mod buffering_consumer;
pub mod debug;
pub mod lang;
pub mod operator;
pub mod outdated;
pub mod proxy;
pub mod query_state;
pub mod types;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Result;
use buffering_consumer::BufferingConsumer;
use inkwell::builder::Builder;
use inkwell::context::Context;
use inkwell::execution_engine::{ExecutionEngine, JitFunction};
use inkwell::module::Module;
use inkwell::types::{FloatType, IntType, PointerType, StructType, VoidType};
use inkwell::values::{BasicValueEnum, FunctionValue};
use inkwell::{AddressSpace, OptimizationLevel};
use lang::FunctionBuilder;
use log::debug;

use crate::common::record_batch::RecordBatch;
use crate::compiler::operator::filter_translator::FilterTranslator;
use crate::compiler::operator::projection_translator::ProjectionTranslator;
use crate::compiler::operator::table_scan_translator::TableScanTranslator;
use crate::compiler::proxy::FunctionArguments;
use crate::compiler::query_state::QueryState;
use crate::compiler::types::{Row, RowBatch};
use crate::expr::logical_plan::LogicalPlan;
use crate::session::SessionContext;

#[allow(dead_code)]
type VoidVoidFnType = unsafe extern "C" fn();
#[allow(dead_code)]
type VoidInt32FnType = unsafe extern "C" fn() -> i32;
#[allow(dead_code)]
type Int32Int32FnType = unsafe extern "C" fn(i32) -> i32;
#[allow(dead_code)]
type PtrPtrVoidFnType = unsafe extern "C" fn(*const i8, *const i8);
#[allow(dead_code)]
type PtrVoidFnType = unsafe extern "C" fn(*const i8);

pub struct TypeWrapper<'ctx> {
    pub i32type: IntType<'ctx>,
    pub i64type: IntType<'ctx>,
    pub i8type: IntType<'ctx>,
    pub i16type: IntType<'ctx>,
    pub f32type: FloatType<'ctx>,
    pub f64type: FloatType<'ctx>,
    pub voidtype: VoidType<'ctx>,
    pub ptrtype: PointerType<'ctx>,
}

// TODO: Rc and RefCell are heavily used, we should use a Rustacian design to avoid its usage

/// Note for the function_builders:
/// The function builder stack follows a stack discipline pattern where:
/// new_and_push_to_stack pushes a new function builder onto the stack
/// pop removes and returns the top function builder from the stack
/// These must be properly paired to avoid stack corruption
pub struct CodeGen<'ctx> {
    pub context: &'ctx Context,
    pub module: Module<'ctx>,
    pub builder: Builder<'ctx>,
    pub engine: ExecutionEngine<'ctx>,
    pub types: TypeWrapper<'ctx>,
    pub function_builders: RefCell<Vec<FunctionBuilder<'ctx>>>, /* RefCell caution, after use
                                                                 * please drop immediately */
}

pub trait OperatorTranslator<'ctx> {
    fn produce(&self, compilation_context: &CompilationContext<'ctx>) -> Result<()>;
    fn consume<'a>(&self, context: &ConsumerContext<'a, 'ctx>, row: &Row<'a, 'ctx>) -> Result<()>;
    // TODO: remove the consome_batch or rework, it may has issues
    fn consume_batch<'a>(
        &self,
        context: &ConsumerContext<'a, 'ctx>,
        batch: &RowBatch<'ctx>,
    ) -> Result<()> {
        batch.iterate(context.compilation_context.codegen.as_ref(), |row| {
            self.consume(context, &row).unwrap(); // TODO: handle the error properly
        });
        Ok(())
    }
    fn get_plan(&self) -> &LogicalPlan;
}

pub struct Pipeline<'ctx> {
    pub operators: Vec<Box<dyn OperatorTranslator<'ctx> + 'ctx>>,
    // pub compilation_context: CompilationContextRef<'ctx>,
    // pub current_op_index: RefCell<i32>,
    pub current_op_index: i32,
    pub id: usize, // unique id for the pipeline
}

pub struct CompilationContext<'ctx> {
    pub codegen: CodeGenRef<'ctx>,
    pub pipelines: Vec<Pipeline<'ctx>>,
    pub execution_consumer: Rc<BufferingConsumer>,
    pub query_state: Rc<RefCell<QueryState<'ctx>>>, /* RefCell caution, after use please drop
                                                     * immediately */
    // store (pipline_id, operator_index) instead of owned translators
    pub op_translators: HashMap<LogicalPlan, (usize, usize)>,
}

// pub struct ConsumerContext<'ctx> {
// pub compilation_context: CompilationContextRef<'ctx>,
// pub pipeline_id: usize,
// }

pub struct ConsumerContext<'a, 'ctx> {
    pub compilation_context: &'a CompilationContext<'ctx>,
    pub pipeline_id: usize,
    pub current_op_index: i32,
}

pub struct Query<'ctx> {
    pub plan: LogicalPlan,
    pub codegen: CodeGenRef<'ctx>,
    pub state: Rc<RefCell<QueryState<'ctx>>>, // RefCell caution, after use please drop immediately
    pub plan_func: Option<JitFunction<'ctx, PtrVoidFnType>>, // will be set when query compiled
}

/// this should be provide some runtime envrionment param like storage manager
pub struct ExecutorContext<'a> {
    // for each table in the query plan, we have a buffering to store the PrimTuple
    // the key is the resolved table name in string format, which can be obtained by
    // session state.resolve.resolve_table_ref(name).to_string();
    pub table_source: HashMap<String, BufferingConsumer>,
    pub session_context: &'a SessionContext,
}

impl<'ctx> Pipeline<'ctx> {
    pub fn new(id: usize) -> Self {
        Pipeline {
            operators: Vec::new(),
            current_op_index: -1, // no operator yet
            id: id,               // pipline unique id
        }
    }
    /// compile the operators in the pipeline into a function
    /// body is the kick-off point for the pipeline execution
    pub fn compile<'a, F>(
        &self,
        compilation_context: &'a CompilationContext<'ctx>,
        body: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut ConsumerContext<'a, 'ctx>) -> Result<()>,
    {
        // get codegen
        let codegen = compilation_context.codegen.as_ref();
        let func_name = self.construct_pipline_func_name();

        // Get the query state pointer (should be available from current function context)
        let query_state_ptr = codegen.get_state();

        // Compile the pipeline to a function
        {
            // Create the function builder for the pipeline function and push to stack
            FunctionBuilder::new_and_push_to_stack(
                codegen,
                func_name.as_str(),
                codegen.types.voidtype,
                &[("query_state".to_owned(), codegen.types.ptrtype.into())],
            );

            let mut consumer_ctx = ConsumerContext::new(compilation_context, self.id);

            // invoke the body to kick-off the code generation chain of translators
            body(&mut consumer_ctx)?;

            // Pop the function builder from stack and finish it
            let mut curr_fn_builder = codegen
                .function_builders
                .borrow_mut()
                .pop()
                .expect("No function builder found");
            curr_fn_builder.return_and_finish(codegen, None);

            // save the FunctionValue of the pipeline function
            // self.llvm_ir_func = codegen.module.//get_function(&func_name);
        }

        let llvm_ir_func = codegen.module.get_function(&func_name);

        // assert the function is defined (not in native code but in llvm ir form)
        assert!(llvm_ir_func.is_some(), "Pipeline function not defined yet");

        codegen
            .builder
            .build_call(
                llvm_ir_func.unwrap(),
                &[query_state_ptr.into()],
                "pipeline_call",
            )
            .unwrap();

        Ok(())
    }

    pub fn construct_pipline_func_name(&self) -> String {
        let mut parts = Vec::new();
        parts.push(format!("pipeline{}", self.id));
        // Iterate through operators in reverse order
        for operator in self.operators.iter().rev() {
            let plan = operator.get_plan();
            let plan_type = plan.get_plan_enum_name();
            // Convert to lowercase to match Peloton's StringUtil::Lower
            parts.push(plan_type.to_lowercase());
        }
        // the last pipeline step for data (the 1st in the original order)
        if self.id == 0 {
            parts.push("output".to_string());
        }
        // Join with underscores
        parts.join("_")
    }

    /// Move to the next step in this pipeline
    /// Returns a reference to the next operator translator, or None if at the end
    pub fn next_step(&mut self) -> Option<&Box<dyn OperatorTranslator<'ctx> + 'ctx>> {
        if self.current_op_index >= 0 {
            let op = self.operators.get(self.current_op_index as usize);
            self.current_op_index -= 1;
            op
        } else {
            None
        }
    }

    pub fn get_current_op_index(&self) -> i32 {
        self.current_op_index
    }
    /// Reset the pipeline index to the end (start of execution)
    /// This should be called before starting pipeline execution
    pub fn reset_to_start(&mut self) {
        self.current_op_index = self.operators.len() as i32 - 1;
    }

    /// Add an operator to the pipeline
    pub fn add_operator(&mut self, operator: Box<dyn OperatorTranslator<'ctx> + 'ctx>) {
        self.operators.push(operator);
        self.current_op_index = self.operators.len() as i32 - 1;
    }
}

impl<'ctx> Query<'ctx> {
    pub fn new(plan: LogicalPlan, codegen: CodeGenRef<'ctx>) -> Self {
        Query {
            plan,
            codegen,
            state: Rc::new(RefCell::new(QueryState::new())),
            plan_func: None,
        }
    }

    /// execute a compiled query plan
    pub fn execute(
        self,
        executor_context: &ExecutorContext,
        consumer: Rc<BufferingConsumer>,
    ) -> Result<RecordBatch> {
        assert!(self.plan_func.is_some(), "Query plan is not compiled");
        // create the struct that will be passed to the llvm ir world
        // the 1st and 2nd members are executor context and buffering consumer

        // prepare the param, then invoke the compiled function
        let target_data = self.codegen.engine.get_target_data();
        let query_state_type = self
            .state
            .as_ref()
            .borrow()
            .get_type()
            .expect("QueryState type not finalized");
        let query_state_size = target_data.get_abi_size(&query_state_type);
        debug!("QueryState size: {:?}", query_state_size);

        // allocate a block of memory for the query state in Rust world and set 1st 2 params
        // then invoke the compiled function
        let mut param_data = vec![0u8; query_state_size as usize];
        let param_ptr = param_data.as_mut_ptr();
        let func_args: *mut FunctionArguments = param_ptr as *mut FunctionArguments;
        unsafe {
            (*func_args).executor_context = executor_context as *const _ as *const i8;
            (*func_args).consumer_arg = Rc::as_ptr(&consumer) as *const i8;
            let plan_func = self.plan_func.unwrap();
            debug!("invoking the plan function with query state ptr: {:?}, executor_context: {:?}, consumer: {:?}", 
                param_ptr,(*func_args).executor_context, (*func_args).consumer_arg);
            plan_func.call(param_ptr as *const i8);
        }
        // convert the result from PrimTuple to RecordBatch
        consumer.to_record_batch()
    }
}

impl<'a> ExecutorContext<'a> {
    pub fn new(plan: &LogicalPlan, session_context: &'a SessionContext) -> Self {
        let mut table_source = HashMap::new();
        for (table, schema) in plan.source_tables().unwrap() {
            let consumer = BufferingConsumer::new(schema);
            let session_state = session_context.state.read();
            let table_name = session_state.resolve_table_ref(&table).to_string();
            table_source.insert(table_name, consumer);
        }
        ExecutorContext {
            table_source,
            session_context,
        }
    }
}

impl<'ctx> CompilationContext<'ctx> {
    pub fn new(
        codegen: CodeGenRef<'ctx>,
        query_state: Rc<RefCell<QueryState<'ctx>>>,
        buffering_consumer: Rc<BufferingConsumer>,
    ) -> Self {
        CompilationContext {
            codegen,
            pipelines: Vec::new(),
            execution_consumer: buffering_consumer.clone(),
            query_state,
            op_translators: HashMap::new(),
        }
    }

    pub fn compile_query(&mut self, query: &mut Query<'ctx>) -> Result<()> {
        // Below is a test function before we have produce and consume and compile
        // compile the query plan
        // let codegen = self.codegen.as_ref();
        // let fn_name = "main_function";
        // let mut fn_builder = FunctionBuilder::new(
        // codegen,
        // fn_name,
        // codegen.types.voidtype,
        // &[("query_state".to_owned(), codegen.types.ptrtype.into())],
        // );
        // register the global query state struct type, 1st is executor context, 2nd is the
        // buffering consumer
        // query
        // .state
        // .borrow_mut()
        // .register_state("executor_context", codegen.types.ptrtype.into());
        //
        // query
        // .state
        // .borrow_mut()
        // .register_state("buffering_consumer", codegen.types.ptrtype.into());
        // finalize the query state struct type
        // query.state.borrow_mut().finalize_type(codegen);
        //
        // now we can interpret the input param as query state struct and extract its members
        //
        // get the 1st and 2nd members of the query state struct
        // let param_ptr = fn_builder.get_arg_by_name("query_state").unwrap();
        // let executor_context_ptr_ptr = codegen
        // .builder
        // .build_struct_gep(
        // query.state.borrow().get_type().unwrap(),
        // param_ptr.into_pointer_value(),
        // 0,
        // "executor_context_ptr_ptr",
        // )
        // .unwrap();
        // let consumer_ptr_ptr = codegen
        // .builder
        // .build_struct_gep(
        // query.state.borrow().get_type().unwrap(),
        // param_ptr.into_pointer_value(),
        // 1,
        // "consumer_ptr_ptr",
        // )
        // .unwrap();
        // load the executor context and consumer pointers
        // let executor_context_ptr = codegen
        // .builder
        // .build_load(
        // codegen.types.ptrtype,
        // executor_context_ptr_ptr,
        // "executor_context_ptr",
        // )
        // .unwrap()
        // .into_pointer_value();
        // let consumer_ptr = codegen
        // .builder
        // .build_load(codegen.types.ptrtype, consumer_ptr_ptr, "consumer_ptr")
        // .unwrap()
        // .into_pointer_value();
        //
        // codegen.emit_printf_call(
        // "\t[info from LLVM IR:query_state_ptr=%p, executor_context_ptr=%p, consumer_ptr=%p]\n",
        // &[
        // fn_builder.get_arg_by_name("query_state").unwrap().into(),
        // executor_context_ptr.into(),
        // consumer_ptr.into(),
        // ],
        // );
        // fn_builder.return_and_finish(codegen, None);

        let codegen = self.codegen.clone();

        // Create the main pipeline
        let main_pipeline = Pipeline::new(0);
        self.pipelines.push(main_pipeline);
        // register the global query state struct type, 1st is executor context, 2nd is the
        // buffering consumer
        query
            .state
            .borrow_mut()
            .register_state("executor_context", codegen.types.ptrtype.into());

        query
            .state
            .borrow_mut()
            .register_state("buffering_consumer", codegen.types.ptrtype.into());

        // prepare the pipeline with operators based on the query plan
        self.prepare(&query.plan, 0)?;

        // finalize the query state struct type
        query.state.borrow_mut().finalize_type(codegen.as_ref());

        let main_fn_name = "plan_entry_function";

        {
            // Create the function builder for the pipeline function and push to stack
            FunctionBuilder::new_and_push_to_stack(
                codegen.as_ref(),
                main_fn_name,
                codegen.types.voidtype,
                &[("query_state".to_owned(), codegen.types.ptrtype.into())],
            );

            self.produce(&query.plan)?;

            let mut fn_builder = codegen
                .function_builders
                .borrow_mut()
                .pop()
                .expect("No function builder found");

            fn_builder.return_and_finish(codegen.as_ref(), None);
        }

        // verify before saving the JIT function
        codegen
            .module
            .verify()
            .map_err(|e| anyhow::anyhow!("Module verification failed: {:?}", e))?;
        debug!("########LLVM IR verified###############");
        // save the JIT function to the query object
        let jit_function: JitFunction<PtrVoidFnType> = unsafe {
            codegen
                .engine
                .get_function(main_fn_name)
                .expect("Failed to get JIT function")
        };
        codegen.show_llvm_ir();

        query.plan_func = Some(jit_function);

        Ok(())
    }

    pub fn get_translator(
        &self,
        plan: &LogicalPlan,
    ) -> Option<&Box<dyn OperatorTranslator<'ctx> + 'ctx>> {
        if let Some((pipeline_id, operator_index)) = self.op_translators.get(plan) {
            self.pipelines
                .get(*pipeline_id)?
                .operators
                .get(*operator_index)
        } else {
            None
        }
    }

    pub fn produce(&self, plan: &LogicalPlan) -> Result<()> {
        if let Some(translator) = self.get_translator(plan) {
            translator.produce(self)
        } else {
            debug!(
                "No translator found for the plan: {}",
                plan.get_plan_enum_name()
            );
            Err(anyhow::anyhow!("No translator found for the plan"))
        }
    }

    pub fn prepare(&mut self, plan: &LogicalPlan, pipeline_id: usize) -> Result<()> {
        match plan {
            LogicalPlan::Projection(_projection) => {
                debug!("1st prepare for child of projection");


                debug!(
                    "Preparing ProjectionTranslator for plan projection: {:?}",
                    plan
                );

                // Create single translator instance
                let translator = ProjectionTranslator::new(plan.clone(), pipeline_id);

                // Add to pipeline (single owner)
                let pipeline = &mut self.pipelines[pipeline_id];
                let operator_index = pipeline.operators.len();
                pipeline.add_operator(Box::new(translator));

                // Store lookup info in HashMap (not the actual translator)
                self.op_translators
                    .insert(plan.clone(), (pipeline_id, operator_index));

                // prepare the child
                let inputs = plan.inputs();
                let child = inputs
                    .get(0)
                    .ok_or_else(|| anyhow::anyhow!("Projection operator has no input"))?;
                self.prepare(child, pipeline_id)?;
            }
            LogicalPlan::Filter(_filter) => {


                // create a FilterTranslator and register it
                debug!("Preparing FilterTranslator for plan filter: {:?}", plan);

                let translator = FilterTranslator::new(plan.clone(), pipeline_id);
                let pipeline = &mut self.pipelines[pipeline_id];
                let operator_index = pipeline.operators.len();
                pipeline.add_operator(Box::new(translator));
                self.op_translators
                    .insert(plan.clone(), (pipeline_id, operator_index));


                debug!("prepare for child of filter");
                // prepare the child 
                let inputs = plan.inputs();
                let child = inputs
                    .get(0)
                    .ok_or_else(|| anyhow::anyhow!("Filter operator has no input"))?;
                self.prepare(child, pipeline_id)?;

            }
            LogicalPlan::TableScan(_scan) => {
                let translator = TableScanTranslator::new(plan.clone(), pipeline_id);
                let pipeline = &mut self.pipelines[pipeline_id];
                let operator_index = pipeline.operators.len();
                pipeline.add_operator(Box::new(translator));
                self.op_translators
                    .insert(plan.clone(), (pipeline_id, operator_index));

                 // table scan do not have child
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported plan type yet"));
            }
        }
        // print the current registered translators
        for (plan, _translator) in self.op_translators.iter() {
            debug!(
                "Registered translator for plan: {}",
                plan.get_plan_enum_name()
            );
        }
        Ok(())
    }
}

impl<'a, 'ctx> ConsumerContext<'a, 'ctx> {
    pub fn new(compilation_context: &'a CompilationContext<'ctx>, pipeline_id: usize) -> Self {
        let pipeline = compilation_context.pipelines.get(pipeline_id).unwrap();
        ConsumerContext {
            compilation_context,
            pipeline_id,
            current_op_index: pipeline.operators.len() as i32 - 1, // Start at the last operator
        }
    }

    pub fn get_query_state(&self) -> Ref<'_, QueryState<'ctx>> {
        self.compilation_context.query_state.borrow()
    }
    pub fn get_query_state_type(&self) -> StructType<'ctx> {
        self.compilation_context
            .query_state
            .borrow()
            .get_type()
            .expect("QueryState type not finalized")
    }

    /// Get pipeline information by ID from the compilation context
    pub fn get_pipeline(&self) -> Option<&Pipeline<'ctx>> {
        self.compilation_context.pipelines.get(self.pipeline_id)
    }

    pub fn consume(&self, row: &Row<'_, 'ctx>) -> Result<()> {
        // using the pipline id to get the pipline from compilation context
        let pipeline = self
            .get_pipeline()
            .ok_or_else(|| anyhow::anyhow!("Pipeline with id {} not found", self.pipeline_id))?;

        // let translator = pipeline.next_step();
        debug!(
            "ConsumerContext consuming at pipeline_id: {}, current_op_index: {}",
            self.pipeline_id, self.current_op_index
        );
        let translator = pipeline.operators.get((self.current_op_index - 1) as usize);

        debug!(
            "Translator at current_op_index {}: {:?}",
            self.current_op_index - 1,
            translator
                .as_ref()
                .map(|t| t.get_plan().get_plan_enum_name())
        );
        // debug output all translators in pipeline.operators
        for (i, op) in pipeline.operators.iter().enumerate() {
            debug!("Operator {}: {}", i, op.get_plan().get_plan_enum_name());
        }

        match translator {
            Some(translator) => {
                // self.current_op_index -= 1; // Move to the next operator for the next consume
                // call;
                //TODO: here is a workaround to create a new ConsumerContext for the next step, can we just update self.current_op_index -= 1;
                // Create a new ConsumerContext for the next step
                let next_context = ConsumerContext {
                    compilation_context: self.compilation_context,
                    pipeline_id: self.pipeline_id,
                    current_op_index: self.current_op_index - 1, // Move to next operator
                };

                // translator.consume(self, row)
                translator.consume(&next_context, row)
            }
            None => {
                // End of pipeline
                let consumer = &self.compilation_context.execution_consumer;
                consumer.consume_result(self, row);
                Ok(())
            }
        }
    }
}

type CodeGenRef<'ctx> = Rc<CodeGen<'ctx>>;
type CompilationContextRef<'ctx> = Rc<CompilationContext<'ctx>>;

impl<'ctx> CodeGen<'ctx> {
    pub fn new_codegen_ref(context: &'ctx Context, name: &str) -> CodeGenRef<'ctx> {
        Rc::new(CodeGen::new(context, name))
    }

    pub fn new(context: &'ctx Context, name: &str) -> CodeGen<'ctx> {
        let module = context.create_module(name);
        let builder = context.create_builder();
        let engine = module
            .create_jit_execution_engine(OptimizationLevel::None)
            .unwrap();
        let types = TypeWrapper::new(context);
        CodeGen {
            context,
            module,
            builder,
            engine,
            types,
            function_builders: RefCell::new(Vec::new()), /* RefCell caution, after use drop
                                                          * immediately */
        }
    }

    /// Get a struct type by name
    pub fn lookup_type(&self, name: &str) -> Option<StructType<'ctx>> {
        self.module.get_struct_type(name)
    }

    /// Initialize the codegen context
    pub fn initialize(&self) {
        // register the proxy functions
        self.register_proxy_functions();
        // register the proxy types
        self.register_proxy_types();
        // register standard library functions
        self.register_stdlib_functions();
    }

    /// When we do code gen, we use the function_builders as a stack,
    /// where the top entry is always the current function that is building
    /// once done, pop the top entry
    /// here we get the state from the top function builder
    /// the state is always the first argument of the function
    pub fn get_state(&self) -> BasicValueEnum<'ctx> {
        // get the function builder at the stack top
        let fn_builder = self.function_builders.borrow();
        fn_builder
            .last()
            .expect("No function builder found")
            .get_arg_by_index(0)
            .expect("Failed to get state argument")
    }

    /// get the current function builder
    pub fn get_current_function_builder(&self) -> Ref<'_, FunctionBuilder<'ctx>> {
        // get the function builder at the stack top
        let fn_builder = self.function_builders.borrow();
        Ref::map(fn_builder, |fb| {
            fb.last().expect("No function builder found")
        })
    }
}

impl<'ctx> TypeWrapper<'ctx> {
    pub fn new(context: &'ctx Context) -> TypeWrapper<'ctx> {
        TypeWrapper {
            i32type: context.i32_type(),
            i64type: context.i64_type(),
            i8type: context.i8_type(),
            i16type: context.i16_type(),
            f32type: context.f32_type(),
            f64type: context.f64_type(),
            voidtype: context.void_type(),
            ptrtype: context.ptr_type(AddressSpace::default()),
        }
    }
}

/// entry function to compile and execute a query plan
pub fn compile_and_execute(plan: &LogicalPlan, session: &SessionContext) -> Result<RecordBatch> {
    // create the query object
    let context = Context::create();
    let codegen = CodeGen::new(&context, "test");
    codegen.initialize();
    let codegen = Rc::new(codegen);

    // create the query object
    let mut query = Query::new(plan.clone(), codegen.clone());

    let output_schema = plan.output_schema();

    let consumer = Rc::new(BufferingConsumer::new(output_schema.clone()));

    // create the compilation context
    let mut compilation_context = CompilationContext::new(
        codegen.clone(),
        query.state.clone(),
        consumer.clone(), // pass the consumer to the compilation context
    );

    // compile the query
    compilation_context.compile_query(&mut query)?;

    // execute the query
    let executor_context = ExecutorContext::new(plan, session);

    query.execute(&executor_context, consumer.clone())
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;
    use crate::parser::parse;
    use crate::session::SessionContext;
    #[test]
    fn test_compile_and_execute() {
        let session = SessionContext::default();
        // use interpreter to create a table and insert some data
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            session
                .state
                .read()
                .run("create table tbl(id int, name varchar(20), age int)")
                .await
                .unwrap();
            session
                .state
                .read()
                .run("insert into tbl values (2, 'mike',34), (3, 'john',40), (4, 'json', 50)")
                .await
                .unwrap();
        });
        // get a logical plan from session
        let plan = rt.block_on(async {
            let statement = parse("select id, name from tbl where age<=40").unwrap();
            let logical_plan = session
                .state
                .read()
                .make_logical_plan(statement)
                .await
                .unwrap();
            logical_plan
        });
        // compile and execute the query plan
        let result = compile_and_execute(&plan, &session);
        assert!(result.is_ok());
        debug!("query result:\n{}\n", result.unwrap().to_string_table());
    }
}
