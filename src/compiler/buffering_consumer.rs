use std::cell::RefCell;
use std::collections::HashMap;

use anyhow::Result;
use inkwell::values::{IntValue, PointerValue};

use crate::common::record_batch::RecordBatch;
use crate::common::schema::SchemaRef;
use crate::compiler::operator::AttributeInfo;
use crate::compiler::proxy::{PrimTupleProxy, PrimValueProxy};
use crate::compiler::types::{PrimTuple, PrimValue, Row, RowBatch};
use crate::compiler::{CodeGen, ConsumerContext};

/// This is the final consumer that will pull data out from the llvm world,
/// so the buffer is vector of primitive tuples, and will be converted to RecordBatch
/// when needed.
#[derive(Debug)]
pub struct BufferingConsumer {
    /// The schema of the data being consumed
    pub schema: SchemaRef,
    /// The buffer for storing the data
    pub buffer: RefCell<Vec<PrimTuple>>, // RefCell caution, after use please drop immediately
    /// The output attributes, this may be just a simplied schema,
    /// things may be redundant here, TODO: see how to simplifi this
    pub output_ais: Vec<AttributeInfo>,
    pub row_map: HashMap<u64, u64>, // map physical row id to cache index, when used as TableCache
}

impl BufferingConsumer {
    /// Create a new buffering consumer with the given schema
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: schema.clone(),
            buffer: RefCell::new(Vec::new()),
            output_ais: schema
                .fields
                .iter()
                .enumerate()
                .map(|(idx, field)| AttributeInfo {
                    ty: field.data_type().into(),
                    idx: idx,
                    name: Some(field.name().clone()),
                })
                .collect(),
            row_map: HashMap::new(), // Initialize an empty row map
        }
    }

    /// Add a primitive tuple to the buffer
    pub fn add(&mut self, tuple: PrimTuple) {
        self.buffer.borrow_mut().push(tuple);
    }

    /// Convert the buffered data to a RecordBatch
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        let buffer = self.buffer.borrow();
        for tuple in buffer.iter() {
            let row = (*tuple).into();
            rows.push(row);
        }
        Ok(RecordBatch::new(self.schema.clone(), rows))
    }

    /// Consume the llvm row by putting it to Vec<PrimTuple>
    /// The idea here is that we will allocate llvm memory for a tuple
    /// in stack, then involke the rust function buffering_consumer_add
    /// which will do a deep copy of the memory in rust world. Rust will
    /// be responsible for freeing the (heap) memory.
    pub fn consume_result<'a, 'ctx>(&self, ctx: &ConsumerContext<'a, 'ctx>, row: &Row<'_, 'ctx>) {
        let codegen = ctx.compilation_context.codegen.clone();
        let num_cols = codegen
            .types
            .i32type
            .const_int(self.output_ais.len() as u64, false);
        let value_type = PrimValueProxy::get_llvm_type(codegen.as_ref());
        let tuple_buffer = codegen
            .builder
            .build_array_alloca(value_type, num_cols, "tuple_buffer")
            .unwrap();

        let size_bytes = codegen.types.i64type.const_int(
            (self.output_ais.len() * std::mem::size_of::<PrimValue>()) as u64,
            false,
        );

        let memset_fn = codegen.module.get_function("memset").unwrap();
        codegen
            .builder
            .build_call(
                memset_fn,
                &[
                    tuple_buffer.into(),
                    codegen.types.i32type.const_zero().into(),
                    size_bytes.into(),
                ],
                "memset_tuple_buffer",
            )
            .unwrap();

        // for each column, derive and write the value to the respective offset;
        // one interesting node, we assume for string and binary, the underlying memory is
        // allocated in the current function stack, which is responsible by the derive_value
        // function where we only allocate for the PrimValue
        let attach_tuple_fn = codegen
            .module
            .get_function("buffering_consumer_add")
            .unwrap();

        let fn_builder = codegen.get_current_function_builder();
        // TODO: abstract  a funciton to  to this???
        let query_state_ptr = fn_builder.get_arg_by_index(0).unwrap().into_pointer_value();
        std::mem::drop(fn_builder); // End the immedately after use to avoid borrow issues
        let query_state_struct_type = ctx.get_query_state_type();

        codegen.emit_printf_call("query_state_ptr: %p\n", &[query_state_ptr.into()]);

        let consumer_ptr_ptr = codegen
            .builder
            .build_struct_gep(
                query_state_struct_type,
                query_state_ptr,
                1, // buffering consumer is the 2nd member
                "consumer_ptr_ptr",
            )
            .unwrap();

        let consumer_ptr = codegen
            .builder
            .build_load(codegen.types.ptrtype, consumer_ptr_ptr, "consumer_ptr")
            .unwrap()
            .into_pointer_value();

        self.write_tuple_to_buffer(codegen.as_ref(), row, tuple_buffer);

        // struct PrimTuple with 2 members, 1 is the buffer pointer
        // another is the number of values, then pass the struct to attach_tuple_fn;
        let tuple_ptr = codegen
            .builder
            .build_alloca(PrimTupleProxy::get_llvm_type(codegen.as_ref()), "tuple_ptr")
            .unwrap();
        self.write_tuple_metadata(
            codegen.as_ref(),
            tuple_ptr,
            tuple_buffer,
            codegen
                .types
                .i64type
                .const_int(self.output_ais.len() as u64, false),
        );

        // lastly, invoke the FFI function to add the tuple to the buffer
        // note in peloton, the buffer pointer is obtained by querying the state map
        // llvm::Value *buffer_ptr =
        // query_state.LoadStateValue(codegen, consumer_state_id_);
        // here we should be above to directly use
        codegen
            .builder
            .build_call(
                attach_tuple_fn,
                &[consumer_ptr.into(), tuple_ptr.into()],
                "consume_tupple",
            )
            .unwrap();
    }

    pub fn write_tuple_to_buffer<'ctx>(
        &self,
        codegen: &CodeGen<'ctx>,
        row: &Row<'_, 'ctx>,
        tuple_buffer: PointerValue<'ctx>,
    ) {
        let value_type = PrimValueProxy::get_llvm_type(codegen);

        for (idx, ai) in self.output_ais.iter().enumerate() {
            let value = row.derive_value_from_attribute(codegen, ai);
            let target_ptr = unsafe {
                codegen
                    .builder
                    .build_in_bounds_gep(
                        value_type,
                        tuple_buffer,
                        &[codegen.types.i32type.const_int(idx as u64, false)],
                        "value_ptr",
                    )
                    .unwrap()
            };
            // 1. ty_id (u8) at index 0
            let ty_id_ptr = codegen
                .builder
                .build_struct_gep(value_type, target_ptr, 0, "ty_id_ptr")
                .unwrap();
            codegen
                .builder
                .build_store(
                    ty_id_ptr,
                    codegen.types.i8type.const_int(ai.ty.ty_id as u64, false),
                )
                .unwrap();

            // 2. is_null (u8) at index 1
            let is_null_ptr = codegen
                .builder
                .build_struct_gep(value_type, target_ptr, 1, "is_null_ptr")
                .unwrap();
            codegen
                .builder
                .build_store(is_null_ptr, value.is_null.into_int_value())
                .unwrap();

            // 3. _padding ([u8; 6]) at index 2 (optional, can zero it)
            let padding_ptr = codegen
                .builder
                .build_struct_gep(value_type, target_ptr, 2, "padding_ptr")
                .unwrap();
            codegen
                .builder
                .build_store(padding_ptr, codegen.types.i64type.const_zero())
                .unwrap();

            // 4. data (PrimValueUnion) at index 3
            let data_ptr = codegen
                .builder
                .build_struct_gep(value_type, target_ptr, 3, "data_ptr")
                .unwrap();
            codegen.builder.build_store(data_ptr, value.value).unwrap();

            // 5. len (u64) at index 4
            let len_ptr = codegen
                .builder
                .build_struct_gep(value_type, target_ptr, 4, "len_ptr")
                .unwrap();
            codegen
                .builder
                .build_store(len_ptr, value.length.into_int_value())
                .unwrap();
            #[cfg(debug_assertions)]
            codegen.emit_print_bytes(
                target_ptr,
                codegen
                    .types
                    .i32type
                    .const_int(std::mem::size_of::<PrimValue>() as u64, false),
                "print_prim_value",
            );
        }
    }

    // Write the metadata (tuple start address, number of elements) for the tuple to the meta_buffer
    pub fn write_tuple_metadata<'ctx>(
        &self,
        codegen: &CodeGen<'ctx>,
        meta_buffer: PointerValue<'ctx>,
        tuple_ptr: PointerValue<'ctx>,
        values_num: IntValue<'ctx>,
    ) {
        // Set the values pointer to tuple_ptr offset 0 and len to tuple_ptr offset 1
        let values_ptr_ptr = codegen
            .builder
            .build_struct_gep(
                PrimTupleProxy::get_llvm_type(codegen),
                meta_buffer,
                0,
                "values_ptr",
            )
            .unwrap();

        // Store the tuple_buffer pointer to values_ptr
        codegen
            .builder
            .build_store(values_ptr_ptr, tuple_ptr)
            .unwrap();

        let len_ptr = codegen
            .builder
            .build_struct_gep(
                PrimTupleProxy::get_llvm_type(codegen),
                meta_buffer,
                1,
                "len_ptr",
            )
            .unwrap();
        // Store the number of values to len_ptr
        codegen.builder.build_store(len_ptr, values_num).unwrap();
    }

    /// consume a batch, for each row delegate to consumer_result
    pub fn consume_batch<'a, 'ctx>(&self, context: &ConsumerContext<'a, 'ctx>, batch: &RowBatch<'ctx>) {
        batch.iterate(context.compilation_context.codegen.as_ref(), |row| {
            self.consume_result(context, &row);
        });
    }

    /// Reclaim all memory owned by the buffer, including deep freeing of strings/binaries.
    pub fn free_buffer(&self) {
        let mut buffer = self.buffer.borrow_mut();
        for tuple in buffer.iter() {
            unsafe {
                Self::free_primtuple_deep(*tuple);
            }
        }
        buffer.clear();
    }

    /// free the underlying memory (string and binary) of PrimTuple
    /// Rememember when we pull out values from llvm ir world to Rust
    /// world, we copied the string and binary and leaked them
    /// here we need to do free them;
    pub unsafe fn free_primtuple_deep(tuple: PrimTuple) {
        if tuple.values.is_null() || tuple.len == 0 {
            return;
        }
        let mut prims = Vec::from_raw_parts(
            tuple.values as *mut PrimValue,
            tuple.len as usize,
            tuple.len as usize,
        );
        for pv in &mut prims {
            if (pv.ty_id == 12 || pv.ty_id == 13) && pv.is_null == 0 {
                let ptr = pv.data.ptr;
                let len = pv.len as usize;
                if !ptr.is_null() && len > 0 {
                    let _ = Vec::from_raw_parts(ptr as *mut u8, len, len);
                }
            }
        }
        // Dropping prims here frees the PrimValue array
    }
}

// use with caution, as it assumes the RecordBatch is not dropped before the conversion
impl From<&RecordBatch> for BufferingConsumer {
    fn from(batch: &RecordBatch) -> Self {
        let schema = batch.schema.clone();
        let buffer = RefCell::new(batch.rows.iter().map(|row| row.into()).collect());
        let output_ais = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| AttributeInfo {
                ty: field.data_type().into(),
                idx,
                name: Some(field.name().clone()),
            })
            .collect();
        BufferingConsumer {
            schema,
            buffer,
            output_ais,
            row_map: HashMap::new(), // Initialize an empty row map
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ptr::null;
    use std::rc::Rc;
    use std::sync::Arc;

    use inkwell::context::Context;
    use log::debug;

    use super::*;
    use crate::common::schema::test::make_schema;
    use crate::common::types::{DataType, DataValue};
    use crate::compiler::lang::FunctionBuilder;
    use crate::compiler::operator::{Accessor, TableAttributeAccessor};
    use crate::compiler::types::tests::{make_primtuple, TestValue};
    use crate::compiler::types::PrimValue;
    use crate::compiler::{
        CodeGen, CompilationContext, FunctionArguments, Pipeline, PtrPtrVoidFnType, PtrVoidFnType,
    };
    use crate::storage::memory::MemTable;
    use crate::storage::Table;

    #[test]
    /// A simple test to invoke a Rust function from LLVM JIT to add a tuple
    fn test_add_tuple_to_buffer() {
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test");
        codegen.initialize();

        let fn_name = "test_add_tuple_to_buffer";
        let args = vec![
            ("consumer_ptr".to_owned(), codegen.types.ptrtype.into()),
            ("tuple_ptr".to_owned(), codegen.types.ptrtype.into()),
        ];
        let mut fn_builder =
            FunctionBuilder::new(&codegen, fn_name, codegen.types.voidtype, args.as_slice());

        let consumer_ptr = fn_builder.get_arg_by_index(0).unwrap().into_pointer_value();
        let tuple_ptr = fn_builder.get_arg_by_index(1).unwrap().into_pointer_value();

        // Call the FFI function
        let add_fn = codegen
            .module
            .get_function("buffering_consumer_add")
            .unwrap();
        codegen
            .builder
            .build_call(add_fn, &[consumer_ptr.into(), tuple_ptr.into()], "")
            .unwrap();

        // Finish the function
        fn_builder.return_and_finish(&codegen, None);

        codegen.module.verify().unwrap();

        let test_func =
            unsafe { codegen.engine.get_function::<PtrPtrVoidFnType>(fn_name) }.unwrap();

        let test_tuple = make_primtuple(&[
            TestValue::Int64(0),
            TestValue::Float32(100.0),
            TestValue::Utf8Lit("user0"),
            TestValue::Boolean(true),
        ]);

        // Create a schema for the tuple, consisting of the types of each element
        let schema = make_schema(&[
            DataType::Int64,
            DataType::Float32,
            DataType::Utf8,
            DataType::Boolean,
        ]);

        // Create a BufferingConsumer
        let mut consumer = BufferingConsumer::new(SchemaRef::new(schema));

        // codegen.show_llvm_ir();
        unsafe {
            test_func.call(
                &mut consumer as *mut BufferingConsumer as *const i8,
                &test_tuple as *const PrimTuple as *const i8,
            );
        }

        let ans = consumer.to_record_batch().unwrap();
        debug!("this data is: {:?}", ans);

        // Check if the data was added correctly
        assert_eq!(ans.rows.len(), 1);
        assert_eq!(ans.rows[0].len(), 4);
        assert_eq!(ans.rows[0][0], DataValue::Int64(Some(0)));
        assert_eq!(ans.rows[0][1], DataValue::Float32(Some(100.0)));
        assert_eq!(ans.rows[0][2], DataValue::Utf8(Some("user0".to_string())));
        assert_eq!(ans.rows[0][3], DataValue::Boolean(Some(true)));
        // Free the test_tuple by reclaiming the memory
        unsafe {
            let _ = Vec::from_raw_parts(
                test_tuple.values as *mut PrimValue,
                test_tuple.len as usize,
                test_tuple.len as usize,
            );
        }
    }

    #[test]
    fn test_consume_batch() {
        // Setup: schema, data, and context
        let schema = make_schema(&[
            DataType::Int64,
            DataType::Float32,
            DataType::Utf8,
            DataType::Boolean,
        ]);
        let schema_ref = SchemaRef::new(schema.clone());
        let record_batch = RecordBatch::new(
            schema_ref.clone(),
            vec![
                vec![
                    DataValue::Int64(Some(999)),
                    DataValue::Float32(Some(6.2821)),
                    DataValue::Utf8(Some("foofoofoo".to_string())),
                    DataValue::Boolean(Some(true)),
                ],
                vec![
                    DataValue::Int64(Some(888)),
                    DataValue::Float32(Some(2.71)),
                    DataValue::Utf8(Some("barbar".to_string())),
                    DataValue::Boolean(Some(false)),
                ],
            ],
        );
        let mem_table = MemTable::try_new(schema_ref.clone(), vec![record_batch]).unwrap();
        // to simulate the case in a LogicalPlan where we only have Arc<dyn Table>
        let mem_table_ref: Arc<dyn Table> = Arc::new(mem_table);

        // Setup compilationContext and CodeGen
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test_consume_batch");
        codegen.initialize();
        let codegen_ref = Rc::new(codegen);

        let mut query_state = crate::compiler::query_state::QueryState::new();
        // Register the buffering consumer in the query state
        query_state.register_state(
            "executor_context",
            codegen_ref.as_ref().types.ptrtype.into(),
        );
        query_state.register_state(
            "buffering_consumer",
            codegen_ref.as_ref().types.ptrtype.into(),
        );
        // Register the executor context in the query state
        query_state.finalize_type(codegen_ref.as_ref());

        // Create a function builder for the test function
        let fn_name = "test_consume_batch";
        FunctionBuilder::new_and_push_to_stack(
            codegen_ref.as_ref(),
            fn_name,
            codegen_ref.as_ref().types.voidtype,
            &[(
                "query_state".to_owned(),
                codegen_ref.as_ref().types.ptrtype.into(),
            )],
        );

        let result_consumer = Rc::new(BufferingConsumer::new(schema_ref.clone()));
        let mut table_row_cache = BufferingConsumer::new(schema_ref.clone());

        debug!(
            "the address of the buffer in consumer before passing to llvm: {:?}",
            result_consumer.buffer.as_ptr()
        );

        // Create a CompilationContext with the codegen and result consumer
        let compilation_context = CompilationContext::new(
            codegen_ref.clone(),
            Rc::new(RefCell::new(query_state)),
            result_consumer.clone(),
        );

        let _test_pipeline = Rc::new(RefCell::new(Pipeline {
            operators: vec![],
            //compilation_context: compilation_context_ref.clone(),
            current_op_index: 0, // Start with the first operator
            id: 0, // unique id for the pipeline
        }));

        //let consumer_ctx = ConsumerContext {
        //    compilation_context: &compilation_context,
        //    pipeline_id: 0,
        //};

        let consumer_ctx = ConsumerContext::new(&compilation_context, 0);

        // Build TableAttributeAccessors for each column
        let mut attribute_accessors = HashMap::new();
        // Get a raw pointer from the Arc<dyn Table>
        let raw_table_ptr = mem_table_ref
            .as_ref()
            .as_any()
            .downcast_ref::<MemTable>()
            .unwrap() as *const MemTable;
        for (idx, field) in schema.fields.iter().enumerate() {
            let ai = AttributeInfo {
                ty: field.data_type().into(),
                idx,
                name: Some(field.name().clone()),
            };
            let accessor: Box<dyn Accessor<'_>> = Box::new(TableAttributeAccessor {
                attribute: ai.clone(),
                store: raw_table_ptr,
                row_cache: &mut table_row_cache as *mut BufferingConsumer,
            });
            attribute_accessors.insert(ai, accessor);
        }

        // Build a RowBatch with 2 rows
        let i64_type = codegen_ref.types.i64type;
        let tid_start = i64_type.const_int(0, false).into();
        let tid_end = i64_type.const_int(2, false).into();
        let num_rows = i64_type.const_int(2, false).into();
        let row_batch = RowBatch {
            tid_start,
            tid_end,
            num_rows,
            attribute_accessors,
        };

        result_consumer
            .clone()
            .consume_batch(&consumer_ctx, &row_batch);
        // Finish the function
        let mut fn_builder =
            FunctionBuilder::get_current_function_builder_mut(codegen_ref.as_ref());
        fn_builder.return_and_finish(codegen_ref.as_ref(), None);
        std::mem::drop(fn_builder); // End the borrow before verification

        codegen_ref.module.verify().unwrap();

        // codegen_ref.show_llvm_ir();

        let test_func = unsafe {
            codegen_ref
                .as_ref()
                .engine
                .get_function::<PtrVoidFnType>(fn_name)
        }
        .unwrap();

        let codegen: &CodeGen<'_> = codegen_ref.as_ref();
        let target_data = codegen.engine.get_target_data();
        let query_state_type = consumer_ctx.get_query_state_type();
        let query_state_size = target_data.get_abi_size(&query_state_type);
        debug!("QueryState size: {:?}", query_state_size);

        // allocate a block of memory for the query state in Rust world and set 1st 2 params
        // then invoke the compiled function
        let mut param_data = vec![0u8; query_state_size as usize];
        let param_ptr = param_data.as_mut_ptr();
        let func_args: *mut FunctionArguments = param_ptr as *mut FunctionArguments;
        unsafe {
            (*func_args).executor_context = null();
            let raw_ptr = Rc::as_ptr(&result_consumer) as *const BufferingConsumer;
            (*func_args).consumer_arg = raw_ptr as *const i8;
            debug!("consumer_arg: {:?}", (*func_args).consumer_arg);

            test_func.call(param_ptr as *const i8);
        }

        let batch = result_consumer.to_record_batch().unwrap();
        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0][0], DataValue::Int64(Some(999)));
        assert_eq!(batch.rows[0][1], DataValue::Float32(Some(6.2821)));
        assert_eq!(
            batch.rows[0][2],
            DataValue::Utf8(Some("foofoofoo".to_string()))
        );
        assert_eq!(batch.rows[0][3], DataValue::Boolean(Some(true)));
        assert_eq!(batch.rows[1][0], DataValue::Int64(Some(888)));
        assert_eq!(batch.rows[1][1], DataValue::Float32(Some(2.71)));
        assert_eq!(
            batch.rows[1][2],
            DataValue::Utf8(Some("barbar".to_string()))
        );
        assert_eq!(batch.rows[1][3], DataValue::Boolean(Some(false)));
        result_consumer.free_buffer();
        table_row_cache.free_buffer();
    }
}
