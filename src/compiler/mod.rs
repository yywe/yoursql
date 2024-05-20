pub mod serialization;
use inkwell::values::{BasicMetadataValueEnum, PointerValue, BasicValueEnum};
use anyhow::anyhow;
use crate::compiler::serialization::serialize_batch;
use tokio::runtime::Runtime;
use futures::TryStreamExt;
use std::ffi::CString;
use std::collections::HashMap;
use std::ffi::CStr;
use inkwell::values::IntValue;
use inkwell::context::Context;
use inkwell::OptimizationLevel;
use inkwell::execution_engine::{ExecutionEngine, JitFunction};
use inkwell::module::Module;
use inkwell::AddressSpace;
use inkwell::builder::Builder;
use inkwell::values::BasicValue;
use llvm_sys::core::LLVMGetTypeKind;
use crate::common::schema::Schema;
use crate::common::types::DataType;
use inkwell::types::{StructType, BasicTypeEnum, PointerType, VoidType};
use anyhow::Result;
use crate::common::record_batch::RecordBatch;
use crate::expr::logical_plan::LogicalPlan;
use crate::session::{SessionState, SessionContext};
use crate::common::types::DataValue;
use crate::common::schema::Field;
use std::sync::Arc;
use inkwell::module::Linkage;
use crate::compiler::serialization::deserialize_batch;
use crate::compiler::serialization::load_table_data;
use inkwell::types::IntType;



// define the function type of the compiled query, it takes raw pointer of session state
// and return a raw pointer of the result, this is needed since the LLVM gear will need to 
// interact with the Rust runtime. <TODO: change this later, we should just need one pointer>
type QueryPlan = unsafe extern "C" fn(*const i8,  *const i8, *const i8) -> *const i8;

/// define the batch in LLVM IR
pub struct LLVMRowBatch<'ctx> {
    pub size: IntValue<'ctx>, // the total size of the memory block
    pub row_ptrs: PointerValue<'ctx>, // array, pointers to rows
    pub num_rows: IntValue<'ctx>, // total number of rows
}

pub struct LLVMValue<'ctx> {
    pub value: BasicValueEnum<'ctx>, // the value of the row
    pub data_type: DataType, // the data type of the value
    pub size: Option<IntValue<'ctx>>, // the size of the value (for string, include the null terminator)
    pub null_rep: IntValue<'ctx>, // the null representation of the value
}

pub struct LLVMRow<'ctx> {
    pub values: Vec<LLVMValue<'ctx>>, // the value of the row
    pub schema: Arc<Schema>, // the schema of the row
}

pub struct ResultCollector {
    pub schema: Option<Arc<Schema>>,
    pub buffer: Vec<*const i8>
}

//TODO:  we need to figure out how to bind the schema. now just pending.
impl ResultCollector {
    pub fn new() -> ResultCollector {
        ResultCollector {
            schema: None,
            buffer: vec![]
        }
    }
    pub fn collect(&mut self, ptr: *const i8) {
        self.buffer.push(ptr);
    }

    pub fn get_result(&self) -> Result<RecordBatch> {
        let schema = self.schema.as_ref().unwrap().clone();
        let mut rows = vec![];
        for ptr in self.buffer.iter() {

            let ans_ptr = *ptr;
            let size_slice = unsafe {std::slice::from_raw_parts(ans_ptr as *const u8, 4)};
            println!("the raw bytes at {:?} are: {:?}",ans_ptr,  size_slice);

            let len=u32::from_le_bytes(size_slice.to_owned().try_into().unwrap());
            println!("the size of the data: {} at {:?}", len, ans_ptr);
            if len == 0 {
                continue;
            }
            
            let data: &[u8] = unsafe { std::slice::from_raw_parts(ans_ptr  as *const u8, len as usize) };
        
            let batch = deserialize_batch(schema.clone(), data)?;

            //unsafe {
            //    libc::free(*ptr as *mut libc::c_void); //note this requires the memory allocated using malloc
            //}
            // add rows in batch to rows
            for row in batch.rows.iter() {
                rows.push(row.clone());
            }
            
        }

        Ok(RecordBatch {
            schema,
            rows
        })

    }
}

pub extern "C" fn deliver_row(ptr: *const i8, collector: *mut i8) {
    let collector = unsafe {&mut *(collector as *mut ResultCollector)};
    collector.collect(ptr);
}


pub struct TypeWrapper<'ctx> {
    pub i32type: IntType<'ctx>,
    pub i64type: IntType<'ctx>,
    pub i8type: IntType<'ctx>,
    pub i8ptrtype: PointerType<'ctx>,
    pub i32ptrtype: PointerType<'ctx>,
    pub i64ptrtype: PointerType<'ctx>,
    pub i16type: IntType<'ctx>,
    pub i16ptrtype: PointerType<'ctx>,
    pub voidtype: VoidType<'ctx>,

}

pub trait OperatorTranslator<'ctx> {
    fn produce(&self) -> Result<()>;
    fn consume(&self, row: &LLVMRow<'ctx>) -> Result<()>;
}

pub struct Pipeline<'ctx> {
    pub operators: Vec<Box<dyn OperatorTranslator<'ctx>>>,
}


pub struct QueryCompiler<'ctx> {
    pub context: &'ctx Context,
    pub module: Module<'ctx>,
    pub builder: Builder<'ctx>,
    pub engine: ExecutionEngine<'ctx>,
    pub types: TypeWrapper<'ctx>,

}

impl<'ctx> QueryCompiler<'ctx> {
    pub fn new(context: &'ctx Context, name: &str) -> QueryCompiler<'ctx> {
        let module = context.create_module(name);
        let builder = context.create_builder();
        let engine = module.create_jit_execution_engine(OptimizationLevel::None).unwrap();
        let types = TypeWrapper::new(context);
        QueryCompiler {
            context,
            module,
            builder,
            engine,
            types,
        }
    }



   



    /// build the query plan
    /// TODO: split to compile_plan_internal and get the jit function pointer
    pub fn compile_plan<'a>(&'a self, plan: &LogicalPlan, collector: &mut ResultCollector) -> Result<JitFunction<QueryPlan>> {
        // for now define a test function just to test the LLVM
        // create a function which takes a pointer of table_name and database_name and session_state pointer and return a pointer of RecordBatch
        let fn_type = self.types.i8ptrtype.fn_type(&[self.types.i8ptrtype.into(), self.types.i8ptrtype.into(), self.types.i8ptrtype.into()], false);
        let fn_value = self.module.add_function("plan", fn_type, None);
        let entry_bb = self.context.append_basic_block(fn_value, "entry");
        self.builder.position_at_end(entry_bb);
        //actual entry of the plan, simply call the get_load_table_data
        // get the input parameters
        let table_name = fn_value.get_first_param().unwrap().into_pointer_value();
        let database_name = fn_value.get_nth_param(1).unwrap().into_pointer_value();
        let session = fn_value.get_nth_param(2).unwrap().into_pointer_value();

        // print the params
        //self.emit_printf_call("table_name: %s, database_name: %s\n", &[table_name.into(), database_name.into()]);

        let loadtbl_func = self.module.get_function("load_table_data").unwrap();
        let call_result = self.builder.build_call(loadtbl_func, &[table_name.into(),  database_name.into() , session.into()], "").unwrap();  
        let result = self.builder.build_pointer_cast(call_result.try_as_basic_value().left().unwrap().into_pointer_value(), self.types.i8ptrtype, "").unwrap();
        
        //load table data finished. now should deserialize the data into a batch

        self.deserialize_batch_test(result).unwrap();

        println!("=============TEST====================");
        let batch_meta = self.load_batch_meta(result).unwrap();
        //get the table scan schema
        let p = match plan {
            LogicalPlan::Projection(p) => p,
            _=> unimplemented!(),
        };
        let scan = match p.input.as_ref() {
            LogicalPlan::TableScan(s) => s,
            _=> unimplemented!(),
        };
        let schema = scan.projected_schema.clone();
        println!("the scan schema is {:?}", schema);
        collector.schema = Some(schema.clone());


        //the row pointer are stored in batch_meta.row_ptrs with num_rows
        //let's build anthoer loop to load the data

       
        let ti = self.builder.build_alloca(self.types.i32type, "ti")?;
        self.builder.build_store(ti, self.types.i32type.const_zero())?;

        let new_result_ptr = self.types.i8ptrtype.const_null();

        // create a loop. TODO: here needs function, we should have a better way to associate function with builder.
        let cur_function = self.builder.get_insert_block().unwrap().get_parent().unwrap();
        let tloop_head = self.context.append_basic_block(cur_function, "tloop_head");
        let tloop_body = self.context.append_basic_block(cur_function, "tloop_body");
        let tloop_end = self.context.append_basic_block(cur_function, "tloop_end");

        self.builder.build_unconditional_branch(tloop_head)?;
        self.builder.position_at_end(tloop_head);
        let ti_val = self.builder.build_load(self.types.i32type, ti, "").unwrap().into_int_value();
        let tcond = self.builder.build_int_compare(inkwell::IntPredicate::ULT, ti_val, batch_meta.num_rows, "loop_cond").unwrap();
        self.builder.build_conditional_branch(tcond, tloop_body, tloop_end)?;

        self.builder.position_at_end(tloop_body);
        let row_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8ptrtype, batch_meta.row_ptrs, &[ti_val], "")?};
        // load the row pointer
        let row_ptr = self.builder.build_load(self.types.i8ptrtype, row_ptr, "")?.into_pointer_value();
        let row: LLVMRow<'_> = self.load_data_row(row_ptr, schema.clone()).unwrap();
        // now technically we should call consume of up layer operators of row
        // note now we are in the rust world and the loop is in the llvm world
        // it may sounds like we can save the row in a rust vec and later serialize it
        // however note that here in rust world we do not have loop at all (loop is the gnerated IR)
        // we have to deliver this row to the rust world eventually. 

        // seralize/deseralize a batch of multiple rows using llvm may be complex as we need to maintain the 
        // buffer size and the offset of each row.
        // for now we just seralize a single row and return the address of the row data

        // perhaps we should introduce (buffer)consumer now.
        
        // here is just poc. we will definitely need refactor later
        // the idea here is that we allocate a new memory block in heap, and serialize the row data to the new memory block

         //now let's serialize the row data
        // first let's calculate the size of the row data
        let mut row_size = self.types.i32type.const_int(0, false);
        for value in row.values.iter() {
            let value_size = match value.data_type {
                DataType::Int32 => self.types.i32type.const_int(4, false),
                DataType::Int64 => self.types.i32type.const_int(8, false),
                DataType::Float32 => self.types.i32type.const_int(4, false),
                DataType::Float64 => self.types.i32type.const_int(8, false),
                DataType::Utf8 => {
                    let size = value.size.unwrap();
                    let field_size = self.builder.build_int_add(self.types.i32type.const_int(4, false), size, "")?;
                    field_size
                }
                DataType::Binary => {
                    let size = value.size.unwrap();
                    let field_size = self.builder.build_int_add(self.types.i32type.const_int(4, false), size, "")?;
                    field_size
                }
                DataType::Boolean => self.types.i32type.const_int(1, false),
                DataType::Null => self.types.i32type.const_int(1, false),
                DataType::Int16 => self.types.i32type.const_int(2, false),
                DataType::Int8 => self.types.i32type.const_int(1, false),
                DataType::UInt8 => self.types.i32type.const_int(1, false),
                DataType::UInt16 => self.types.i32type.const_int(2, false),
                DataType::UInt32 => self.types.i32type.const_int(4, false),
                DataType::UInt64 => self.types.i32type.const_int(8, false),
                DataType::Date32 => self.types.i32type.const_int(4, false),
                DataType::Date64 => self.types.i32type.const_int(8, false),
                DataType::Time32Millisecond => self.types.i32type.const_int(4, false),
                DataType::Time64Nanosecond => self.types.i32type.const_int(8, false),
                DataType::Time32Second => self.types.i32type.const_int(4, false),
                DataType::Time64Microsecond => self.types.i32type.const_int(8, false),
            };
            row_size = self.builder.build_int_add(row_size, value_size, "row_size")?;
        }
        row_size = self.builder.build_int_add(row_size, self.types.i32type.const_int(row.values.len() as u64, false), "")?; // the null bitmap
        // allocate the memory block
        //let new_result = self.builder.build_array_malloc(self.types.i8type, row_size, "new_result")?;
        

        // serialize the row data
        // first write the total size of the row data. now equals = 4 + 4 + 4 + row_size (remember total_size:number_of_rows:row_offset:row_data)
        let total_size = self.builder.build_int_add(self.types.i32type.const_int(12, false), row_size, "total_size")?;

        let new_result = self.builder.build_call(self.module.get_function("malloc").unwrap(), &[total_size.into()], "new_result").unwrap().try_as_basic_value().left().unwrap().into_pointer_value(); 
        // set memory to 0
        self.builder.build_call(self.module.get_function("memset").unwrap(), &[new_result.into(), self.types.i32type.const_int(0, false).into(), total_size.into()], "")?;


        
        //TODO: check the size of row_size.
        self.emit_printf_call("$$$$$the address of new_result is-----> %p\n", &[new_result.into()]);



        self.emit_printf_call("$$$$the total size of the row data----->: %d\n", &[total_size.into()]);
        let total_size_ptr = new_result.const_cast(self.types.i32ptrtype);
        self.builder.build_store(total_size_ptr, total_size)?;

        // read it out and check.
        let total_sizetemp = self.builder.build_load(self.types.i32type, total_size_ptr, "")?.into_int_value();
        self.emit_printf_call("$$$$verified total size of the row data----->: %d\n", &[total_sizetemp.into()]);
        
        
        // write the number of rows
        let num_rows_ptr = unsafe {total_size_ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(1, false)])};
        self.builder.build_store(num_rows_ptr, self.types.i32type.const_int(1, false))?;
        // write the row offset
        let row_offset_ptr = unsafe {num_rows_ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(1, false)])};
        self.builder.build_store(row_offset_ptr, self.types.i32type.const_int(12, false))?;
        // write the row data
        let mut row_data_ptr = unsafe {new_result.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(12, false)])};
        // iterate the values and write the data
        //let mut cur_offset = self.types.i32type.const_int(0, false);
        for value in row.values.iter() {
            // write the value
            match value.data_type {
                DataType::Int32 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::Int64 => {
                    self.emit_printf_call("store int64 to address:%p\n", &[row_data_ptr.into()]);
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
                DataType::Float32 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_float_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::Float64 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_float_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
                DataType::Utf8 => {
                    let field_size = value.size.unwrap();
                    let field_size_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.emit_printf_call("$$$$$store size %d to addrss:%p\n", &[field_size.into(), field_size_ptr.into()]);
                    self.builder.build_store(field_size_ptr, field_size)?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                    let field_ptr = row_data_ptr;
                    self.emit_printf_call("$$$store string to addrss:%p from %p\n", &[field_ptr.into(),value.value.into()]);

                    
                    self.emit_printf_call("$$$$$$$$try to copy string:%s, size=%d\n", &[value.value.into(),field_size.into()]);
                    
                    
                   self.builder.build_memcpy(field_ptr, 1, value.value.into_pointer_value(), 1, self.types.i32type.const_int(4, false))?;
                    //self.builder.build_call(self.module.get_function("strcpy").unwrap(), &[field_ptr.into(), value.value.into()], "")?;
                    

                    /* 
                    let tchar = self.types.i8type.const_int(10, false);
                    self.builder.build_store(field_ptr, tchar)?;
                    let field_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, field_ptr, &[self.types.i32type.const_int(1, false)], "")?};
                    self.builder.build_store(field_ptr,  tchar)?;
                    let field_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, field_ptr, &[self.types.i32type.const_int(1, false)], "")?};
                    self.builder.build_store(field_ptr,  tchar)?;
                    let field_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, field_ptr, &[self.types.i32type.const_int(1, false)], "")?};
                    self.builder.build_store(field_ptr,  self.types.i8type.const_int(0, false))?;
                    */

                    // note the size of the string is known at runtime, we need to build gep to the next field
                    self.emit_printf_call("$$$after memcpy:%s\n", &[field_ptr.into()]);

                    
                    row_data_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, row_data_ptr, &[field_size], "")?};
                }
                DataType::Binary=>{
                    let field_size = value.size.unwrap();
                    let field_size_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_size_ptr, field_size)?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                    let field_ptr = row_data_ptr;


                    self.builder.build_memcpy(field_ptr, 4, value.value.into_pointer_value(), 4, field_size)?;
                    // note the size of the binary is also is known at runtime, we need to build gep to the next field
                    row_data_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, row_data_ptr, &[field_size], "")?};
                }
                DataType::Boolean => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i8ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
                }
                DataType::Null => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i8ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
                }
                DataType::Int16 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i16ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(2, false)])};
                }
                DataType::Int8 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i8ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
                }
                DataType::UInt8 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i8ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
                }
                DataType::UInt16 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i16ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(2, false)])};
                }
                DataType::UInt32 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::UInt64 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
                DataType::Date32 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::Date64 => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
                DataType::Time32Millisecond => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::Time64Nanosecond => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
                DataType::Time32Second => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i32ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(4, false)])};
                }
                DataType::Time64Microsecond => {
                    let field_ptr = row_data_ptr.const_cast(self.types.i64ptrtype);
                    self.builder.build_store(field_ptr, value.value.into_int_value())?;
                    row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(8, false)])};
                }
            }
        }
        // now append the null bitmap
        for value in row.values.iter() {
            let null_bitmap_ptr = row_data_ptr.const_cast(self.types.i8ptrtype);
            self.builder.build_store(null_bitmap_ptr, value.null_rep)?;
            row_data_ptr = unsafe {row_data_ptr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
        }
        self.emit_printf_call("$$$$$the address of row_data_ptr is %p\n", &[row_data_ptr.into()]);
    
        // now collect the result to the collector
        self.emit_printf_call("$$$$$the address of new_result is %p\n", &[new_result.into()]);
        // invoke the call of deliver_row
        let deliver_row_func = self.module.get_function("deliver_row").unwrap();
        let collector_ptr = collector as *mut ResultCollector as *const i8;
        let ptr_addr = self.types.i64type.const_int(collector_ptr as u64, false);
        let ptr_addr_llvm = self.builder.build_int_to_ptr(ptr_addr, self.types.i8ptrtype, "")?;
        self.builder.build_call(deliver_row_func, &[new_result.into(), ptr_addr_llvm.into()], "")?;




                // read it out and check.
        let total_sizetemp2 = self.builder.build_load(self.types.i32type, total_size_ptr, "")?.into_int_value();
        self.emit_printf_call("$$$$verified total size of the row data=====>>>>>>: %d\n", &[total_sizetemp2.into()]);
                
                

        

        //======end of serialize=== increment i now
        let next_ti = self.builder.build_int_add(ti_val, self.types.i32type.const_int(1, false), "next_ti")?;
        self.builder.build_store(ti, next_ti)?;
        // jump back to loop_head
        self.builder.build_unconditional_branch(tloop_head)?;

        // now the loop ends
        self.builder.position_at_end(tloop_end);



        //now serialize the data back to another memory address
        //let new_result = self.serialize_batch().unwrap();

        
        // let's return the new address
        //self.builder.build_return(Some(&result)).unwrap();

        let ret = self.types.i8ptrtype.const_null();
        self.builder.build_return(Some(&ret)).unwrap();

        //self.builder.build_return(Some(&new_result)).unwrap();


        // get the jit function and return it
        let maybe_fn = unsafe {self.engine.get_function::<QueryPlan>("plan")};
        match maybe_fn {
            Ok(f) => Ok(f),
            Err(err) => Err(anyhow!("{:?}", err))
        }

    }



    /// load the batch meta data, i.e. size, row pointer array, number of rows
    /// prepare for later row data access. dataptr is a pointer of i8 type.
    pub fn load_batch_meta(&self, dataptr: PointerValue<'ctx>) ->Result<LLVMRowBatch> {
        let sizeptr = dataptr.const_cast(self.types.i32ptrtype);
        let size = self.builder.build_load(self.types.i32type, sizeptr, "")?.into_int_value();
        let rownumptr = unsafe {sizeptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(1, false)])};
        let num_rows = self.builder.build_load(self.types.i32type, rownumptr, "")?.into_int_value();
        // allocate the row pointer array in heap. TODO: verify when to free the memory
        // note LLVM also support alloca array in stack, like VLA (variable length array) in C. but there are some risks of stack overflow
        // note the type is i8ptrtype, as the row pointer is pointer to row address.
        let row_ptrs = self.builder.build_array_malloc(self.types.i8ptrtype, num_rows, "")?;
        // do a llvm IR loop to load the row pointers, equal to the dataptr + offset


        // init. allocate temp var i and set i=0
        let i = self.builder.build_alloca(self.types.i32type, "i")?;
        self.builder.build_store(i, self.types.i32type.const_zero())?;

        // create a loop. TODO: here needs function, we should have a better way to associate function with builder.
        let cur_function = self.builder.get_insert_block().unwrap().get_parent().unwrap();
        let loop_head = self.context.append_basic_block(cur_function, "loop_head");
        let loop_body = self.context.append_basic_block(cur_function, "loop_body");
        let loop_end = self.context.append_basic_block(cur_function, "loop_end");

        self.builder.build_unconditional_branch(loop_head)?;
        self.builder.position_at_end(loop_head);
        let i_val = self.builder.build_load(self.types.i32type, i, "").unwrap().into_int_value();
        let cond = self.builder.build_int_compare(inkwell::IntPredicate::ULT, i_val, num_rows, "loop_cond").unwrap();
        self.builder.build_conditional_branch(cond, loop_body, loop_end)?;

        self.builder.position_at_end(loop_body);
        // note there are two offsets. one is the offset of the row pointer array, the other is the offset of the row data
        // calculate offset location. ith row, its offset is at: dataptr + 4 + 4 + i*4
        let offset_arr_idx = self.builder.build_int_mul(i_val, self.types.i32type.const_int(4, false), "").unwrap();
        let offset_arr_idx = self.builder.build_int_add(offset_arr_idx, self.types.i32type.const_int(8, false), "").unwrap();
        self.emit_printf_call("the offset array: %d\n", &[offset_arr_idx.into()]);
        let offset_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, dataptr, &[offset_arr_idx], "")?};
       
        // now load the offset value of the row data from the start of the memory block
        let offset_value = self.builder.build_load(self.types.i32type, offset_ptr, "")?.into_int_value();
        // print the offset value
        self.emit_printf_call("the offset of the row data: %d\n", &[offset_value.into()]);
        // now add the offset to the memory start to get the row data address
        let row_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, dataptr, &[offset_value], "")?};
        // print the row data address
        self.emit_printf_call("the address of the row data: %p\n", &[row_ptr.into()]);
        
        // this is the dynamic heap array storing the row pointers
        let row_ptr_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8ptrtype, row_ptrs, &[i_val], "")?};
        self.builder.build_store(row_ptr_addr, row_ptr)?;

        //--- Test code. Test if the row_ptr_addr is correct
        self.emit_printf_call("the address of the row_ptr_addr: %p\n", &[row_ptr_addr.into()]);
        //cast the row_ptr_addr to 64 bit pointer as the column is int
        //let row_ptr_addr = row_ptr_addr.const_cast(self.types.i64type.ptr_type(AddressSpace::default()));
        // load the value at row_ptr_addr  
        let row_ptr_value = self.builder.build_load(self.types.i8ptrtype, row_ptr_addr, "")?.into_pointer_value();
        //print
        self.emit_printf_call("verify the value of the row_ptr_addr: %p\n", &[row_ptr_value.into()]);
        // load the 1st value, which is the id (int64)
        let row_ptr_value = row_ptr_value.const_cast(self.types.i64type.ptr_type(AddressSpace::default()));
        let row_ptr_value = self.builder.build_load(self.types.i64type, row_ptr_value, "")?.into_int_value();
        //print
        self.emit_printf_call("verify the value of at row_ptr_addr: %d\n", &[row_ptr_value.into()]);
        //--- Test code end



        // increment i
        let next_i = self.builder.build_int_add(i_val, self.types.i32type.const_int(1, false), "next_i")?;
        self.builder.build_store(i, next_i)?;

        // jump back to loop_head
        self.builder.build_unconditional_branch(loop_head)?;

        // now the loop ends
        self.builder.position_at_end(loop_end);

        // print the size, num_rows and row_ptrs
        self.emit_printf_call("########size: %d, num_rows: %d######\n", &[size.into(), num_rows.into()]);

        // return the LLVMRowBatch
        Ok(LLVMRowBatch {
            size,
            row_ptrs,
            num_rows,
        })
    }


    
    pub fn load_data_row<'a, 'b:'a>(&'a self, row_ptr: PointerValue<'b>, schema: Arc<Schema>) -> Result<LLVMRow> {
        let mut values = vec![];
        let mut cur_addr = row_ptr;
        
        for field in schema.fields.iter() {
            let dtype = field.data_type();
            match dtype {
                DataType::Int32 => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Int32,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                   cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::Int64 => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Int64,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
                DataType::Float32 => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Float32,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::Float64 => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Float64,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
                DataType::Null => {
                    values.push(LLVMValue {
                        value: self.types.i32type.const_int(0, false).into(),
                        data_type: DataType::Null,
                        size: None,
                        null_rep: self.types.i32type.const_int(1, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(1, false)], "")?};
                }
                DataType::Boolean => {
                    let field_ptr = cur_addr.const_cast(self.types.i8ptrtype);
                    let value = self.builder.build_load(self.types.i8type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Boolean,
                        size: None,
                        null_rep: self.types.i8type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(1, false)], "")?};
                }
                DataType::Utf8 => {
                    let field_size_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let field_size = self.builder.build_load(self.types.i32type, field_size_ptr, "").unwrap().into_int_value();
                    let field_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                    values.push(LLVMValue {
                        value: field_ptr.into(),
                        data_type: DataType::Utf8,
                        size: Some(field_size),
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    // add field_size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, field_ptr, &[field_size], "")?};
                }
                DataType::Binary => {
                    let field_size_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let field_size = self.builder.build_load(self.types.i32type, field_size_ptr, "").unwrap().into_int_value();
                    let field_ptr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                    values.push(LLVMValue {
                        value: field_ptr.into(),
                        data_type: DataType::Binary,
                        size: Some(field_size),
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    // add field_size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, field_ptr, &[field_size], "")?};
                }
                DataType::Date32 => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Date32,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::Date64 => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Date64,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
                DataType::Int8 => {
                    let field_ptr = cur_addr.const_cast(self.types.i8ptrtype);
                    let value = self.builder.build_load(self.types.i8type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Int8,
                        size: None,
                        null_rep: self.types.i8type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(1, false)], "")?};
                }
                DataType::Int16 => {
                    let field_ptr = cur_addr.const_cast(self.types.i16ptrtype);
                    let value = self.builder.build_load(self.types.i16type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Int16,
                        size: None,
                        null_rep: self.types.i16type.const_int(0, false),
                    });
                    // add teh size to the cur_addr
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(2, false)], "")?};
                }
                DataType::UInt8 => {
                    let field_ptr = cur_addr.const_cast(self.types.i8ptrtype);
                    let value = self.builder.build_load(self.types.i8type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::UInt8,
                        size: None,
                        null_rep: self.types.i8type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(1, false)], "")?};
                }
                DataType::UInt16 => {
                    let field_ptr = cur_addr.const_cast(self.types.i16ptrtype);
                    let value = self.builder.build_load(self.types.i16type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::UInt16,
                        size: None,
                        null_rep: self.types.i16type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(2, false)], "")?};
                }
                DataType::UInt32 => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::UInt32,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::UInt64 => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::UInt64,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
                DataType::Time32Millisecond => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Time32Millisecond,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::Time64Nanosecond => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Time64Nanosecond,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
                DataType::Time32Second => {
                    let field_ptr = cur_addr.const_cast(self.types.i32ptrtype);
                    let value = self.builder.build_load(self.types.i32type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Time32Second,
                        size: None,
                        null_rep: self.types.i32type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(4, false)], "")?};
                }
                DataType::Time64Microsecond => {
                    let field_ptr = cur_addr.const_cast(self.types.i64ptrtype);
                    let value = self.builder.build_load(self.types.i64type, field_ptr, "").unwrap();
                    values.push(LLVMValue {
                        value: value.into(),
                        data_type: DataType::Time64Microsecond,
                        size: None,
                        null_rep: self.types.i64type.const_int(0, false),
                    });
                    cur_addr = unsafe {self.builder.build_in_bounds_gep(self.types.i8type, cur_addr, &[self.types.i32type.const_int(8, false)], "")?};
                }
            }
        }
        // do another loop to patch the null values
        
        for (idx, _field) in schema.fields.iter().enumerate() {
            let field_ptr = cur_addr.const_cast(self.types.i8ptrtype);
            let value = self.builder.build_load(self.types.i8type, field_ptr, "").unwrap().into_int_value();
            values[idx].null_rep = value;
            //test code. print the data value
            
            match _field.data_type() {
                DataType::UInt64 | DataType::Int64 => {
                    self.emit_printf_call("the value of the field----->: %ld\n", &[values[idx].value.into()]);
                }
                DataType::Utf8 => {
                    self.emit_printf_call("the value of the field---->: %s\n", &[values[idx].value.into()]);
                }
                _=> {
                    self.emit_printf_call("error should not go to here for test: %d\n", &[values[idx].value.into()]);
                }
            }
        }
        Ok(LLVMRow { values: values, schema: schema })
    }

    pub fn load_row_ptrs(&self, batch: &LLVMRowBatch) -> Vec<PointerValue<'ctx>> {
        let mut row_ptrs = vec![];

        row_ptrs
    }

    /// the the authors machine is Intel and used little endian, so we directly read the u32
    /// for big endian, it will need to convert the bytes to u32, here did not do that yet
    /// also pay attention to the usage of const_in_bounds_gep(x,y). the offset calculated
    /// will be sizeof(x) * y. if x is a pointer type and y=1, will offset 8 bytes.
    pub fn deserialize_batch_test(&self, memaddr: PointerValue<'ctx>)-> Result<()>{

        // note memaddr is a pointer value with 8 bytes. here add 1 will offset 8 bytes.
        self.emit_printf_call("original pointer: %p\n", &[memaddr.into()]);
        let row_start =  unsafe {memaddr.const_in_bounds_gep(self.types.i8type, &[self.types.i32type.const_int(1, false)])};
        self.emit_printf_call("updated pointer: %p\n", &[row_start.into()]);

        // get a pointer from the memory address. cast to memaddr to u32 type
        // LLVM IR does not differentiate between signed and unsigned integers.
        let u32ptr=memaddr.const_cast(self.types.i32type.ptr_type(AddressSpace::default()));
        let u32val = self.builder.build_load(self.types.i32type, u32ptr, "")?.into_int_value();
        // print the total size
        // print the pointer location of u32val
        self.emit_printf_call("pointer location of u32val: %p\n", &[u32ptr.into()]);
        self.emit_printf_call("total size got in Llvm ir: %d\n", &[u32val.into()]);

        // now decode the number of rows
        //let rows_numptr = unsafe {memaddr.const_gep(memaddr.get_type(), &[self.types.i32type.const_int(4, false)])};
        //let rows_numptr = rows_numptr.const_cast(self.types.i32type.ptr_type(AddressSpace::default()));
        let rows_numptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(1, false)])};
        let rows_num = self.builder.build_load(self.types.i32type, rows_numptr, "")?.into_int_value();
        // print the number of rows
        self.emit_printf_call("pointer location of rows_numptr: %p\n", &[rows_numptr.into()]);
        self.emit_printf_call("number of rows got in Llvm ir: %d\n", &[rows_num.into()]);

        //simple test can we allocate array dynamic in stack?
        let stack_array = self.builder.build_array_alloca(self.types.i32type, rows_num, "stack_array").unwrap();

        // now get the 1st row offset.
        let row1_offset_ptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(2, false)])};
        let row1_offset = self.builder.build_load(self.types.i32type, row1_offset_ptr, "")?.into_int_value();
        // print the 1st row offset
        self.emit_printf_call("pointer location of row1_offset_ptr: %p\n", &[row1_offset_ptr.into()]);
        self.emit_printf_call("1st row offset got in Llvm ir: %d\n", &[row1_offset.into()]);
        // now get 1st row content. use schema for guide: create table tbl(id int, name varchar(20))
        // (65539, 'big')
        // the 1st row is 2, 'world'



        // now get the 1st row offset. (note this is a temp solution, should add offset to the start, here just use 4)
        let row1e1_offset_ptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(4, false)])};
        // cast to i64 type as int is 64 bit in SQL
        let row1e1_offset_ptr = row1e1_offset_ptr.const_cast(self.types.i64type.ptr_type(AddressSpace::default()));


        let row1e1 = self.builder.build_load(self.types.i32type, row1e1_offset_ptr, "")?.into_int_value();
        // print the 1st row offset
        self.emit_printf_call("pointer location of row1e1_offset_ptr: %p\n", &[row1e1_offset_ptr.into()]);
        self.emit_printf_call("row1e1: %d\n", &[row1e1.into()]);

        // now the the string value of 'big'
         // now get the 1st row offset. (note this is a temp solution, should add offset to the start, here just use 4)
         let row1e2_offset_ptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(6, false)])};
         // cast to back to i32 type for str size first
         let row1e2size = row1e2_offset_ptr.const_cast(self.types.i32type.ptr_type(AddressSpace::default()));
 
 
         let row1e1 = self.builder.build_load(self.types.i32type, row1e2size, "")?.into_int_value();
         // print the 1st row offset
         self.emit_printf_call("the address of the size: %p\n", &[row1e2size.into()]);
         self.emit_printf_call("the size of the string: %d\n", &[row1e1.into()]);
 
        // now print the string 'big'. note this is a c-string end with 0.
        let row1e2_offset_ptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(7, false)])};
        // cast to i8ptr type
        let row1e2_offset_ptr = row1e2_offset_ptr.const_cast(self.types.i8ptrtype.ptr_type(AddressSpace::default()));
        // print the string
        self.emit_printf_call("the address of the string: %p\n", &[row1e2_offset_ptr.into()]);
        self.emit_printf_call("the string: %s\n", &[row1e2_offset_ptr.into()]);

        //=====================test use start of the memory plus offset to get the data
        //let row1init = memaddr.const_cast(self.types.i8ptrtype.ptr_type(AddressSpace::default()));
        //self.emit_printf_call("===>>>the address of the start of the memory: %p\n", &[row1init.into()]);
        //self.emit_printf_call("the offset used is %d\n", &[row1_offset.into()]);
        let row_start =  unsafe {memaddr.const_in_bounds_gep(self.types.i8type, &[row1_offset])};
        
        let row_start = row_start.const_cast(self.types.i64type.ptr_type(AddressSpace::default()));
        let row1e1 = self.builder.build_load(self.types.i64type, row_start, "")?.into_int_value();
        // print the 1st row offset
        self.emit_printf_call("newnewnew===>>>pointer location of row1e1_offset_ptr: %p\n", &[row_start.into()]);
        self.emit_printf_call("newnewnew===>>>row1e1: %d\n", &[row1e1.into()]);

        //=======================
        // now get the 2nd row offset.
        let row2_offset_ptr = unsafe {u32ptr.const_in_bounds_gep(self.types.i32type, &[self.types.i32type.const_int(3, false)])};
        let row2_offset = self.builder.build_load(self.types.i32type, row2_offset_ptr, "")?.into_int_value();
        // print the 1st row offset
        self.emit_printf_call("pointer location of row2_offset_ptr: %p\n", &[row2_offset_ptr.into()]);
        self.emit_printf_call("2st row offset got in Llvm ir: %d\n", &[row2_offset.into()]);


        // now get 2nd row content.use schema for guide

        Ok(())
    }

    pub fn serialize_batch(&self)-> Result<PointerValue>{
        // get a pointer from the memory address

        unimplemented!()
    }


    pub fn execute(&self, func: JitFunction<QueryPlan>,  session: &SessionContext) -> Result<()/*RecordBatch*/> {
        let table_name = "tbl";
        let database_name = "master";

        // convert the string to CString and get the raw pointer
        let table_name = std::ffi::CString::new(table_name).unwrap();
        let database_name = std::ffi::CString::new(database_name).unwrap();

        let raw_session_ptr = session as *const SessionContext as *const i8;

        let ans_ptr = unsafe {
            func.call(table_name.as_ptr() as *const i8, database_name.as_ptr() as *const i8, raw_session_ptr)
        };
        // get the schema and then deserialize the RecordBatch
        let schema = session.database(database_name.to_str().unwrap()).unwrap().get_table_sync(table_name.to_str().unwrap()).unwrap().get_table();

        //TODO: deserialize the data using LLVM IR
        //TODO: serialize the data using LLVM IR

        //the 1st 4 bytes are the length of the buffer, note it is in little endian and serialized in u32

        //let size_slice = unsafe {std::slice::from_raw_parts(ans_ptr as *const u8, 4)};

        //let len=u32::from_le_bytes(size_slice.to_owned().try_into().unwrap());

        //let data: &[u8] = unsafe { std::slice::from_raw_parts(ans_ptr  as *const u8, len as usize) };
        
        //let ret = deserialize_batch(schema, data);
        //let ret = Err(anyhow!("test error"));

        // for 1st data exchange, we reclaim the memory, there may be memory leak.
        //unsafe {
        //    let _vec = Vec::from_raw_parts(ans_ptr as *mut i8, len, len);
        //};
        // free the memory passed in by the LLVM (which was allocated by amalloc call in load_table_data function)
        //unsafe {
        //   libc::free(ans_ptr as *mut libc::c_void); //note this requires the memory allocated using malloc
        //}
        //ret

        Ok(())

    }

    pub fn register_external_func(&self) {
        // Register for printf function
        let printf_type = self.types.i32type.fn_type(&[self.types.i8ptrtype.into()], true);
        self.module.add_function("printf", printf_type, Some(Linkage::External));

        // Register malloc function
        let malloc_type = self.types.i8ptrtype.fn_type(&[self.types.i64type.into()], false);
        self.module.add_function("malloc", malloc_type, Some(Linkage::External));

        // Registe memset function
        let memset_type = self.types.i8ptrtype.fn_type(&[self.types.i8ptrtype.into(), self.types.i32type.into(), self.types.i64type.into()], false);
        self.module.add_function("memset", memset_type, Some(Linkage::External));
        
        // Regiser for load_table_data function
        let load_table_type = self.types.i8ptrtype.fn_type(&[self.types.i8ptrtype.into(), self.types.i8ptrtype.into(), self.types.i8ptrtype.into()], false);
        let load_table_func = self.module.add_function("load_table_data", load_table_type, None);
        self.engine.add_global_mapping(&load_table_func, load_table_data as usize);

        // Register for deliver_row
        let deliver_row_type = self.types.voidtype.fn_type(&[self.types.i8ptrtype.into(), self.types.i8ptrtype.into()], false);
        let deliver_row_func = self.module.add_function("deliver_row", deliver_row_type, None);
        self.engine.add_global_mapping(&deliver_row_func, deliver_row as usize);
    }

    /// handy function to emit printf call for debug purpose.
    /// int printf(const char *format, ...)
    #[allow(dead_code)]
    fn emit_printf_call<'a,'b>(&'a self, format: &str, args: &[BasicMetadataValueEnum<'b>]) {
        let printf = self.module.get_function("printf").unwrap();        
        let pointer_value = self.builder.build_global_string_ptr(format, "").unwrap();
        let mut build_args = vec![pointer_value.as_basic_value_enum().into()];
        build_args.extend_from_slice(args);
        self.builder.build_call(printf, &build_args, "").unwrap();
    }

    /// show the LLVM IR code.
    #[allow(dead_code)]
    fn display(&self) {
        println!("---------------The LLVM IR---------------------");
        self.module.print_to_stderr();
        println!("---------------End of LLVM IR------------------")
    }

}


impl<'ctx> TypeWrapper<'ctx> {
    pub fn new(context: &'ctx Context) -> TypeWrapper<'ctx> {
        TypeWrapper {
            i32type: context.i32_type(),
            i64type: context.i64_type(),
            i8type: context.i8_type(),
            i8ptrtype: context.i8_type().ptr_type(AddressSpace::default()),
            i32ptrtype: context.i32_type().ptr_type(AddressSpace::default()),
            i64ptrtype: context.i64_type().ptr_type(AddressSpace::default()),
            i16type: context.i16_type(),
            i16ptrtype: context.i16_type().ptr_type(AddressSpace::default()),
            voidtype: context.void_type(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use std::time::Instant;
    use crate::parser::parse;
    use tokio::runtime::Runtime;

    //RUST_LOG=info cargo test --package yoursql --lib -- compiler::tests::test_llvm --exact --nocapture
    #[test]
    fn test_llvm() -> Result<()> {
        env_logger::init();
        let session = SessionContext::default();
        let context = Context::create();
        let compiler = QueryCompiler::new(&context, "test");
        compiler.register_external_func();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            session.state.read().run("create table tbl(id int, name varchar(20))").await.unwrap();
            session.state.read().run("insert into tbl values (2, 'worl'), (65539, 'big')").await.unwrap();
        });
        // get a logical plan from session
        let plan = rt.block_on(async {
            let statement = parse("select * from tbl").unwrap();
            let logical_plan = session.state.read().make_logical_plan(statement).await.unwrap();
            logical_plan
        });
        // compile the plan
        let start = Instant::now();
        let mut collector = ResultCollector::new();
        let func = compiler.compile_plan(&plan,&mut collector).unwrap();
        info!("Compliation time is: {:?}", start.elapsed());
        // display the IR
        compiler.display();
        // execute the plan
        let start = Instant::now();
        let batch = compiler.execute(func, &session)?;
        //info!("Execution  time is: {:?}", start.elapsed());
        //info!("the query result is \n {:?}",batch);

        for p in collector.buffer.iter() {
            info!("the query row addrs: {:?}\n",p);
        }

        info!("the final result is :{:?}", collector.get_result());

        Ok(())
    }
}
