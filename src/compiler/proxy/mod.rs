use std::ptr;

use inkwell::module::Linkage;
use inkwell::types::StructType;
use log::debug;
use crate::compiler::operator::TableCache;
use crate::compiler::types::{PrimTuple, PrimValue, PrimValueUnion};
use crate::compiler::{BufferingConsumer, CodeGen};
use crate::storage::memory::MemTable;

/// This module will be responsible to bridge the gap between the Rust code and the LLVM code
/// generation. It will provide the necessary types and functions to interact with the LLVM code
/// generation.

/// This is the function arguments struct, it has same memory layout as the query state
/// which is also the only function argument for llvm IR functions
#[repr(C)]
pub struct FunctionArguments {
    pub executor_context: *const i8,
    pub consumer_arg: *const i8,
    pub rest: [u8; 1], // Flexible array member
}

pub struct PrimTupleProxy;
impl PrimTupleProxy {
    pub const NAME: &str = "PrimTupleProxy";
    pub fn get_llvm_type<'ctx>(codegen: &CodeGen<'ctx>) -> StructType<'ctx> {
        codegen.lookup_type(Self::NAME).unwrap()
    }
    pub fn register_llvm_type<'ctx>(codegen: &CodeGen<'ctx>) {
        // 1st member of PrimTuple is a pointer to the data
        let ptr_type = codegen.types.ptrtype;
        // 2nd member is the number of tuples, u32
        let i32_type = codegen.types.i32type;
        codegen
            .context
            .opaque_struct_type(Self::NAME)
            .set_body(&[ptr_type.into(), i32_type.into()], false);
    }
}

pub struct PrimValueProxy;
impl PrimValueProxy {
    pub const NAME: &str = "PrimValueProxy";
    pub fn get_llvm_type<'ctx>(codegen: &CodeGen<'ctx>) -> StructType<'ctx> {
        codegen.lookup_type(Self::NAME).unwrap()
    }
    pub fn register_llvm_type<'ctx>(codegen: &CodeGen<'ctx>) {
        // 1. i8 for ty_id
        let i8_type_1 = codegen.types.i8type;
        // 2. i8 for is_null
        let i8_type_2 = codegen.types.i8type;
        // 3. 6 padding bytes
        let array_type_3 = codegen.types.i8type.array_type(6);
        // 4. the data itself, let it be an array of size of PrimValueUnion, 8 bytes;
        let array_type_4 = codegen
            .types
            .i8type
            .array_type(std::mem::size_of::<PrimValueUnion>() as u32);
        // 5. the length of the data for variable length data, u64
        let i32_type_5 = codegen.types.i64type;
        codegen.context.opaque_struct_type(Self::NAME).set_body(
            &[
                i8_type_1.into(),
                i8_type_2.into(),
                array_type_3.into(),
                array_type_4.into(),
                i32_type_5.into(),
            ],
            false,
        );
    }
}

/// This is the bridge between llvm world and rust world, llvm ir layer will prepare tuple_ptr
/// and then invoke this function, we need to do a deep copy of the tuple since the llvm tuple_ptr
/// may exist only in stack and get dropped after the function returns.
/// We will not use non-stack (heap) memory in llvm ir layer, although we can do
/// that, but that will complicate things as memory management can be messed up.
#[no_mangle]
pub extern "C" fn buffering_consumer_add(
    consumer_ptr: *mut BufferingConsumer,
    tuple_ptr: *const PrimTuple,
) {
    if consumer_ptr.is_null() || tuple_ptr.is_null() {
        return;
    }

    let consumer = unsafe { &mut *(consumer_ptr as *mut BufferingConsumer) };

    let tuple = unsafe { &*tuple_ptr };
    if tuple.values.is_null() || tuple.len == 0 {
        return;
    }

    debug!(
        "consumer pointer: {:?}, tuple pointer: {:?}, adding tuples with values: {:?}, length: {}",
        consumer_ptr, tuple_ptr, tuple.values, tuple.len
    );

    // SAFETY: tuple.values points to an array of PrimValue of length tuple.len
    let src_slice = unsafe { std::slice::from_raw_parts(tuple.values, tuple.len as usize) };
    let mut copied_values: Vec<PrimValue> = Vec::with_capacity(src_slice.len());

    #[cfg(debug_assertions)]
    for (i, pv) in src_slice.iter().enumerate() {
        debug!("PrimValue at index:{}, value:{:?}", i, pv);
    }

    for pv in src_slice {
        let mut pv_copy = *pv;
        // Deep copy for Utf8 or Binary
        if (pv.ty_id == 12 || pv.ty_id == 13) && pv.is_null == 0 {
            let ptr = unsafe { pv.data.ptr };
            let len = pv.len as usize;
            if !ptr.is_null() && len > 0 {
                // Copy the data into a new Rust-owned Vec<u8>
                let src = unsafe { std::slice::from_raw_parts(ptr, len) };
                let mut buf = Vec::with_capacity(len);
                buf.extend_from_slice(src);
                let new_ptr = buf.as_ptr();
                // Leak the buffer so it lives as long as needed (caller must reclaim)
                std::mem::forget(buf);
                pv_copy.data.ptr = new_ptr;
            } else {
                pv_copy.data.ptr = std::ptr::null();
                pv_copy.len = 0;
            }
        }
        copied_values.push(pv_copy);
    }

    // Leak the copied_values Vec to keep the memory alive (caller must reclaim)
    let values_ptr = copied_values.as_ptr();
    let len = copied_values.len() as u64;
    std::mem::forget(copied_values);

    let copied_tuple = PrimTuple {
        values: values_ptr,
        len,
    };
    let mut buffer = consumer.buffer.borrow_mut();
    buffer.push(copied_tuple);
    debug!(
        "successfully added tuple to consumer buffer, consumer buffer length: {}",
        buffer.len()
    );
}

/// This function will read the raw table data in Rust RecordBatch form
/// and then convert it to PrimTuple format for llvm consumption.
/// the PrimTuple will be saved to the buffer in executor context's buffer.
/// the params of this function should be resolved in the Rust part before
/// invoking this function in llvm ir layer, specifically, the buffering_consumer
/// should be one entry in the hash map of the executor context
#[no_mangle]
pub extern "C" fn get_prim_tuple_from_mem_table(
    table_ptr: *const MemTable,
    row_id: u64,
    row_cache: *mut TableCache,
) -> *const PrimTuple {
    if table_ptr.is_null() || row_cache.is_null() {
        return ptr::null();
    }
    let cache = unsafe { &mut *row_cache };
    if let Some(&cached_row_id) = cache.row_map.get(&row_id) {
        // If the row has been cached, return the cached PrimTuple
        debug!(
            "Row ID {} found in cache, returning cached PrimTuple at index {}",
            row_id, cached_row_id
        );
        let buffer = cache.buffer.borrow();
        if let Some(prim_tuple) = buffer.get(cached_row_id as usize) {
            return prim_tuple as *const PrimTuple;
        }
    }
    let table = unsafe { &*table_ptr };
    let mut remaining = row_id as usize;
    let mut found_row = None;
    let batches = table.batches.lock().unwrap();
    for batch in batches.iter() {
        if remaining < batch.rows.len() {
            found_row = Some(&batch.rows[remaining]);
            break;
        } else {
            remaining -= batch.rows.len();
        }
    }
    let row = match found_row {
        Some(r) => r,
        None => return ptr::null(),
    };

    // two things:
    // 1: the (string/binary) memory for PrimValue is not copied from DataValue
    // 2: it called std::mem::forget(prims); need to reclaim memory when done
    let prim_tuple: PrimTuple = row.into();
    // Deep copy the PrimTuple to ensure the string/binary data is valid even after the row is
    // dropped
    let prim_tuple_deep_copied = prim_tuple.deep_copy();

    cache.buffer.borrow_mut().push(prim_tuple_deep_copied);
    // update the row map with the cached row id
    let cache_index = cache.buffer.borrow().len() - 1;
    cache.row_map.insert(row_id, cache_index as u64);
    // now it's time to reclaim the temporary memory and release
    unsafe {
        let _ = Vec::from_raw_parts(
            prim_tuple.values as *mut PrimValue,
            prim_tuple.len as usize,
            prim_tuple.len as usize,
        );
        // Dropping this Vec will free the PrimValue array memory
    }
    // return the PrimTuple pointer, which is owned by the Rust executor context
    let buffer = cache.buffer.borrow();
    let ptr = unsafe { buffer.as_ptr().add(buffer.len() - 1) };
    ptr
}

impl<'ctx> CodeGen<'ctx> {
    // Register the proxy functions
    pub fn register_proxy_functions(&self) {
        // buffering_consumer_add
        let buffering_consumer_add_type = self.types.voidtype.fn_type(
            &[self.types.ptrtype.into(), self.types.ptrtype.into()],
            false,
        );
        let buffering_consumer_add_func =
            self.module
                .add_function("buffering_consumer_add", buffering_consumer_add_type, None);
        self.engine.add_global_mapping(
            &buffering_consumer_add_func,
            buffering_consumer_add as usize,
        );
        // get_prim_tuple_from_mem_table
        let get_prim_tuple_from_mem_table_type = self.types.ptrtype.fn_type(
            &[
                self.types.ptrtype.into(),
                self.types.i64type.into(),
                self.types.ptrtype.into(),
            ],
            false,
        );
        let get_prim_tuple_from_mem_table_func = self.module.add_function(
            "get_prim_tuple_from_mem_table",
            get_prim_tuple_from_mem_table_type,
            None,
        );
        self.engine.add_global_mapping(
            &get_prim_tuple_from_mem_table_func,
            get_prim_tuple_from_mem_table as usize,
        );
    }

    // Register the proxy struct types
    pub fn register_proxy_types(&self) {
        // ValueProxy::register_llvm_type(self);
        PrimTupleProxy::register_llvm_type(self);
        PrimValueProxy::register_llvm_type(self);
    }

    // Register standard library functions
    pub fn register_stdlib_functions(&self) {
        // printf
        let printf_type = self
            .types
            .i32type
            .fn_type(&[self.types.ptrtype.into()], true);
        self.module
            .add_function("printf", printf_type, Some(Linkage::External));

        // malloc
        let malloc_type = self
            .types
            .ptrtype
            .fn_type(&[self.types.i64type.into()], false);
        self.module
            .add_function("malloc", malloc_type, Some(Linkage::External));

        // memset
        let memset_type = self.types.ptrtype.fn_type(
            &[
                self.types.ptrtype.into(),
                self.types.i32type.into(),
                self.types.i64type.into(),
            ],
            false,
        );
        self.module
            .add_function("memset", memset_type, Some(Linkage::External));

        // memcpy
        let memcpy_type = self.types.ptrtype.fn_type(
            &[
                self.types.ptrtype.into(),
                self.types.ptrtype.into(),
                self.types.i64type.into(),
            ],
            false,
        );
        self.module
            .add_function("memcpy", memcpy_type, Some(Linkage::External));
    }
}
