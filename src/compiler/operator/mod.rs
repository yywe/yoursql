pub mod table_scan_translator;
pub mod projection_translator;
pub mod filter_translator;
use inkwell::values::{BasicValueEnum, IntValue, PointerValue};
use crate::compiler::lang::IfBuilder;
use crate::compiler::proxy::{PrimTupleProxy, PrimValueProxy};
use crate::compiler::types::{CGType, CGValue};
use crate::compiler::{BufferingConsumer, CodeGen, Row};
use crate::storage::memory::MemTable;

/// trait defines how to access a column value in a row
pub trait Accessor<'ctx> {
    fn access(&self, codegen: &CodeGen<'ctx>, row: &Row<'_, 'ctx>) -> CGValue<'ctx>;
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct AttributeInfo {
    pub ty: CGType,           // Type of the attribute
    pub idx: usize,           // Index of the attribute in the row
    pub name: Option<String>, // Optional name of the attribute
}

/// simply use the BufferingConsumer as the TableCache
pub type TableCache = BufferingConsumer;

pub struct TableAttributeAccessor {
    pub attribute: AttributeInfo, // which attribute to access
    pub store: *const MemTable,   // The memory table pointer
    pub row_cache: *mut TableCache, /* The location where the fetched row is store (in
                                   * Rust world) */
}

impl TableAttributeAccessor {
    pub fn new(
        attribute: AttributeInfo,
        store: *const MemTable,
        row_cache: *mut TableCache,
    ) -> Self {
        Self {
            attribute,
            store,
            row_cache,
        }
    }
    /// Given a pointer of a primitive value, read and parse to be a CGValue
    pub fn read_prim_value<'ctx>(
        &self,
        codegen: &CodeGen<'ctx>,
        element_ptr: PointerValue<'ctx>,
    ) -> CGValue<'ctx> {
        // is_null: u8 at index 1
        let is_null_ptr = codegen
            .builder
            .build_struct_gep(
                PrimValueProxy::get_llvm_type(codegen),
                element_ptr,
                1,
                "is_null_ptr",
            )
            .unwrap();

        let is_null = codegen
            .builder
            .build_load(codegen.types.i8type, is_null_ptr, "is_null_load")
            .unwrap();

        let data_ptr = codegen
            .builder
            .build_struct_gep(
                PrimValueProxy::get_llvm_type(codegen),
                element_ptr,
                3, // the data: PrimValueUnion is at index 3
                "data_ptr",
            )
            .unwrap();

        let len_ptr = codegen
            .builder
            .build_struct_gep(
                PrimValueProxy::get_llvm_type(codegen),
                element_ptr,
                4,
                "len_ptr",
            )
            .unwrap();

        let len_value = codegen
            .builder
            .build_load(codegen.types.i32type, len_ptr, "len_load")
            .unwrap()
            .into_int_value();

        let data: BasicValueEnum<'ctx> = match self.attribute.ty.ty_id {
            0 => {
                // Null type, no data
                codegen.context.i8_type().const_zero().into()
            }
            1 => {
                // Boolean: load int8
                codegen
                    .builder
                    .build_load(codegen.types.i8type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            2 => {
                // Float32: load float32
                codegen
                    .builder
                    .build_load(codegen.types.f32type, data_ptr, "data_load2")
                    .unwrap()
                    .into_float_value()
                    .into()
            }
            3 => {
                // Float64: load float64
                codegen
                    .builder
                    .build_load(codegen.types.f64type, data_ptr, "data_load")
                    .unwrap()
                    .into_float_value()
                    .into()
            }
            4 => {
                // Int8: load int8
                codegen
                    .builder
                    .build_load(codegen.types.i8type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            5 => {
                // Int16: load int16
                codegen
                    .builder
                    .build_load(codegen.types.i16type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            6 | 14 | 16 | 17 => {
                // Int32 (and Date32, Time32Second, Time32Millisecond): load int32
                codegen
                    .builder
                    .build_load(codegen.types.i32type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            7 | 15 | 18 | 19 => {
                // Int64 (and Date64, Time64Microsecond, Time64Nanosecond): load int64
                codegen
                    .builder
                    .build_load(codegen.types.i64type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            8 => {
                // UInt8: load uint8
                codegen
                    .builder
                    .build_load(codegen.types.i8type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            9 => {
                // UInt16: load uint16
                codegen
                    .builder
                    .build_load(codegen.types.i16type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            10 => {
                // UInt32: load uint32
                codegen
                    .builder
                    .build_load(codegen.types.i32type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            11 => {
                // UInt64: load uint64
                codegen
                    .builder
                    .build_load(codegen.types.i64type, data_ptr, "data_load")
                    .unwrap()
                    .into_int_value()
                    .into()
            }
            12 | 13 => {
                // Utf8 or Binary: the union stored a pointer, let's load the pointer
                codegen
                    .builder
                    .build_load(codegen.types.ptrtype, data_ptr, "data_load")
                    .unwrap()
                    .into_pointer_value()
                    .into()
            }
            _ => panic!("Unsupported type ID for PrimValue"),
        };
        let value = CGValue {
            ty: self.attribute.ty,
            value: data,
            length: len_value.into(),
            is_null: is_null,
            elem_addr: Some(element_ptr), // Store the element address for debugging
        };
        value
    }

    /// Read the tuple metadata (values pointer and length) from a PrimTuple pointer
    pub fn read_tuple_meta<'ctx>(
        &self,
        codegen: &CodeGen<'ctx>,
        tuple_ptr: PointerValue<'ctx>,
    ) -> (PointerValue<'ctx>, IntValue<'ctx>) {
        // Get the pointer to the values and the length of the tuple
        let prim_tuple_type = PrimTupleProxy::get_llvm_type(codegen);
        let values_ptr_ptr = codegen
            .builder
            .build_struct_gep(prim_tuple_type, tuple_ptr, 0, "tuple_values_ptr")
            .unwrap();
        let values_len_ptr = codegen
            .builder
            .build_struct_gep(prim_tuple_type, tuple_ptr, 1, "tuple_len")
            .unwrap();

        let values_ptr = codegen
            .builder
            .build_load(codegen.types.ptrtype, values_ptr_ptr, "values_ptr_load")
            .unwrap()
            .into_pointer_value();
        let len_value = codegen
            .builder
            .build_load(codegen.types.i32type, values_len_ptr, "tuple_len_load")
            .unwrap()
            .into_int_value();
        (values_ptr, len_value)
    }
}

/// Implement the access logic for table attributes
impl<'ctx> Accessor<'ctx> for TableAttributeAccessor {
    fn access(&self, codegen: &CodeGen<'ctx>, row: &Row<'_, 'ctx>) -> CGValue<'ctx> {
        let get_prim_tuple_from_mem_table_type_func = codegen
            .module
            .get_function("get_prim_tuple_from_mem_table")
            .unwrap();
        // pass relevant pointers to the function
        let table_store_ptr = codegen
            .types
            .i64type
            .const_int(self.store as u64, false)
            .const_to_pointer(codegen.types.ptrtype);

        let table_row_cache_ptr = codegen
            .types
            .i64type
            .const_int(self.row_cache as u64, false)
            .const_to_pointer(codegen.types.ptrtype);

        let tid_start_intv = row.get_owner_batch_tid_start().into_int_value();
        let batch_position_inv = row.batch_position.into_int_value();

        // Add the batch_position to the batch's tid_start to get the physical row id;
        let physical_row_id = codegen
            .builder
            .build_int_add(tid_start_intv, batch_position_inv, "physical_row_id")
            .unwrap();

        let call_result = codegen
            .builder
            .build_call(
                get_prim_tuple_from_mem_table_type_func,
                &[
                    table_store_ptr.into(),
                    physical_row_id.into(),
                    table_row_cache_ptr.into(),
                ],
                "fetch_row_data",
            )
            .unwrap();

        let prim_tuple_ptr = call_result
            .try_as_basic_value()
            .left()
            .unwrap()
            .into_pointer_value();

        #[cfg(debug_assertions)]
        codegen.emit_printf_call(
            "fetched row with physical row id: %d, table store pointer address in llvm: %p, primtuple pointer: %p\n",
            &[physical_row_id.into(), table_store_ptr.into(), prim_tuple_ptr.into()],
        );

        // Now we have the PrimTuple pointer, we can read the values pointer and length
        // from the PrimTuple pointer
        let (values_ptr, values_len) = self.read_tuple_meta(codegen, prim_tuple_ptr);

        // We need to move to the offset to the specified column index
        // Remember that variable in Rust world (when doing the llvm ir emit)
        // , it is constant in JIT world since the value is known at JIT compile time
        let column_idx = self.attribute.idx as u64;
        let column_offset = codegen.context.i32_type().const_int(column_idx, false);

        let in_bounds = codegen
            .builder
            .build_int_compare(
                inkwell::IntPredicate::ULT,
                column_offset,
                values_len,
                "column_in_bounds",
            )
            .unwrap();

        let ifb = IfBuilder::new(codegen, in_bounds, "column_bounds");
        let mut result: Option<CGValue> = None;
        ifb.then(codegen, || {
            // Ok block: continue normal codegen

            // Calculate the pointer to the specific column element
            // We use in-bounds GEP to ensure we don't go out of bounds
            // This is safe because we checked the bounds above
            // The element_ptr is the address of the PrimValue struct for the column
            let element_ptr = unsafe {
                codegen
                    .builder
                    .build_in_bounds_gep(
                        PrimValueProxy::get_llvm_type(codegen),
                        values_ptr,
                        &[column_offset],
                        "prim_value_element_ptr",
                    )
                    .unwrap()
            };
            let prim_value = self.read_prim_value(codegen, element_ptr);
            #[cfg(debug_assertions)]
            prim_value.debug_print(codegen);
            result = Some(prim_value);
        });
        ifb.otherwise(codegen, || {
            // Trap block: emit a call to llvm.trap or abort
            let trap_fn = codegen.module.get_function("llvm.trap").unwrap_or_else(|| {
                codegen.module.add_function(
                    "llvm.trap",
                    codegen.types.voidtype.fn_type(&[], false),
                    None,
                )
            });
            codegen.builder.build_call(trap_fn, &[], "trap").unwrap();
            codegen.builder.build_unreachable().unwrap();
        });
        ifb.after(codegen);
        result.expect("CGValue should be set in then branch")
    }
}
