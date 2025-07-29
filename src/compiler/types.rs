use std::collections::HashMap;
use std::fmt::Debug;

use inkwell::values::{BasicValueEnum, PointerValue};

use crate::common::types::{DataType, DataValue};
use crate::compiler::lang::{LoopVariable, WhileLoopBuilder};
use crate::compiler::operator::{Accessor, AttributeInfo};
use crate::compiler::{CodeGen, CompilationContextRef};

/// This is the types that will be involved in the code generation.
/// The 1st step is that data will be loaded into the `CGValue` struct, which is a
/// representation of the value in the LLVM IR. The `CGValue` will then be used
/// in the code generation process (include different types of operations). This requires
/// that if we need to call any Rust function, it has to accept primitive types;
///
/// Finally, when the Data is out, i.e, when we collect the result of rows from CGValue,
/// we will convert the CGValue back to PrimValue, which is the representation
/// using primitive types. The code generator will prepare PrimValue and then invoke a
/// Rust function, which will convert the PrimValue to DataValue in Rust world.
///
/// This is the last step of the query execution. except this last step,
/// all the data is in the LLVM IR world.
/// engine)

#[derive(Copy, Clone)]
#[repr(C)]
pub union PrimValueUnion {
    pub int8: i8,
    pub int16: i16,
    pub int32: i32,
    pub int64: i64,
    pub uint8: u8,
    pub uint16: u16,
    pub uint32: u32,
    pub uint64: u64,
    pub float32: f32,
    pub float64: f64,
    pub ptr: *const u8, // For variable-length data (Utf8, Binary)
}

/// The size of a value in primitive form.
/// IMPORTANT: we have to make the len a u64, so as to make the total size of PrimValue
/// 24 bytes and aligned with 8 bytes boundary. If using u32, total size will still be
/// 24 bytes although the actual size is 20 bytes.
/// If we do not align here, in the llvm IR, we may access the wrong data due to the
/// alignment issues.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct PrimValue {
    pub ty_id: u8,         // Type tag/discriminant
    pub is_null: u8,       // 1 = NULL, 0 = not null
    pub _padding: [u8; 6], // For alignment (optional, to make struct 16 bytes)
    pub data: PrimValueUnion,
    pub len: u64, // Length for variable-length data (only used when Utf8, Binary)
}

impl Debug for PrimValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_addr = &self.data as *const PrimValueUnion as usize;
        let len_addr: usize = &self.len as *const u64 as usize;
        let element_addr = self as *const PrimValue as usize;
        // Read the 8 bytes at the data field as a u64 (unsafe, but matches the union layout)
        let data_bytes =
            unsafe { std::slice::from_raw_parts(&self.data as *const _ as *const u8, 8) };
        write!(
            f,
            "PrimValue(ty_id: {}, is_null: {}, len: {}, element_addr: 0x{:x}, data_addr: 0x{:x}, len_addr:0x{:x}, data_bytes: [",
            self.ty_id, self.is_null, self.len, element_addr, data_addr, len_addr
        )?;
        for (i, b) in data_bytes.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{:02x}", b)?;
        }
        write!(f, "], ")?;
        // also print the data value based on the type id
        match self.ty_id {
            0 => write!(f, "DataValue: Null"),
            1 => write!(f, "DataValue: Boolean({})", unsafe { self.data.int8 != 0 }),
            2 => write!(f, "DataValue: Float32({})", unsafe { self.data.float32 }),
            3 => write!(f, "DataValue: Float64({})", unsafe { self.data.float64 }),
            4 => write!(f, "DataValue: Int8({})", unsafe { self.data.int8 }),
            5 => write!(f, "DataValue: Int16({})", unsafe { self.data.int16 }),
            6 => write!(f, "DataValue: Int32({})", unsafe { self.data.int32 }),
            7 => write!(f, "DataValue: Int64({})", unsafe { self.data.int64 }),
            8 => write!(f, "DataValue: UInt8({})", unsafe { self.data.uint8 }),
            9 => write!(f, "DataValue: UInt16({})", unsafe { self.data.uint16 }),
            10 => write!(f, "DataValue: UInt32({})", unsafe { self.data.uint32 }),
            11 => write!(f, "DataValue: UInt64({})", unsafe { self.data.uint64 }),
            12 => {
                let ptr = unsafe { self.data.ptr };
                if ptr.is_null() || self.len == 0 {
                    write!(f, "DataValue: Utf8(None)")
                } else {
                    let slice = unsafe { std::slice::from_raw_parts(ptr, self.len as usize) };
                    let s = String::from_utf8_lossy(slice);
                    write!(f, "DataValue: Utf8(Some({}))", s)
                }
            }
            13 => {
                let ptr = unsafe { self.data.ptr };
                if ptr.is_null() || self.len == 0 {
                    write!(f, "DataValue: Binary(None)")
                } else {
                    let slice = unsafe { std::slice::from_raw_parts(ptr, self.len as usize) };
                    let v = slice.to_vec();
                    write!(f, "DataValue: Binary(Some({:?}))", v)
                }
            }
            _ => write!(f, "DataValue: Unknown Type"),
        }?;
        write!(f, "])\n")?;

        // also write all the bytes of this PrimValue
        let all_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const _ as *const u8,
                std::mem::size_of::<PrimValue>(),
            )
        };
        write!(
            f,
            "All bytes of PrimValue at memory: 0x{:x}",
            self as *const _ as usize
        )?;
        for (i, b) in all_bytes.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{:02x}", b)?;
        }

        Ok(())
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct PrimTuple {
    pub values: *const PrimValue, // Pointer to an array of PrimValue
    pub len: u64,                 // Number of values in the tuple
}

impl PrimTuple {
    pub fn deep_copy(&self) -> Self {
        if self.values.is_null() || self.len == 0 {
            return PrimTuple {
                values: std::ptr::null(),
                len: 0,
            };
        }
        let slice = unsafe { std::slice::from_raw_parts(self.values, self.len as usize) };
        let mut prims: Vec<PrimValue> = Vec::with_capacity(self.len as usize);
        for pv in slice.iter() {
            let mut pv_copy = *pv;
            // Deep copy for Utf8 or Binary
            if (pv_copy.ty_id == 12 || pv_copy.ty_id == 13) && pv_copy.is_null == 0 {
                let ptr = unsafe { pv_copy.data.ptr };
                let len = pv_copy.len as usize;
                if !ptr.is_null() && len > 0 {
                    let src = unsafe { std::slice::from_raw_parts(ptr, len) };
                    let mut buf = Vec::with_capacity(len);
                    buf.extend_from_slice(src);
                    let new_ptr = buf.as_ptr();
                    std::mem::forget(buf);
                    pv_copy.data.ptr = new_ptr;
                } else {
                    pv_copy.data.ptr = std::ptr::null();
                    pv_copy.len = 0;
                }
            }
            prims.push(pv_copy);
        }
        let values_ptr = prims.as_ptr();
        let len = prims.len() as u64;
        std::mem::forget(prims);
        PrimTuple {
            values: values_ptr,
            len,
        }
    }
}

impl Debug for PrimTuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\nPrimTuple with {} elements, starting at address:{:p}\n",
            self.len, self.values
        )?;
        if !self.values.is_null() && self.len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(self.values, self.len as usize) };
            for (i, val) in slice.iter().enumerate() {
                write!(f, "\n  [{}]: {:?}", i, val)?;
            }
        } else {
            write!(f, " (empty or null)")?;
        }
        Ok(())
    }
}

impl From<PrimTuple> for Vec<DataValue> {
    fn from(pt: PrimTuple) -> Self {
        if pt.values.is_null() || pt.len == 0 {
            return Vec::new();
        }
        let slice = unsafe { std::slice::from_raw_parts(pt.values, pt.len as usize) };
        slice.iter().map(|pv| DataValue::from(*pv)).collect()
    }
}

/// should only use with caution, as if DataValue has string, the memory may be dropped and causing
/// dangling pointer issues.
impl From<&Vec<DataValue>> for PrimTuple {
    fn from(vec: &Vec<DataValue>) -> Self {
        let prims: Vec<PrimValue> = vec.iter().map(PrimValue::from).collect();
        let ptr = prims.as_ptr();
        let len = prims.len() as u64;
        // Leak the Vec to keep the memory alive (caller must reclaim)
        // below is the way for the caller to reclaim the memory ;
        // the caller should NOT use libc::free to reclaim the buffer created by
        // vec_datavalue_to_primtuple. The memory was allocated by Rust's allocator (via
        // Vec<PrimValue>), not by C's malloc, so the layout and bookkeeping are different.
        // unsafe {
        // Reconstruct the Vec<PrimValue> so Rust can drop and free it
        // let _ = Vec::from_raw_parts(tuple.values as *mut PrimValue, tuple.len as usize, tuple.len
        // as usize); Dropping this Vec will free the memory
        // }
        std::mem::forget(prims);
        PrimTuple { values: ptr, len }
    }
}

impl From<PrimValue> for DataValue {
    fn from(pv: PrimValue) -> Self {
        if pv.is_null == 1 {
            return DataValue::Null;
        }
        match pv.ty_id {
            0 => DataValue::Null,
            1 => DataValue::Boolean(Some(unsafe { pv.data.int8 != 0 })),
            2 => DataValue::Float32(Some(unsafe { pv.data.float32 })),
            3 => DataValue::Float64(Some(unsafe { pv.data.float64 })),
            4 => DataValue::Int8(Some(unsafe { pv.data.int8 })),
            5 => DataValue::Int16(Some(unsafe { pv.data.int16 })),
            6 => DataValue::Int32(Some(unsafe { pv.data.int32 })),
            7 => DataValue::Int64(Some(unsafe { pv.data.int64 })),
            8 => DataValue::UInt8(Some(unsafe { pv.data.uint8 })),
            9 => DataValue::UInt16(Some(unsafe { pv.data.uint16 })),
            10 => DataValue::UInt32(Some(unsafe { pv.data.uint32 })),
            11 => DataValue::UInt64(Some(unsafe { pv.data.uint64 })),
            12 => {
                // Utf8: copy the data from ptr
                let ptr = unsafe { pv.data.ptr };
                if ptr.is_null() || pv.len == 0 {
                    DataValue::Utf8(None)
                } else {
                    let slice = unsafe { std::slice::from_raw_parts(ptr, pv.len as usize) };
                    let s = String::from_utf8_lossy(slice).to_string();
                    DataValue::Utf8(Some(s))
                }
            }
            13 => {
                // Binary: copy the data from ptr
                let ptr = unsafe { pv.data.ptr };
                if ptr.is_null() || pv.len == 0 {
                    DataValue::Binary(None)
                } else {
                    let slice = unsafe { std::slice::from_raw_parts(ptr, pv.len as usize) };
                    let v = slice.to_vec();
                    DataValue::Binary(Some(v))
                }
            }
            14 => DataValue::Date32(Some(unsafe { pv.data.int32 })),
            15 => DataValue::Date64(Some(unsafe { pv.data.int64 })),
            16 => DataValue::Time32Second(Some(unsafe { pv.data.int32 })),
            17 => DataValue::Time32Millisecond(Some(unsafe { pv.data.int32 })),
            18 => DataValue::Time64Microsecond(Some(unsafe { pv.data.int64 })),
            19 => DataValue::Time64Nanosecond(Some(unsafe { pv.data.int64 })),
            _ => DataValue::Null,
        }
    }
}

/// also use with caution, as if DataValue has string, the memory may be dropped
/// memory is not copied for PrimValue
impl From<&DataValue> for PrimValue {
    fn from(dv: &DataValue) -> Self {
        match dv {
            DataValue::Null => PrimValue {
                ty_id: 0,
                is_null: 1,
                _padding: [0; 6],
                data: PrimValueUnion { int64: 0 },
                len: 0,
            },
            DataValue::Boolean(opt) => PrimValue {
                ty_id: 1,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int8: opt.unwrap_or(false) as i8,
                },
                len: 0,
            },
            DataValue::Float32(opt) => PrimValue {
                ty_id: 2,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    float32: opt.unwrap_or(0.0),
                },
                len: 0,
            },
            DataValue::Float64(opt) => PrimValue {
                ty_id: 3,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    float64: opt.unwrap_or(0.0),
                },
                len: 0,
            },
            DataValue::Int8(opt) => PrimValue {
                ty_id: 4,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int8: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Int16(opt) => PrimValue {
                ty_id: 5,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int16: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Int32(opt) => PrimValue {
                ty_id: 6,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int32: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Int64(opt) => PrimValue {
                ty_id: 7,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int64: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::UInt8(opt) => PrimValue {
                ty_id: 8,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    uint8: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::UInt16(opt) => PrimValue {
                ty_id: 9,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    uint16: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::UInt32(opt) => PrimValue {
                ty_id: 10,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    uint32: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::UInt64(opt) => PrimValue {
                ty_id: 11,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    uint64: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Utf8(opt) => {
                if let Some(s) = opt {
                    let bytes = s.as_bytes();
                    PrimValue {
                        ty_id: 12,
                        is_null: 0,
                        _padding: [0; 6],
                        data: PrimValueUnion {
                            ptr: bytes.as_ptr(),
                        },
                        len: bytes.len() as u64,
                    }
                } else {
                    PrimValue {
                        ty_id: 12,
                        is_null: 1,
                        _padding: [0; 6],
                        data: PrimValueUnion {
                            ptr: std::ptr::null(),
                        },
                        len: 0,
                    }
                }
            }
            DataValue::Binary(opt) => {
                if let Some(v) = opt {
                    PrimValue {
                        ty_id: 13,
                        is_null: 0,
                        _padding: [0; 6],
                        data: PrimValueUnion { ptr: v.as_ptr() },
                        len: v.len() as u64,
                    }
                } else {
                    PrimValue {
                        ty_id: 13,
                        is_null: 1,
                        _padding: [0; 6],
                        data: PrimValueUnion {
                            ptr: std::ptr::null(),
                        },
                        len: 0,
                    }
                }
            }
            DataValue::Date32(opt) => PrimValue {
                ty_id: 14,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int32: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Date64(opt) => PrimValue {
                ty_id: 15,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int64: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Time32Second(opt) => PrimValue {
                ty_id: 16,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int32: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Time32Millisecond(opt) => PrimValue {
                ty_id: 17,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int32: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Time64Microsecond(opt) => PrimValue {
                ty_id: 18,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int64: opt.unwrap_or(0),
                },
                len: 0,
            },
            DataValue::Time64Nanosecond(opt) => PrimValue {
                ty_id: 19,
                is_null: if opt.is_none() { 1 } else { 0 },
                _padding: [0; 6],
                data: PrimValueUnion {
                    int64: opt.unwrap_or(0),
                },
                len: 0,
            },
        }
    }
}

/// Codegen type descriptor using a primitive type tag for FFI/codegen compatibility
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct CGType {
    pub ty_id: u8, // type tag/discriminant
}
impl From<u8> for CGType {
    fn from(ty_id: u8) -> Self {
        CGType { ty_id }
    }
}

impl CGType {
    /// Convert from DataType to ty_id (u8)
    pub fn from_datatype(dt: &DataType) -> Self {
        // You may want to keep this mapping in sync with PrimValue ty field
        let ty_id = match dt {
            DataType::Null => 0,
            DataType::Boolean => 1,
            DataType::Float32 => 2,
            DataType::Float64 => 3,
            DataType::Int8 => 4,
            DataType::Int16 => 5,
            DataType::Int32 => 6,
            DataType::Int64 => 7,
            DataType::UInt8 => 8,
            DataType::UInt16 => 9,
            DataType::UInt32 => 10,
            DataType::UInt64 => 11,
            DataType::Utf8 => 12,
            DataType::Binary => 13,
            DataType::Date32 => 14,
            DataType::Date64 => 15,
            DataType::Time32Second => 16,
            DataType::Time32Millisecond => 17,
            DataType::Time64Microsecond => 18,
            DataType::Time64Nanosecond => 19,
        };
        CGType { ty_id }
    }

    /// Convert from ty_id (u8) to DataType
    pub fn to_datatype(&self) -> Option<DataType> {
        match self.ty_id {
            0 => Some(DataType::Null),
            1 => Some(DataType::Boolean),
            2 => Some(DataType::Float32),
            3 => Some(DataType::Float64),
            4 => Some(DataType::Int8),
            5 => Some(DataType::Int16),
            6 => Some(DataType::Int32),
            7 => Some(DataType::Int64),
            8 => Some(DataType::UInt8),
            9 => Some(DataType::UInt16),
            10 => Some(DataType::UInt32),
            11 => Some(DataType::UInt64),
            12 => Some(DataType::Utf8),
            13 => Some(DataType::Binary),
            14 => Some(DataType::Date32),
            15 => Some(DataType::Date64),
            16 => Some(DataType::Time32Second),
            17 => Some(DataType::Time32Millisecond),
            18 => Some(DataType::Time64Microsecond),
            19 => Some(DataType::Time64Nanosecond),
            _ => None,
        }
    }
}

impl From<&DataType> for CGType {
    fn from(dt: &DataType) -> Self {
        CGType::from_datatype(&dt)
    }
}

pub struct CGValue<'ctx> {
    pub ty: CGType,
    pub value: BasicValueEnum<'ctx>,
    pub length: BasicValueEnum<'ctx>,
    pub is_null: BasicValueEnum<'ctx>,
    pub elem_addr: Option<PointerValue<'ctx>>, // the element address, i.e, start of the PrimValue
}

impl<'ctx> CGValue<'ctx> {
    pub fn new(
        ty: CGType,
        value: BasicValueEnum<'ctx>,
        length: BasicValueEnum<'ctx>,
        is_null: BasicValueEnum<'ctx>,
    ) -> Self {
        CGValue {
            ty,
            value,
            length,
            is_null,
            elem_addr: None,
        }
    }

    pub fn debug_print(&self, codegen: &CodeGen<'ctx>) {
        codegen.emit_printf_call("CGValue information:\n", &[]);
        codegen.emit_printf_call(
            "  Type ID: %d\n",
            &[codegen
                .types
                .i32type
                .const_int(self.ty.ty_id as u64, false)
                .into()],
        );
        // for value, for simplicity, we just support print int, float, and string
        if self.ty.ty_id == 0 {
            codegen.emit_printf_call("  Value: NULL\n", &[]);
            return;
        }
        codegen.emit_printf_call(
            "  IsNull (if true, omit <value>): %d\n",
            &[self.is_null.into_int_value().into()],
        );
        match self.ty.ty_id {
            0 => codegen.emit_printf_call("  Value: NULL\n", &[]),
            1 => codegen.emit_printf_call(
                "  Value: Boolean, %d\n",
                &[self.value.into_int_value().into()],
            ),
            2 => {
                // IMPORTANT knowledge:
                //
                // Due to the C language standard and how variadic functions like printf work:
                //
                // In C, when you call a variadic function (like printf), all float arguments are
                // automatically promoted to double due to "default argument promotions."
                // So, when you write printf("%f", my_float);, the float is promoted to double
                // before being passed to printf. The %f format specifier in printf
                // always expects a double argument, never a float. In LLVM IR:
                //
                // If you pass an f32 (float) to printf("%f"), the bits are interpreted as a double,
                // which produces garbage output. You must explicitly convert
                // (fpext) the float to a double (f64) before passing it to printf.
                let double_val = codegen
                    .builder
                    .build_float_ext(
                        self.value.into_float_value(),
                        codegen.types.f64type,
                        "float32_to_double",
                    )
                    .unwrap();
                codegen.emit_printf_call("  Value: Float32, %f\n", &[double_val.into()]);
            }
            3 => codegen.emit_printf_call(
                "  Value: Float64, %f\n",
                &[self.value.into_float_value().into()],
            ),
            4..=11 => codegen.emit_printf_call(
                "  Value: Integer, %d\n",
                &[self.value.into_int_value().into()],
            ),
            12 => {
                // For Utf8, we call the special function to print the string
                let ptr = self.value.into_pointer_value();
                codegen.emit_print_string(
                    ptr,
                    self.length.into_int_value(),
                    Some("  Value: Utf8, "),
                );
            }
            _ => codegen.emit_printf_call("  Value: Unsupported display type\n", &[]),
        }
    }
}

/// Here a Row Batch is just a logical view (does not own the storage)
/// of row data in the llvm IR world; we cannot directly access the data
/// but need to use the accessor object;
pub struct RowBatch<'ctx> {
    pub tid_start: BasicValueEnum<'ctx>, // start row id of the row data, relative to store layer
    pub tid_end: BasicValueEnum<'ctx>,   // end row id of the row data, relative to store layer
    pub num_rows: BasicValueEnum<'ctx>,  // number of rows in the batch
    //pub context: CompilationContextRef<'ctx>, // compilation context
    pub attribute_accessors: HashMap<AttributeInfo, Box<dyn Accessor<'ctx>>>,
}

impl<'ctx> RowBatch<'ctx> {
    /// Iterate over all valid rows in this batch, calling the callback for each row.
    /// The callback receives a Row<'ctx> representing the current row.
    pub fn iterate<F>(self: &Self, codegen: &CodeGen<'ctx>, mut cb: F)
    where
        F: FnMut(Row<'_, 'ctx>),
    {
        let zero = codegen.types.i64type.const_zero();
        let num_rows = self.num_rows;
        let loop_vars = [LoopVariable {
            name: Some("row_idx".to_string()),
            value: zero.into(),
            ty: codegen.types.i64type.into(),
        }];
        let loop_builder = WhileLoopBuilder::new(codegen, &loop_vars);

        // Condition block: check row_idx < num_rows
        loop_builder.position_at_cond(codegen);
        let row_idx = loop_builder.get_loop_var(0);
        let cond = codegen
            .builder
            .build_int_compare(
                inkwell::IntPredicate::ULT,
                row_idx.into_int_value(),
                num_rows.into_int_value(),
                "row_iter_cond",
            )
            .unwrap();
        loop_builder.build_conditional_branch(codegen, cond);

        // Body block: execute callback for valid row
        loop_builder.position_at_body(codegen);
        let row = Row {
            batch: self,
            batch_position: row_idx,
        };
        #[cfg(debug_assertions)]
        codegen.emit_printf_call(
            "RowBatch::iterate: row_idx: %d, num_rows: %d\n",
            &[row_idx.into(), num_rows.into()],
        );
        // invoke the callback with the current row
        cb(row);
        // next row_idx = row_idx + 1
        let next_idx = codegen
            .builder
            .build_int_add(
                row_idx.into_int_value(),
                codegen.types.i64type.const_int(1, false),
                "next_row_idx",
            )
            .unwrap()
            .into();
        loop_builder.loop_continue(codegen, &[next_idx]);

        // After loop
        loop_builder.position_at_after(codegen);
    }
}

pub struct Row<'a, 'ctx> {
    pub batch: &'a RowBatch<'ctx>, // The row batch this row belongs to
    pub batch_position: BasicValueEnum<'ctx>, /* The position of this row relative to the batch
                                    * start */
}

impl<'a, 'ctx> Row<'a, 'ctx> {
    pub fn get_owner_batch(&self) -> &'a RowBatch<'ctx> {
        self.batch
    }
    pub fn get_owner_batch_tid_start(&self) -> BasicValueEnum<'ctx> {
        self.batch.tid_start
    }

    pub fn derive_value_from_attribute(
        &self,
        codegen: &CodeGen<'ctx>,
        attribute: &AttributeInfo,
    ) -> CGValue<'ctx> {
        if let Some(accessor) = self.batch.attribute_accessors.get(attribute) {
            accessor.access(codegen, self)
        } else {
            panic!("Attribute accessor not found for {:?}", attribute);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::common::types::{DataType, DataValue};

    /// Helper for test: construct a PrimTuple from primitive values and string literals.
    /// Usage: make_primtuple(&[Int64(1), Float32(3.14), Utf8Lit("hello"), Boolean(true)])
    pub enum TestValue<'a> {
        Int32(i32),
        Int64(i64),
        Float32(f32),
        Float64(f64),
        Utf8Lit(&'a str),
        Boolean(bool),
        // Add more as needed
    }

    pub fn make_primtuple<'a>(vals: &[TestValue<'a>]) -> PrimTuple {
        use TestValue::*;
        let mut prims = Vec::with_capacity(vals.len());
        for v in vals {
            let p = match v {
                Int32(i) => PrimValue {
                    ty_id: 6,
                    is_null: 0,
                    _padding: [0; 6],
                    data: PrimValueUnion { int32: *i },
                    len: 0,
                },
                Int64(i) => PrimValue {
                    ty_id: 7,
                    is_null: 0,
                    _padding: [0; 6],
                    data: PrimValueUnion { int64: *i },
                    len: 0,
                },
                Float32(f) => PrimValue {
                    ty_id: 2,
                    is_null: 0,
                    _padding: [0; 6],
                    data: PrimValueUnion { float32: *f },
                    len: 0,
                },
                Float64(f) => PrimValue {
                    ty_id: 3,
                    is_null: 0,
                    _padding: [0; 6],
                    data: PrimValueUnion { float64: *f },
                    len: 0,
                },
                Utf8Lit(s) => {
                    // let cstr = CString::new(*s).expect("CString::new failed");
                    // let ptr = cstr.as_ptr();
                    // You must keep cstr alive! For tests, you can leak it:
                    // let _ = Box::leak(Box::new(cstr));
                    // We do not have to use a CString (endsd with \0)
                    PrimValue {
                        ty_id: 12,
                        is_null: 0,
                        _padding: [0; 6],
                        // data: PrimValueUnion { ptr: ptr as *const u8 },
                        data: PrimValueUnion {
                            ptr: s.as_ptr() as *const u8,
                        },
                        len: s.len() as u64, // or cstr.to_bytes().len() as u64
                    }
                }
                Boolean(b) => PrimValue {
                    ty_id: 1,
                    is_null: 0,
                    _padding: [0; 6],
                    data: PrimValueUnion {
                        int8: if *b { 1 } else { 0 },
                    },
                    len: 0,
                },
            };
            prims.push(p);
        }
        let ptr = prims.as_ptr();
        let len = prims.len() as u64;
        std::mem::forget(prims);
        PrimTuple { values: ptr, len }
    }
    #[test]
    fn test_cgtype_conversion() {
        let all_types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Float32,
            DataType::Float64,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32Second,
            DataType::Time32Millisecond,
            DataType::Time64Microsecond,
            DataType::Time64Nanosecond,
        ];
        for dt in all_types {
            let cg = CGType::from_datatype(&dt);
            let dt2 = cg.to_datatype();
            assert_eq!(Some(dt.clone()), dt2, "Failed for type: {:?}", dt);
        }
    }

    #[test]
    fn test_primtuple_to_vec_datavalue() {
        let tuple = make_primtuple(&[
            TestValue::Int32(42),
            TestValue::Float32(3.14),
            TestValue::Utf8Lit("hello"),
        ]);
        // Convert to Vec<DataValue>
        let vec: Vec<DataValue> = tuple.into();
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], DataValue::Int32(Some(42)));
        assert_eq!(vec[1], DataValue::Float32(Some(3.14)));
        assert_eq!(vec[2], DataValue::Utf8(Some("hello".to_string())));
    }
}
