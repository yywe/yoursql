use inkwell::values::{BasicMetadataValueEnum, BasicValue, IntValue, PointerValue};
use inkwell::IntPredicate::ULT;

use crate::compiler::lang::{LoopVariable, WhileLoopBuilder};
use crate::compiler::CodeGen;
use log::info;

/// implemenetation of a set of debug utilities for the CodeGen

impl<'ctx> CodeGen<'ctx> {
    /// emit printf call.
    /// int printf(const char *format, ...)
    /// TODO: can we omit the life time annotations here? if omit, only error in outdated.rs
    /// maybe can omit when we delete outdated.rs
    #[allow(dead_code)]
    pub fn emit_printf_call<'a, 'b>(&'a self, format: &str, args: &[BasicMetadataValueEnum<'b>]) {
        let printf = self.module.get_function("printf").unwrap();
        let pointer_value = self.builder.build_global_string_ptr(format, "").unwrap();
        let mut build_args = vec![pointer_value.as_basic_value_enum().into()];
        build_args.extend_from_slice(args);
        self.builder.build_call(printf, &build_args, "").unwrap();
    }

    /// print a (rust) string that does not end with '\0', input the address and len
    /// If prefix is Some, print it before the string.
    #[allow(dead_code)]
    pub fn emit_print_string(
        &self,
        addr: PointerValue<'ctx>,
        len: IntValue<'ctx>,
        prefix: Option<&str>,
    ) {
        let i8_type = self.types.i8type;
        let i32_type = self.types.i32type;
        // Allocate buffer of len + 1 bytes (for null terminator)
        let total_len = self
            .builder
            .build_int_add(len, i32_type.const_int(1, false), "str_total_len")
            .unwrap();
        let buf_ptr = self
            .builder
            .build_array_alloca(i8_type, total_len, "str_buf")
            .unwrap();

        // Copy len bytes from addr to buf_ptr (memcpy)
        let memcpy = self.module.get_function("memcpy").unwrap();
        let len64 = self
            .builder
            .build_int_z_extend(len, self.types.i64type, "len64")
            .unwrap();
        self.builder
            .build_call(
                memcpy,
                &[
                    buf_ptr.as_basic_value_enum().into(),
                    addr.as_basic_value_enum().into(),
                    len64.into(),
                ],
                "call_memcpy",
            )
            .unwrap();

        // Write null terminator at buf_ptr[len] = 0
        let zero = i8_type.const_zero();
        let idx = len;
        let term_ptr = unsafe {
            self.builder
                .build_in_bounds_gep(i8_type, buf_ptr, &[idx], "str_term_ptr")
                .unwrap()
        };
        self.builder.build_store(term_ptr, zero).unwrap();

        let format = if let Some(prefix_str) = prefix {
            format!("{}%s\n", prefix_str)
        } else {
            "%s\n".to_string()
        };
        self.emit_printf_call(&format, &[buf_ptr.as_basic_value_enum().into()]);
    }

    /// print `len` bytes at `addr` as hex using printf in a loop.
    /// addr: pointer to start of memory (i8*)
    /// len: number of bytes to print (i32)
    #[allow(dead_code)]
    pub fn emit_print_bytes(&self, addr: PointerValue<'ctx>, len: IntValue<'ctx>, name: &str) {
        let i32_type = self.types.i32type;
        //let i8_type = self.types.i8type;
        // Loop variable: idx = 0
        let loop_vars = [LoopVariable {
            name: Some("idx".to_string()),
            value: i32_type.const_zero().into(),
            ty: i32_type.into(),
        }];
        let loop_builder = WhileLoopBuilder::new(self, &loop_vars);

        // Condition: idx < len
        loop_builder.position_at_cond(self);
        let idx = loop_builder.get_loop_var(0).into_int_value();
        let cond = self
            .builder
            .build_int_compare(ULT, idx, len, &format!("{name}_cond"))
            .unwrap();

        loop_builder.build_conditional_branch(self, cond);

        // Body: print byte at addr+idx
        loop_builder.position_at_body(self);
        // Compute ptr = addr + idx
        let offset = idx;
        let byte_ptr = unsafe {
            self.builder
                .build_in_bounds_gep(i32_type, addr, &[offset], &format!("{name}_byte_ptr"))
        }
        .unwrap();
        // Load the byte
        let byte_val = self
            .builder
            .build_load(i32_type, byte_ptr, &format!("{name}_byte"))
            .unwrap();
        // Print the byte as hex
        self.emit_printf_call("%02x ", &[byte_val.into()]);

        // Next idx = idx + 1
        let next_idx = self
            .builder
            .build_int_add(
                idx,
                i32_type.const_int(1, false),
                &format!("{name}_next_idx"),
            )
            .unwrap()
            .into();
        loop_builder.loop_continue(self, &[next_idx]);

        // After loop: print newline
        loop_builder.position_at_after(self);
        self.emit_printf_call("\n", &[]);
    }

    /// show the LLVM IR code.
    #[allow(dead_code)]
    pub fn show_llvm_ir(&self) {
        info!("---------------The LLVM IR---------------------");
        self.module.print_to_stderr();
        info!("---------------End of LLVM IR------------------")
    }
}
