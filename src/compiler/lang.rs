use std::cell::{Ref, RefMut};
use std::collections::HashMap;

use inkwell::basic_block::BasicBlock;
use inkwell::types::{BasicMetadataTypeEnum, BasicType, BasicTypeEnum, FunctionType, VoidType};
use inkwell::values::{BasicValueEnum, FunctionValue, IntValue, PhiValue};

use crate::compiler::CodeGen;
/// Helper for ergonomic PHI node construction in LLVM IR
pub struct BuildPHI<'ctx> {
    pub phi: PhiValue<'ctx>,
}

impl<'ctx> BuildPHI<'ctx> {
    /// Create a new PHI node of the given type at the current insertion point
    pub fn new(codegen: &CodeGen<'ctx>, ty: BasicTypeEnum<'ctx>, name: &str) -> Self {
        let phi = codegen.builder.build_phi(ty, name).unwrap();
        BuildPHI { phi }
    }

    /// Add an incoming value from a block
    pub fn add_incoming(&self, value: BasicValueEnum<'ctx>, block: BasicBlock<'ctx>) {
        self.phi.add_incoming(&[(&value, block)]);
    }

    /// Get the resulting PHI value
    pub fn as_value(&self) -> BasicValueEnum<'ctx> {
        self.phi.as_basic_value()
    }
}

pub struct IfBuilder<'ctx> {
    pub cond: IntValue<'ctx>,
    pub then_block: BasicBlock<'ctx>,
    pub else_block: BasicBlock<'ctx>,
    pub merge_block: BasicBlock<'ctx>,
    pub parent_fn: FunctionValue<'ctx>,
}

impl<'ctx> IfBuilder<'ctx> {
    /// Create a PHI node at the merge block for merging values from both branches
    pub fn build_phi(
        &self,
        codegen: &CodeGen<'ctx>,
        ty: BasicTypeEnum<'ctx>,
        name: &str,
    ) -> BuildPHI<'ctx> {
        codegen.builder.position_at_end(self.merge_block);
        BuildPHI::new(codegen, ty, name)
    }

    pub fn new(codegen: &CodeGen<'ctx>, cond: IntValue<'ctx>, name: &str) -> Self {
        let builder = &codegen.builder;
        let parent_fn = builder.get_insert_block().unwrap().get_parent().unwrap();
        let then_block = codegen
            .context
            .append_basic_block(parent_fn, &format!("{}_then", name));
        let else_block = codegen
            .context
            .append_basic_block(parent_fn, &format!("{}_else", name));
        let merge_block = codegen
            .context
            .append_basic_block(parent_fn, &format!("{}_merge", name));
        builder
            .build_conditional_branch(cond, then_block, else_block)
            .unwrap();
        Self {
            cond,
            then_block,
            else_block,
            merge_block,
            parent_fn,
        }
    }

    pub fn then<F>(&self, codegen: &CodeGen<'ctx>, f: F)
    where
        F: FnOnce(),
    {
        codegen.builder.position_at_end(self.then_block);
        f();
        if !codegen
            .builder
            .get_insert_block()
            .unwrap()
            .get_terminator()
            .is_some()
        {
            codegen
                .builder
                .build_unconditional_branch(self.merge_block)
                .unwrap();
        }
    }

    pub fn otherwise<F>(&self, codegen: &CodeGen<'ctx>, f: F)
    where
        F: FnOnce(),
    {
        codegen.builder.position_at_end(self.else_block);
        f();
        if !codegen
            .builder
            .get_insert_block()
            .unwrap()
            .get_terminator()
            .is_some()
        {
            codegen
                .builder
                .build_unconditional_branch(self.merge_block)
                .unwrap();
        }
    }

    pub fn after(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.merge_block);
    }
}

/// Represents a loop variable (name is optional, for debugging)
pub struct LoopVariable<'ctx> {
    pub name: Option<String>,
    pub value: BasicValueEnum<'ctx>,
    pub ty: BasicTypeEnum<'ctx>,
}

/// Builder for ergonomic LLVM IR loop construction
/// Note this will a be do while loop style;
pub struct DoWhileLoopBuilder<'ctx> {
    pub parent_fn: FunctionValue<'ctx>,
    pub pre_loop_bb: BasicBlock<'ctx>,
    pub loop_bb: BasicBlock<'ctx>,
    pub after_bb: BasicBlock<'ctx>,
    pub break_bbs: Vec<BasicBlock<'ctx>>,
    pub phi_nodes: Vec<PhiValue<'ctx>>,
}

/// let loop_vars = vec![
/// LoopVariable { name: Some("i".to_string()), value: start_i, ty: i32_type.into() },
/// LoopVariable { name: Some("sum".to_string()), value: start_sum, ty: i32_type.into() },
/// ];
/// let mut loop_builder = LoopBuilder::new(codegen, &loop_vars);
///
/// loop {
/// let i = loop_builder.get_loop_var(0);
/// let sum = loop_builder.get_loop_var(1);
///
/// ... do work, maybe break if i >= N ...
///
/// note, unlike the IfBuilder, we do not need a closure to customize the loop body
/// we can simply write the loop body directly here
/// e.g., increment i and sum:
/// Compute next values
/// let next_i = ...;
/// let next_sum = ...;
///
/// At end of loop body:
/// loop_builder.loop_end(cond, &[next_i, next_sum]);
/// break;
/// }
///
///
///
/// After loop:
/// let [final_i, final_sum] = loop_builder.collect_final_loop_vars();

impl<'ctx> DoWhileLoopBuilder<'ctx> {
    /// Create a new loop with initial loop variables
    pub fn new(codegen: &CodeGen<'ctx>, loop_vars: &[LoopVariable<'ctx>]) -> Self {
        let builder = &codegen.builder;
        let parent_fn = builder.get_insert_block().unwrap().get_parent().unwrap();

        let pre_loop_bb = builder.get_insert_block().unwrap();
        let loop_bb = codegen.context.append_basic_block(parent_fn, "loop");
        let after_bb = codegen.context.append_basic_block(parent_fn, "after_loop");

        // Jump to loop block
        builder.build_unconditional_branch(loop_bb).unwrap();
        builder.position_at_end(loop_bb);

        // Create PHI nodes for each loop variable
        let mut phi_nodes = Vec::with_capacity(loop_vars.len());
        for var in loop_vars {
            let phi = builder
                .build_phi(var.ty, var.name.as_deref().unwrap_or("loop_var"))
                .unwrap();
            phi.add_incoming(&[(&var.value, pre_loop_bb)]);
            phi_nodes.push(phi);
        }

        Self {
            parent_fn,
            pre_loop_bb,
            loop_bb,
            after_bb,
            break_bbs: Vec::new(),
            phi_nodes,
        }
    }

    /// Get the current value of a loop variable by index
    pub fn get_loop_var(&self, idx: usize) -> BasicValueEnum<'ctx> {
        self.phi_nodes[idx].as_basic_value()
    }

    /// Mark the current block as a break (will jump to after_bb at LoopEnd)
    pub fn break_here(&mut self, codegen: &CodeGen<'ctx>) {
        let builder = &codegen.builder;
        let cur_bb = builder.get_insert_block().unwrap();
        builder.build_unconditional_branch(self.after_bb).unwrap();
        self.break_bbs.push(cur_bb);
    }

    /// Mark the end of the loop body, provide the end condition and next values for loop vars
    pub fn loop_end(
        &mut self,
        codegen: &CodeGen<'ctx>,
        end_condition: IntValue<'ctx>,
        next_vars: &[BasicValueEnum<'ctx>],
    ) {
        let builder = &codegen.builder;
        let cur_bb = builder.get_insert_block().unwrap();

        // Add incoming values to PHI nodes for the next iteration
        for (phi, next) in self.phi_nodes.iter().zip(next_vars.iter()) {
            phi.add_incoming(&[(next, cur_bb)]);
        }

        // Conditional branch: continue loop or exit
        builder
            .build_conditional_branch(end_condition, self.loop_bb, self.after_bb)
            .unwrap();

        // Patch all break blocks to jump to after_bb
        for bb in &self.break_bbs {
            if bb.get_terminator().is_none() {
                builder.position_at_end(*bb);
                builder.build_unconditional_branch(self.after_bb).unwrap();
            }
        }

        // Move insertion point to after_bb for code after the loop
        builder.position_at_end(self.after_bb);
    }

    /// Collect the final values of all loop variables after the loop
    pub fn collect_final_loop_vars(&self) -> Vec<BasicValueEnum<'ctx>> {
        self.phi_nodes
            .iter()
            .map(|phi| phi.as_basic_value())
            .collect()
    }
}

/// entry:
/// br cond_block
///
/// cond_block:
/// ; evaluate condition
/// br i1 %cond, body_block, after_block
///
/// body_block:
/// ; loop body
/// ; update loop vars
/// br cond_block
///
/// after_block:
/// ; code after loop

pub struct WhileLoopBuilder<'ctx> {
    pub parent_fn: FunctionValue<'ctx>,
    pub entry_bb: BasicBlock<'ctx>,
    pub cond_bb: BasicBlock<'ctx>,
    pub body_bb: BasicBlock<'ctx>,
    pub after_bb: BasicBlock<'ctx>,
    pub phi_nodes: Vec<PhiValue<'ctx>>,
}

impl<'ctx> WhileLoopBuilder<'ctx> {
    pub fn new(codegen: &CodeGen<'ctx>, loop_vars: &[LoopVariable<'ctx>]) -> Self {
        let builder = &codegen.builder;
        let parent_fn = builder.get_insert_block().unwrap().get_parent().unwrap();

        let entry_bb = builder.get_insert_block().unwrap();
        let cond_bb = codegen.context.append_basic_block(parent_fn, "loop_cond");
        let body_bb = codegen.context.append_basic_block(parent_fn, "loop_body");
        let after_bb = codegen.context.append_basic_block(parent_fn, "after_loop");

        // Jump to cond block
        builder.build_unconditional_branch(cond_bb).unwrap();
        builder.position_at_end(cond_bb);

        // Create PHI nodes for each loop variable (in cond block)
        let mut phi_nodes = Vec::with_capacity(loop_vars.len());
        for var in loop_vars {
            let phi = builder
                .build_phi(var.ty, var.name.as_deref().unwrap_or("loop_var"))
                .unwrap();
            phi.add_incoming(&[(&var.value, entry_bb)]);
            phi_nodes.push(phi);
        }

        Self {
            parent_fn,
            entry_bb,
            cond_bb,
            body_bb,
            after_bb,
            phi_nodes,
        }
    }

    /// Get the current value of a loop variable by index (in cond or body block)
    pub fn get_loop_var(&self, idx: usize) -> BasicValueEnum<'ctx> {
        self.phi_nodes[idx].as_basic_value()
    }

    /// Build the loop condition and branch to body or after
    pub fn build_conditional_branch(&self, codegen: &CodeGen<'ctx>, cond: IntValue<'ctx>) {
        let builder = &codegen.builder;
        builder
            .build_conditional_branch(cond, self.body_bb, self.after_bb)
            .unwrap();
    }

    /// At the end of the body, update loop vars and branch back to cond
    pub fn loop_continue(&self, codegen: &CodeGen<'ctx>, next_vars: &[BasicValueEnum<'ctx>]) {
        let builder = &codegen.builder;
        let cur_bb = builder.get_insert_block().unwrap();
        for (phi, next) in self.phi_nodes.iter().zip(next_vars.iter()) {
            phi.add_incoming(&[(next, cur_bb)]);
        }
        builder.build_unconditional_branch(self.cond_bb).unwrap();
    }

    /// Position builder at cond block
    pub fn position_at_cond(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.cond_bb);
    }

    /// Position builder at body block
    pub fn position_at_body(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.body_bb);
    }

    /// Position builder at after block
    pub fn position_at_after(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.after_bb);
    }

    /// Collect the final values of all loop variables after the loop
    pub fn collect_final_loop_vars(&self) -> Vec<BasicValueEnum<'ctx>> {
        self.phi_nodes
            .iter()
            .map(|phi| phi.as_basic_value())
            .collect()
    }
}

/// Function builder, beside the entry block, here we also created a universal return block
/// which will be used to handle all return statements in the function
/// Altough in simple functions where a single return is used, in complex funcitons
/// a universal return block is helpful regards resource clean up, choose return values,etc
/// It support return a sepcified value or a PHI node collecting all possible return values

/// nested functionbuilder is supported, ref test_nested_function_definition;

pub struct FunctionBuilder<'ctx> {
    pub function: FunctionValue<'ctx>,
    pub entry_bb: BasicBlock<'ctx>,
    pub return_bb: BasicBlock<'ctx>,
    pub finished: bool,
    pub arg_names: Vec<String>,
    pub arg_map: HashMap<String, BasicValueEnum<'ctx>>,
    pub return_values: Vec<(BasicValueEnum<'ctx>, BasicBlock<'ctx>)>, /* to support PHI in case
                                                                       * need to chose value */
    pub previous_insert_point: Option<BasicBlock<'ctx>>, // to support nested function builder
}

impl<'ctx> FunctionBuilder<'ctx> {
    pub fn new(
        codegen: &CodeGen<'ctx>,
        name: &str,
        ret_type: impl FnTypeExt<'ctx>,
        args: &[(String, BasicMetadataTypeEnum<'ctx>)],
    ) -> Self {
        let previous_insert_point = codegen.builder.get_insert_block();

        let arg_types: Vec<BasicMetadataTypeEnum<'ctx>> = args.iter().map(|(_, t)| *t).collect();
        let fn_type = ret_type.create_fn_type(
            &arg_types.iter().map(|t| (*t).into()).collect::<Vec<_>>(),
            false,
        );
        let function = codegen.module.add_function(name, fn_type, None);
        let entry_bb = codegen.context.append_basic_block(function, "entry");
        let return_bb = codegen.context.append_basic_block(function, "return");
        codegen.builder.position_at_end(entry_bb);

        let mut arg_map = HashMap::new();
        for (i, (arg_name, _)) in args.iter().enumerate() {
            let arg_val = function.get_nth_param(i as u32).unwrap();
            arg_map.insert(arg_name.clone(), arg_val);
        }
        let cur_func = Self {
            function,
            entry_bb,
            return_bb,
            finished: false,
            arg_names: args.iter().map(|(name, _)| name.clone()).collect(),
            arg_map,
            return_values: Vec::new(),
            previous_insert_point,
        };
        cur_func
    }

    pub fn new_and_push_to_stack<'a>(
        codegen: &'a CodeGen<'ctx>,
        name: &str,
        ret_type: impl FnTypeExt<'ctx>,
        args: &[(String, BasicMetadataTypeEnum<'ctx>)],
    ) {
        let func = Self::new(codegen, name, ret_type, args);
        codegen.function_builders.borrow_mut().push(func);
    }
    pub fn get_current_function_function(
        codegen: &'ctx CodeGen<'ctx>,
    ) -> Ref<'ctx, FunctionBuilder<'ctx>> {
        Ref::map(codegen.function_builders.borrow(), |vec| {
            vec.last().unwrap()
        })
    }

    pub fn get_current_function_builder_mut<'a>(
        codegen: &'a CodeGen<'ctx>,
    ) -> RefMut<'a, FunctionBuilder<'ctx>> {
        let fn_builders = codegen.function_builders.borrow_mut();
        let len = fn_builders.len();
        RefMut::map(fn_builders, |fb| {
            fb.get_mut(len - 1).expect("No function builder found")
        })
    }

    /// Get the LLVM function value
    pub fn get_function(&self) -> FunctionValue<'ctx> {
        self.function
    }

    /// Get the entry block
    pub fn get_entry_block(&self) -> BasicBlock<'ctx> {
        self.entry_bb
    }

    /// Get the return block
    pub fn get_return_block(&self) -> BasicBlock<'ctx> {
        self.return_bb
    }

    /// Get argument by name
    pub fn get_arg_by_name(&self, name: &str) -> Option<BasicValueEnum<'ctx>> {
        self.arg_map.get(name).cloned()
    }

    /// Get argument by position
    pub fn get_arg_by_index(&self, idx: usize) -> Option<BasicValueEnum<'ctx>> {
        self.function.get_nth_param(idx as u32)
    }

    /// Position builder at the entry block
    pub fn position_at_entry(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.entry_bb);
    }

    /// Position builder at the return block
    pub fn position_at_return(&self, codegen: &CodeGen<'ctx>) {
        codegen.builder.position_at_end(self.return_bb);
    }

    /// Record a return value and its originating block for PHI node merging in the return block
    pub fn add_return_value(&mut self, value: BasicValueEnum<'ctx>, block: BasicBlock<'ctx>) {
        self.return_values.push((value, block));
    }

    pub fn return_and_finish(
        &mut self,
        codegen: &CodeGen<'ctx>,
        ret_val: Option<BasicValueEnum<'ctx>>,
    ) {
        if self.finished {
            return;
        }
        let cur_bb = codegen.builder.get_insert_block().unwrap();
        // if not already at return block, and the current block does not has terminator yet, branch
        // to return, otherwise if already has terminator, just position to return block
        if cur_bb != self.return_bb {
            if cur_bb.get_terminator().is_none() {
                codegen
                    .builder
                    .build_unconditional_branch(self.return_bb)
                    .unwrap();
            }
            // Move builder to return block and finish
            self.position_at_return(codegen);
            // now recursively call itself
            self.return_and_finish(codegen, ret_val);
            return;
        }
        // now we are already at the return block
        // if specified return value
        if let Some(val) = ret_val {
            codegen.builder.build_return(Some(&val)).unwrap();
            // Restore previous insertion point
            if let Some(bb) = self.previous_insert_point {
                codegen.builder.position_at_end(bb);
            }
        } else {
            // if no supplied return value also check if we have any return values collected
            if !self.return_values.is_empty() {
                // Create a PHI node to collect all return values
                let phi = codegen
                    .builder
                    .build_phi(self.return_values[0].0.get_type(), "return_phi")
                    .unwrap();
                for (val, block) in &self.return_values {
                    phi.add_incoming(&[(val, *block)]);
                }
                codegen
                    .builder
                    .build_return(Some(&phi.as_basic_value()))
                    .unwrap();
            } else {
                codegen.builder.build_return(None).unwrap();
            }
            // Restore previous insertion point
            if let Some(bb) = self.previous_insert_point {
                codegen.builder.position_at_end(bb);
            }
        }
        self.finished = true;
    }
}

/// Helper trait to allow both BasicTypeEnum and VoidType as return types
pub trait FnTypeExt<'ctx> {
    fn create_fn_type(
        &self,
        param_types: &[BasicMetadataTypeEnum<'ctx>],
        is_var_args: bool,
    ) -> FunctionType<'ctx>;
}

impl<'ctx> FnTypeExt<'ctx> for BasicTypeEnum<'ctx> {
    fn create_fn_type(
        &self,
        param_types: &[BasicMetadataTypeEnum<'ctx>],
        is_var_args: bool,
    ) -> FunctionType<'ctx> {
        self.fn_type(param_types, is_var_args)
    }
}

impl<'ctx> FnTypeExt<'ctx> for VoidType<'ctx> {
    fn create_fn_type(
        &self,
        param_types: &[BasicMetadataTypeEnum<'ctx>],
        is_var_args: bool,
    ) -> FunctionType<'ctx> {
        self.fn_type(param_types, is_var_args)
    }
}

#[cfg(test)]
mod tests {
    use inkwell::context::Context;
    use inkwell::values::BasicValue;

    use super::{DoWhileLoopBuilder, IfBuilder, LoopVariable, WhileLoopBuilder, *};
    use crate::compiler::{CodeGen, Int32Int32FnType, VoidInt32FnType};
    #[test]
    fn test_function_builder() {
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test_phi_return");
        codegen.initialize();
        let fn_name = "test_function_builder";
        let mut fnbuilder = FunctionBuilder::new(
            &codegen,
            fn_name,
            codegen.types.i32type.as_basic_type_enum(),
            &[("cond".to_string(), codegen.types.i32type.into())],
        );
        let cond_val = fnbuilder.get_arg_by_name("cond").unwrap().into_int_value();
        let then_block = codegen
            .context
            .append_basic_block(fnbuilder.function, "then");
        let else_block = codegen
            .context
            .append_basic_block(fnbuilder.function, "else");
        // if (cond != 0) return 1; else return 2;
        codegen
            .builder
            .build_conditional_branch(
                codegen
                    .builder
                    .build_int_compare(
                        inkwell::IntPredicate::NE,
                        cond_val,
                        codegen.types.i32type.const_zero(),
                        "cond_ne_zero",
                    )
                    .unwrap(),
                then_block,
                else_block,
            )
            .unwrap();
        // Then block: return 1
        codegen.builder.position_at_end(then_block);
        let one = codegen.types.i32type.const_int(1, false).into();
        fnbuilder.add_return_value(one, then_block);
        codegen
            .builder
            .build_unconditional_branch(fnbuilder.return_bb)
            .unwrap();

        // Else block: return 2
        codegen.builder.position_at_end(else_block);
        let two = codegen.types.i32type.const_int(2, false).into();
        fnbuilder.add_return_value(two, else_block);
        codegen
            .builder
            .build_unconditional_branch(fnbuilder.return_bb)
            .unwrap();

        // Now finish the function
        fnbuilder.return_and_finish(&codegen, None);

        // Verify the module IR
        codegen.module.verify().unwrap();

        // codegen.show_llvm_ir();

        let test_func =
            unsafe { codegen.engine.get_function::<Int32Int32FnType>(fn_name) }.unwrap();
        // codegen.show_llvm_ir();
        // Test: cond != 0 returns 1, cond == 0 returns 2
        assert_eq!(unsafe { test_func.call(42) }, 1);
        assert_eq!(unsafe { test_func.call(0) }, 2);
    }

    #[test]
    fn test_dowhile_loop_builder() {
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test_phi_return");
        codegen.initialize();
        let fn_name = "test_dowhile_loop_builder";
        let mut fnbuilder = FunctionBuilder::new(
            &codegen,
            "test_dowhile_loop_builder",
            codegen.types.i32type.as_basic_type_enum(),
            &[],
        );
        // LoopBuilder: sum numbers from 1 to 100 (inclusive)
        // i = 1, sum = 0
        let i32_type = codegen.types.i32type;
        let loop_vars = vec![
            LoopVariable {
                name: Some("i".to_string()),
                value: i32_type.const_int(1, false).into(),
                ty: i32_type.into(),
            },
            LoopVariable {
                name: Some("sum".to_string()),
                value: i32_type.const_int(0, false).into(),
                ty: i32_type.into(),
            },
        ];
        let mut loop_builder = DoWhileLoopBuilder::new(&codegen, &loop_vars);

        // Loop body
        // while i <= 100
        let i_val = loop_builder.get_loop_var(0).into_int_value();
        let sum_val = loop_builder.get_loop_var(1).into_int_value();
        let cond = codegen
            .builder
            .build_int_compare(
                inkwell::IntPredicate::ULE,
                i_val,
                i32_type.const_int(100, false),
                "loop_cond",
            )
            .unwrap();

        // next_i = i + 1
        let next_i = codegen
            .builder
            .build_int_add(i_val, i32_type.const_int(1, false), "next_i")
            .unwrap()
            .into();
        // next_sum = sum + i
        let next_sum = codegen
            .builder
            .build_int_add(sum_val, i_val, "next_sum")
            .unwrap()
            .into();

        loop_builder.loop_end(&codegen, cond, &[next_i, next_sum]);

        // After loop: get the final sum
        let final_vars = loop_builder.collect_final_loop_vars();
        let final_sum = final_vars[1].into_int_value();

        // Optionally, use IfBuilder to check/assert the result (e.g., sum == 5050)
        let expected = i32_type.const_int(5050, false);
        let check = codegen
            .builder
            .build_int_compare(inkwell::IntPredicate::EQ, final_sum, expected, "check_sum")
            .unwrap();
        let if_builder = IfBuilder::new(&codegen, check, "assert_sum");
        if_builder.then(&codegen, || {
            codegen.emit_printf_call(" the result is   %d\n ", &[final_sum.into()]);
            // codegen.builder.build_return(Some(&final_sum.as_basic_value_enum())).unwrap();
            // fnbuilder.return_and_finish(&codegen, Some(final_sum.as_basic_value_enum()));
        });
        if_builder.otherwise(&codegen, || {
            // Failure: trap
            let trap_fn = codegen.module.get_function("llvm.trap").unwrap_or_else(|| {
                codegen.module.add_function(
                    "llvm.trap",
                    codegen.context.void_type().fn_type(&[], false),
                    None,
                )
            });
            codegen.builder.build_call(trap_fn, &[], "trap").unwrap();
            codegen.builder.build_unreachable().unwrap();
        });
        if_builder.after(&codegen);

        fnbuilder.return_and_finish(&codegen, Some(final_sum.as_basic_value_enum()));

        codegen.module.verify().unwrap();

        let test_func = unsafe { codegen.engine.get_function::<VoidInt32FnType>(fn_name) }.unwrap();

        // codegen.show_llvm_ir();
        // Call the JIT FUNCTION;
        unsafe {
            assert_eq!({ test_func.call() }, 5050);
        }
    }

    #[test]
    fn test_while_loop_builder() {
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test");
        codegen.initialize();
        let fn_name = "test_while_loop_builder";
        let mut fnbuilder = FunctionBuilder::new(
            &codegen,
            fn_name,
            codegen.types.i32type.as_basic_type_enum(),
            &[],
        );
        let i32_type = codegen.types.i32type;
        // sum numbers from 1 to 100 (inclusive)
        let loop_vars = vec![
            LoopVariable {
                name: Some("i".to_string()),
                value: i32_type.const_int(1, false).into(),
                ty: i32_type.into(),
            },
            LoopVariable {
                name: Some("sum".to_string()),
                value: i32_type.const_int(0, false).into(),
                ty: i32_type.into(),
            },
        ];
        let loop_builder = WhileLoopBuilder::new(&codegen, &loop_vars);

        // Position at cond block and build condition
        loop_builder.position_at_cond(&codegen);
        let i_val = loop_builder.get_loop_var(0).into_int_value();
        let sum_val = loop_builder.get_loop_var(1).into_int_value();
        let cond = codegen
            .builder
            .build_int_compare(
                inkwell::IntPredicate::ULE,
                i_val,
                i32_type.const_int(100, false),
                "loop_cond",
            )
            .unwrap();
        loop_builder.build_conditional_branch(&codegen, cond);

        // Position at body block and build body
        loop_builder.position_at_body(&codegen);
        // next_i = i + 1
        let next_i = codegen
            .builder
            .build_int_add(i_val, i32_type.const_int(1, false), "next_i")
            .unwrap()
            .into();
        // next_sum = sum + i
        let next_sum = codegen
            .builder
            .build_int_add(sum_val, i_val, "next_sum")
            .unwrap()
            .into();
        loop_builder.loop_continue(&codegen, &[next_i, next_sum]);

        // After loop: get the final sum
        loop_builder.position_at_after(&codegen);
        let final_vars = loop_builder.collect_final_loop_vars();
        let final_sum = final_vars[1].into_int_value();

        // Optionally, use IfBuilder to check/assert the result (e.g., sum == 5050)
        let expected = i32_type.const_int(5050, false);
        let check = codegen
            .builder
            .build_int_compare(inkwell::IntPredicate::EQ, final_sum, expected, "check_sum")
            .unwrap();
        let if_builder = IfBuilder::new(&codegen, check, "assert_sum");
        if_builder.then(&codegen, || {
            codegen.emit_printf_call(" the result is   %d\n ", &[final_sum.into()]);
        });
        if_builder.otherwise(&codegen, || {
            // Failure: trap
            let trap_fn = codegen.module.get_function("llvm.trap").unwrap_or_else(|| {
                codegen.module.add_function(
                    "llvm.trap",
                    codegen.context.void_type().fn_type(&[], false),
                    None,
                )
            });
            codegen.builder.build_call(trap_fn, &[], "trap").unwrap();
            codegen.builder.build_unreachable().unwrap();
        });
        if_builder.after(&codegen);

        fnbuilder.return_and_finish(&codegen, Some(final_sum.as_basic_value_enum()));

        codegen.module.verify().unwrap();
        let test_func = unsafe { codegen.engine.get_function::<VoidInt32FnType>(fn_name) }.unwrap();

        // codegen.show_llvm_ir();
        unsafe {
            assert_eq!({ test_func.call() }, 5050);
        }
    }

    #[test]
    fn test_nested_function_definition() {
        let context = Context::create();
        let codegen = CodeGen::new(&context, "test_nested_fn");
        codegen.initialize();

        // Outer function: takes i32, returns i32
        let outer_fn_name = "outer_fn";
        FunctionBuilder::new_and_push_to_stack(
            &codegen,
            outer_fn_name,
            codegen.types.i32type.as_basic_type_enum(),
            &[("x".to_string(), codegen.types.i32type.into())],
        );
        {
            // there is an imporant coding pattern with regard to RefCell;
            // the function_builders is a stack wrapped in RefCell, say when
            // we want to do some stuff temporarily, we need to pop the item
            // out, instead of using a borrow (either mutable) or not. because
            // if we use a borrow, then other code cannot borrow it again, often
            // with errors xxx already borrowed
            // instead, we do not use borrow but pop the item out and pass by value
            // if needed, after the temp stuff is done, just push back the item.

            // pop the current function builder
            let outer_builder = codegen.function_builders.borrow_mut().pop().unwrap();
            // do stuff
            let x_val = outer_builder.get_arg_by_name("x").unwrap().into_int_value();
            // push back to the stack
            codegen.function_builders.borrow_mut().push(outer_builder);

            // build the inner function, now the function builder nested
            let inner_fn_name = "inner_fn";
            FunctionBuilder::new_and_push_to_stack(
                &codegen,
                inner_fn_name,
                codegen.types.i32type.as_basic_type_enum(),
                &[("y".to_string(), codegen.types.i32type.into())],
            );
            {
                // take the inner builder off and do stuff
                let mut inner_builder = codegen.function_builders.borrow_mut().pop().unwrap();
                let y_val = inner_builder.get_arg_by_name("y").unwrap().into_int_value();
                let square = codegen
                    .builder
                    .build_int_mul(y_val, y_val, "square")
                    .unwrap();
                inner_builder.return_and_finish(&codegen, Some(square.into()));
                // here we do not need to put the builder back to the stack as already done
                // in the meanwhile, return_and_finish will restore the insert point to
                //  self.previous_insert_point
            }
            // continue with the outer builder (now the stack top)
            let mut outer_builder = codegen.function_builders.borrow_mut().pop().unwrap();
            let inner_fn = codegen.module.get_function(inner_fn_name).unwrap();
            let call = codegen
                .builder
                .build_call(inner_fn, &[x_val.into()], "call_inner")
                .unwrap();
            let result = call.try_as_basic_value().left().unwrap();
            outer_builder.return_and_finish(&codegen, Some(result));
        }
        codegen.module.verify().unwrap();
        // JIT test: outer_fn(x) should return x*x
        let outer_func = unsafe {
            codegen
                .engine
                .get_function::<Int32Int32FnType>(outer_fn_name)
        }
        .unwrap();
        assert_eq!(unsafe { outer_func.call(7) }, 49);
        assert_eq!(unsafe { outer_func.call(3) }, 9);
    }
}
