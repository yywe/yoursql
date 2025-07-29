use std::collections::HashMap;

use inkwell::types::{BasicTypeEnum, StructType};
use inkwell::values::{BasicValueEnum, PointerValue};

use crate::compiler::CodeGen;

/// An identifier for state slots
pub type StateId = u32;

/// Information about a single state slot
#[derive(Debug, Clone)]
pub struct StateInfo<'ctx> {
    pub name: String,
    pub ty: BasicTypeEnum<'ctx>,
    pub index: u32,
    pub val: Option<BasicValueEnum<'ctx>>, // For local state, if needed
}

/// QueryState manages all operator state for a query plan.
/// All global state is combined into a single struct type for LLVM codegen.
///
/// In the meanwhile, the query state is always the first argument of all the functions
/// which is obtained through: `codegen.get_state()`
///
/// Also, the 1st member will be the executor context, the 2nd will be the buffering consumer
///
/// Ref BufferingConsumer::Prepare in Peloton
pub struct QueryState<'ctx> {
    state_slots: Vec<StateInfo<'ctx>>,
    name_to_id: HashMap<String, StateId>,
    constructed_type: Option<StructType<'ctx>>,
}

impl<'ctx> QueryState<'ctx> {
    pub fn new() -> Self {
        QueryState {
            state_slots: Vec::new(),
            name_to_id: HashMap::new(),
            constructed_type: None,
        }
    }

    /// Register a new state slot with a name and LLVM type.
    pub fn register_state(&mut self, name: impl Into<String>, ty: BasicTypeEnum<'ctx>) -> StateId {
        let name = name.into();
        let id = self.state_slots.len() as StateId;
        self.state_slots.push(StateInfo {
            name: name.clone(),
            ty,
            index: id,
            val: None,
        });
        self.name_to_id.insert(name, id);
        id
    }

    /// Get the pointer to the state slot with the given id from the runtime state struct pointer.
    pub fn load_state_ptr(&self, codegen: &CodeGen<'ctx>, state_id: StateId) -> PointerValue<'ctx> {
        let struct_type = self.get_type().expect("QueryState type not finalized");
        let query_state = codegen.get_state();
        let state_slot_info = &self.state_slots[state_id as usize];
        let idx = state_slot_info.index as u32;
        let state_ptr = codegen.builder.build_struct_gep(
            struct_type,
            query_state.into_pointer_value(),
            idx,
            "state_ptr",
        );
        state_ptr.unwrap_or_else(|_| {
            panic!(
                "Failed to get state pointer for id {}: {:?}",
                state_id, state_slot_info
            )
        })
    }

    /// Get the value of the state slot with the given id from the runtime state struct pointer.
    pub fn load_state_value(
        &self,
        codegen: &CodeGen<'ctx>,
        state_id: StateId,
    ) -> BasicValueEnum<'ctx> {
        let state_ptr = self.load_state_ptr(codegen, state_id);
        let state_info = &self.state_slots[state_id as usize];
        codegen
            .builder
            .build_load(
                state_info.ty,
                state_ptr,
                &format!("load_state_{}", state_info.name),
            )
            .unwrap()
    }

    /// Finalize and return the LLVM struct type representing all state.
    pub fn finalize_type(&mut self, codegen: &CodeGen<'ctx>) -> StructType<'ctx> {
        let types: Vec<BasicTypeEnum<'ctx>> = self.state_slots.iter().map(|s| s.ty).collect();
        let struct_type = codegen.context.struct_type(&types, false);
        self.constructed_type = Some(struct_type);
        struct_type
    }

    /// Get the constructed LLVM struct type, if finalized.
    pub fn get_type(&self) -> Option<StructType<'ctx>> {
        self.constructed_type
    }

    /// Get the state id by name, if registered.
    pub fn get_state_id(&self, name: &str) -> Option<StateId> {
        self.name_to_id.get(name).copied()
    }
}
