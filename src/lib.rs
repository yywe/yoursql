pub mod catalog;
pub mod common;
pub mod expr;
pub mod logical_planner;
pub mod parser;
pub mod physical_expr;
pub mod physical_planner;
pub mod session;
pub mod storage;
pub mod compiler;
pub mod test;

/// hook so that logger is automatically initialized
#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}