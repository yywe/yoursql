extern crate which;
use std::env;
use which::which;
fn main() {
    // PLEASE CHANGE THIS TO YOUR OWN LLVM VERSION AND PATH
    // NOTE: For macos with intel used: clang+llvm-15.0.7-x86_64-apple-darwin21.0.tar.xz
    // HERE WE USE LLVM_SYS_150_PREFIX for compatiblity with more systems
    let llvm_path = match which("llc") {
        Ok(path) => {
            if let Some(parent) = path.parent() {
                parent.to_str().unwrap().to_string()
            } else {
                eprintln!("Error finding 'llc': {:?}", path);
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("Error finding 'llc': {:?}", e);
            std::process::exit(1);
        }
    };
    unsafe {
        env::set_var("LLVM_SYS_150_PREFIX", llvm_path);
    }
}