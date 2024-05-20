use std::env;
fn main() {
    // PLEASE CHANGE THIS TO YOUR OWN LLVM VERSION AND PATH
    env::set_var("LLVM_SYS_160_PREFIX", "~/Tools/clangllvm");
}
/*
required by rust-analyzer
.vscode/settings.json
{
   "rust-analyzer.cargo.extraEnv": {
        "LLVM_SYS_160_PREFIX": "/home/yy/Tools/clangllvm"  
    },
}
*/