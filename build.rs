use std::env;

fn main() {
    // PLEASE CHANGE THIS TO YOUR OWN LLVM VERSION AND PATH
    // For linux used: clang+llvm-16.0.0-x86_64-linux-gnu-ubuntu-18.04.tar.xz
    //env::set_var("LLVM_SYS_160_PREFIX", "~/Tools/clangllvm");
    // For macos with intel used: clang+llvm-15.0.7-x86_64-apple-darwin21.0.tar.xz


    // set accourdingly
    #[cfg(target_os="macos")]
    let llvm_path = "/Users/yongyongwei/Tools/clangllvm";

    #[cfg(target_os="linux")]
    let llvm_path = "/home/yy/Tools/clangllvm";

    env::set_var("LLVM_SYS_150_PREFIX", llvm_path);
}
/*
required by rust-analyzer
.vscode/settings.json
{
   "rust-analyzer.cargo.extraEnv": {
        "LLVM_SYS_160_PREFIX": "~/Tools/clangllvm"  
    },
}
*/