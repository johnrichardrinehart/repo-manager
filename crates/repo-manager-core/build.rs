fn main() -> std::io::Result<()> {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    prost_build::compile_protos(&["../../api/repo_manager/v1/rpc.proto"], &["../../api"])?;
    println!("cargo:rerun-if-changed=../../api/repo_manager/v1/rpc.proto");
    Ok(())
}
