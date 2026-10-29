fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用纯 Rust 的 protox 解析 proto, 无需系统安装 protoc
    let fds = protox::compile(["proto/monitor.proto"], ["proto"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    println!("cargo:rerun-if-changed=proto/monitor.proto");
    Ok(())
}
