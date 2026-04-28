fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/example.proto");
    prost_build::compile_protos(&["proto/example.proto"], &["proto"])?;
    Ok(())
}
