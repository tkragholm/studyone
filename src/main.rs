use par_reader::{
    Result,
};
use std::path::Path;
use std::io;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("Par-Reader - A Rust library for efficient Parquet file reading");
    println!("Run tests with 'cargo test' to see detailed functionality");
    println!("Example usage:");
    println!("1. Reading parquet file(s)");
    println!("2. Schema validation");
    println!("3. Filtering and transformation");
    println!("4. Registry operations");
    println!("5. Asynchronous operations");

    // Demo usage with basic registry manager example
    let data_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet");
    
    if !data_dir.exists() {
        println!("\nExample data directory not found at: {}", data_dir.display());
        println!("Adjust the file paths in the code to use your own Parquet files.");
        return Ok(());
    }

    println!("\nAvailable registries in {}", data_dir.display());
    if let Ok(entries) = std::fs::read_dir(data_dir) {
        for entry in entries.filter_map(|e: io::Result<_>| e.ok()) {
            if entry.path().is_dir() {
                println!("  - {}", entry.file_name().to_string_lossy());
            }
        }
    }

    println!("\nFor comprehensive examples, please run the test suite with:");
    println!("  cargo test");
    println!("\nOr explore specific test modules:");
    println!("  cargo test -p par-reader tests::registry::akm_test");
    println!("  cargo test -p par-reader tests::integration::filtering_test");
    println!("  cargo test -p par-reader tests::integration::async_test");

    Ok(())
}