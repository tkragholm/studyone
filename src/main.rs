use par_reader::Result;
use std::path::Path;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Use the base directory with registry data
    let base_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet");
    if !base_dir.exists() {
        println!("Data directory not found: {}", base_dir.display());
        return Ok(());
    }
    println!("Hello :))");

    Ok(())
}
