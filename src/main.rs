use par_reader::Result;
use par_reader::examples::sequential_registry_loader::run_sequential_registry_example;
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

    // Run the sequential registry processing example
    // This follows the logical data dependencies:
    // 1. Identify children from BEF based on birth date
    // 2. Get birth details from MFR and match with BEF
    // 3. Add mortality/migration from DOD/VNDS
    // 4. Add socioeconomic info from AKM/UDDF/IND
    let individuals_count = run_sequential_registry_example(base_dir).await?;

    println!("Successfully processed {individuals_count} individuals following a sequential approach");

    Ok(())
}
