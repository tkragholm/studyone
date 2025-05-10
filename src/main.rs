use par_reader::Result;
use par_reader::examples::conversion_example::run_conversion_example;
use std::path::Path;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Use the data directory with BEF files
    let data_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet/bef");
    if !data_dir.exists() {
        println!("BEF data directory not found: {}", data_dir.display());
        return Ok(());
    }

    // Run the conversion example which loads BEF files, filters by date range,
    // and converts Arrow RecordBatches to Individual models
    let individuals_count = run_conversion_example(data_dir).await?;

    println!("Successfully processed {individuals_count} Individual records");

    Ok(())
}
