use par_reader::Result;
use par_reader::examples::async_loader_example::run_async_loader_example;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Use the real data directory with generated data
    let data_dir = std::path::Path::new("/Users/tobiaskragholm/generated_data/parquet");
    if !data_dir.exists() {
        println!(
            "Data directory not found, skipping test: {}",
            data_dir.display()
        );
        return Ok(());
    }

    println!("Running async loader example with real data directory");
    run_async_loader_example(data_dir).await?;

    Ok(())
}
