use par_reader::Result;
use par_reader::examples::sequential_registry_loader::{run_sequential_registry_example, run_unified_registry_example};
use std::env;
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

    // Check for command-line argument to use unified system
    let args: Vec<String> = env::args().collect();
    let use_unified = args.len() > 1 && args[1] == "--unified";

    // Filter parameters for the cohort
    let start_date = "20080101";
    let end_date = "20091231";

    if use_unified {
        println!("Using unified system for registry processing");
        
        // Run the unified registry processing example
        run_unified_registry_example(base_dir, start_date, end_date).await?;
    } else {
        println!("Using original system for registry processing");
        
        // Run the sequential registry processing example
        // This follows the logical data dependencies:
        // 1. Identify children from BEF based on birth date
        // 2. Get birth details from MFR and match with BEF
        // 3. Add mortality/migration from DOD/VNDS
        // 4. Add socioeconomic info from AKM/UDDF/IND
        let individuals_count = run_sequential_registry_example(base_dir, start_date, end_date).await?;

        println!("Successfully processed {individuals_count} individuals following a sequential approach");
    }

    Ok(())
}