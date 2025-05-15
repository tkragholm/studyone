use log::{info, warn};
use par_reader::Result;
use par_reader::registry::factory::{registry_from_name, registry_from_path};
use std::path::Path;
use std::time::Instant;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Use the base directory with registry data
    let base_dir = Path::new("/Users/tobiaskragholm/generated_data/parquet");
    if !base_dir.exists() {
        warn!("Data directory not found: {}", base_dir.display());
        return Ok(());
    }

    info!("Loading registry data from: {}", base_dir.display());

    // Example 1: Load BEF registry using registry_from_name
    let bef_path = base_dir.join("bef");
    if bef_path.exists() {
        info!("Loading BEF registry data...");
        let start = Instant::now();
        let registry = registry_from_name("bef")?;
        let batches = registry.load(&bef_path, None)?;
        info!(
            "Loaded {} BEF batches in {:?}",
            batches.len(),
            start.elapsed()
        );

        // Example of deserializing the batches to individuals
        if !batches.is_empty() {
            let sample_batch = &batches[0];
            let deserializer = par_reader::registry::bef::create_deserializer();
            let individuals =
                par_reader::registry::bef::deserialize_batch(&deserializer, sample_batch)?;
            info!(
                "Deserialized {} individuals from first BEF batch",
                individuals.len()
            );
        }
    }

    // Example 2: Load AKM registry using registry_from_path (auto-detect)
    let akm_path = base_dir.join("akm");
    if akm_path.exists() {
        info!("Loading AKM registry data...");
        let start = Instant::now();
        let registry = registry_from_path(&akm_path)?;
        let batches = registry.load(&akm_path, None)?;
        info!(
            "Loaded {} AKM batches in {:?}",
            batches.len(),
            start.elapsed()
        );
    }

    // Example 3: Load IND registry
    let ind_path = base_dir.join("ind");
    if ind_path.exists() {
        info!("Loading IND registry data...");
        let start = Instant::now();
        let registry = registry_from_name("ind")?;
        let batches = registry.load(&ind_path, None)?;
        info!(
            "Loaded {} IND batches in {:?}",
            batches.len(),
            start.elapsed()
        );
    }

    // Example 4: Load DOD registry
    let dod_path = base_dir.join("dod");
    if dod_path.exists() {
        info!("Loading DOD registry data...");
        let start = Instant::now();
        let registry = registry_from_name("dod")?;
        let batches = registry.load(&dod_path, None)?;
        info!(
            "Loaded {} DOD batches in {:?}",
            batches.len(),
            start.elapsed()
        );
    }

    // Example 5: Load multiple registries asynchronously
    info!("Loading multiple registries asynchronously...");
    
    // Create longer-lived path variables
    let bef_path = base_dir.join("bef");
    let akm_path = base_dir.join("akm");
    let ind_path = base_dir.join("ind");
    let dod_path = base_dir.join("dod");
    
    let registry_paths = [
        ("bef", bef_path.as_path()),
        ("akm", akm_path.as_path()),
        ("ind", ind_path.as_path()),
        ("dod", dod_path.as_path()),
    ];

    let start = Instant::now();
    let result = par_reader::registry::factory::load_multiple_registries_async(
        &registry_paths,
        None,
    )
    .await;

    match result {
        Ok(batches) => {
            info!(
                "Loaded {} total batches from multiple registries in {:?}",
                batches.len(),
                start.elapsed()
            );
        }
        Err(e) => {
            warn!("Error loading multiple registries: {}", e);
        }
    }

    info!("Registry loading examples completed successfully");
    Ok(())
}
