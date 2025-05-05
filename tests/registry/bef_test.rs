use std::collections::HashSet;

use crate::utils::{
    ensure_path_exists, print_batch_summary, print_sample_rows, print_schema_info, registry_dir,
    registry_file, timed_execution,
};
use par_reader::{RegistryManager, load_parquet_files_parallel, read_parquet};

#[tokio::test]
async fn test_bef_basic_read() -> par_reader::Result<()> {
    let path = registry_file("bef", "2020.parquet");
    ensure_path_exists(&path)?;

    let (elapsed, result) = timed_execution(|| {
        read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)
    });

    let batches = result?;
    print_batch_summary(&batches, elapsed);

    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
        print_sample_rows(first_batch, 3);
    }

    Ok(())
}

#[tokio::test]
async fn test_bef_parallel_read() -> par_reader::Result<()> {
    let bef_dir = registry_dir("bef");
    ensure_path_exists(&bef_dir)?;

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(&bef_dir, None, None)
    });

    let batches = result?;
    print_batch_summary(&batches, elapsed);

    // Print schema of first batch if available
    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}

#[tokio::test]
async fn test_bef_registry_manager() -> par_reader::Result<()> {
    let bef_path = registry_dir("bef");
    ensure_path_exists(&bef_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("bef", bef_path.as_path())?;
    println!("Registered BEF registry from {}", bef_path.display());

    // Load data
    let batches = manager.load("bef")?;
    println!("Loaded {} record batches", batches.len());
    println!(
        "Total rows: {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // Print schema of first batch if available
    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}

#[tokio::test]
async fn test_bef_pnr_filter() -> par_reader::Result<()> {
    let bef_path = registry_dir("bef");
    ensure_path_exists(&bef_path)?;

    let akm_path = registry_dir("akm");
    if !akm_path.exists() {
        println!("AKM directory not found, skipping cross-registry PNR filter test");
        return Ok(());
    }

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data sources
    manager.register("bef", bef_path.as_path())?;
    manager.register("akm", akm_path.as_path())?;

    // Create a sample PNR filter with some synthetic PNRs
    let mut pnr_filter = HashSet::new();
    pnr_filter.insert("0101701234".to_string());
    pnr_filter.insert("0101801234".to_string());
    pnr_filter.insert("0101901234".to_string());

    let filtered_data = manager.filter_by_pnr(&["bef", "akm"], &pnr_filter)?;

    for (registry, batches) in filtered_data {
        println!(
            "{registry}: {} batches with {} total rows",
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        // Print schema of first batch if available
        if let Some(first_batch) = batches.first() {
            print_schema_info(first_batch);
        }
    }

    Ok(())
}
