use crate::utils::{
    get_available_year_files, print_batch_summary, print_sample_rows, print_schema_info,
    registry_dir, timed_execution,
};
use par_reader::{RegistryManager, load_parquet_files_parallel, read_parquet};

#[tokio::test]
async fn test_vnds_basic_read() -> par_reader::Result<()> {
    // VNDS directory might contain fewer files, so we'll first check which files are available
    let vnds_files = get_available_year_files("vnds");

    if vnds_files.is_empty() {
        println!("No VNDS parquet files found. Skipping test.");
        return Ok(());
    }

    let path = &vnds_files[0]; // Use the first available file

    let (elapsed, result) = timed_execution(|| {
        read_parquet::<std::collections::hash_map::RandomState>(path, None, None, None, None)
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
async fn test_vnds_registry_manager() -> par_reader::Result<()> {
    let vnds_path = registry_dir("vnds");

    if !vnds_path.exists()
        || std::fs::read_dir(&vnds_path)
            .map(std::iter::Iterator::count)
            .unwrap_or(0)
            == 0
    {
        println!("VNDS directory empty or not found. Skipping test.");
        return Ok(());
    }

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("vnds", &vnds_path)?;
    println!("Registered VNDS registry from {}", vnds_path.display());

    // Load data
    let batches = manager.load("vnds")?;
    println!("Loaded {} record batches", batches.len());
    println!(
        "Total rows: {}",
        batches
            .iter()
            .map(par_reader::RecordBatch::num_rows)
            .sum::<usize>()
    );

    // Print schema of first batch if available
    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}

#[tokio::test]
async fn test_vnds_parallel_read() -> par_reader::Result<()> {
    let vnds_dir = registry_dir("vnds");

    if !vnds_dir.exists()
        || std::fs::read_dir(&vnds_dir)
            .map(std::iter::Iterator::count)
            .unwrap_or(0)
            == 0
    {
        println!("VNDS directory empty or not found. Skipping test.");
        return Ok(());
    }

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &vnds_dir, None, None, None, None,
        )
    });

    let batches = result?;
    print_batch_summary(&batches, elapsed);

    // Print schema of first batch if available
    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}
