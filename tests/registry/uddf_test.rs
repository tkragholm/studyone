use crate::utils::{
    get_available_year_files, print_batch_summary, print_sample_rows, print_schema_info,
    registry_dir, timed_execution,
};
use par_reader::{RegistryManager, load_parquet_files_parallel, read_parquet};

#[tokio::test]
async fn test_uddf_basic_read() -> par_reader::Result<()> {
    // UDDF directory might contain fewer files, so we'll first check which files are available
    let uddf_files = get_available_year_files("uddf");

    if uddf_files.is_empty() {
        println!("No UDDF parquet files found. Skipping test.");
        return Ok(());
    }

    let path = &uddf_files[0]; // Use the first available file

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
async fn test_uddf_registry_manager() -> par_reader::Result<()> {
    let uddf_path = registry_dir("uddf");

    if !uddf_path.exists()
        || std::fs::read_dir(&uddf_path)
            .map(std::iter::Iterator::count)
            .unwrap_or(0)
            == 0
    {
        println!("UDDF directory empty or not found. Skipping test.");
        return Ok(());
    }

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("uddf", &uddf_path)?;
    println!("Registered UDDF registry from {}", uddf_path.display());

    // Load data
    let batches = manager.load("uddf")?;
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
async fn test_uddf_parallel_read() -> par_reader::Result<()> {
    let uddf_dir = registry_dir("uddf");

    if !uddf_dir.exists()
        || std::fs::read_dir(&uddf_dir)
            .map(std::iter::Iterator::count)
            .unwrap_or(0)
            == 0
    {
        println!("UDDF directory empty or not found. Skipping test.");
        return Ok(());
    }

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &uddf_dir, None, None, None, None,
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
