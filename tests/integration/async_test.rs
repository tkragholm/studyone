use crate::utils::{expr_to_filter, print_schema_info, registry_dir, registry_file};
use futures::future::join_all;
use par_reader::{
    Expr, LiteralValue, load_parquet_files_parallel_async, read_parquet_async,
    read_parquet_with_filter_async,
};

/// Test basic async reading of parquet files
#[tokio::test]
async fn test_async_read() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    println!("Testing async read of a single file:");
    let start = std::time::Instant::now();
    let batches = read_parquet_async(&path, None, None).await?;
    let elapsed = start.elapsed();

    println!("Read {} record batches in {:?}", batches.len(), elapsed);
    println!(
        "Total rows: {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}

/// Test async filtering operations
#[tokio::test]
async fn test_async_filtering() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Simple filter: SOCIO > 200
    let filter_expr = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200));

    println!("Testing async filtering with condition (SOCIO > 200):");
    let start = std::time::Instant::now();
    let batches = read_parquet_with_filter_async(&path, expr_to_filter(&filter_expr), None).await?;
    let elapsed = start.elapsed();

    println!(
        "Read {} filtered record batches in {:?}",
        batches.len(),
        elapsed
    );
    println!(
        "Total filtered rows: {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    Ok(())
}

/// Test parallel async reading of multiple files
#[tokio::test]
async fn test_parallel_async_read() -> par_reader::Result<()> {
    let akm_dir = registry_dir("akm");
    if !akm_dir.exists() {
        println!("AKM directory not found. Skipping test.");
        return Ok(());
    }

    println!("Testing parallel async read of multiple files:");
    let start = std::time::Instant::now();
    let batches = load_parquet_files_parallel_async(&akm_dir, None, None).await?;
    let elapsed = start.elapsed();

    println!("Read {} record batches in {:?}", batches.len(), elapsed);
    println!(
        "Total rows: {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    Ok(())
}

/// Test concurrent async operations
#[tokio::test]
async fn test_concurrent_async_operations() -> par_reader::Result<()> {
    let registries = ["akm", "bef", "lpr_adm", "lpr_diag", "lpr_bes", "ind", "mfr"];

    // Collect tasks for all available registries
    let mut registry_dirs = Vec::new();
    let mut registry_names = Vec::new();

    for &registry in &registries {
        let dir = registry_dir(registry);
        if dir.exists() {
            println!("Adding {} registry to concurrent processing", registry);
            registry_dirs.push(dir);
            registry_names.push(registry);
        }
    }

    if registry_dirs.is_empty() {
        println!("No registry directories found. Skipping test.");
        return Ok(());
    }

    // Create tasks for each directory
    let mut tasks = Vec::new();
    for dir in &registry_dirs {
        // Create a task with a cloned PathBuf
        let dir_clone = dir.clone();
        let task =
            tokio::spawn(
                async move { load_parquet_files_parallel_async(&dir_clone, None, None).await },
            );
        tasks.push(task);
    }

    println!(
        "Starting concurrent processing of {} registries...",
        tasks.len()
    );
    let start = std::time::Instant::now();

    // Execute all tasks concurrently
    let results = join_all(tasks).await;

    let elapsed = start.elapsed();
    println!("Completed all concurrent operations in {:?}", elapsed);

    // Process results
    let mut total_batches = 0;
    let mut total_rows = 0;
    let mut success_count = 0;

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(batches)) => {
                let registry = if i < registry_names.len() {
                    registry_names[i]
                } else {
                    "unknown"
                };
                let registry_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

                println!(
                    "  {}: {} batches with {} rows",
                    registry,
                    batches.len(),
                    registry_rows
                );

                total_batches += batches.len();
                total_rows += registry_rows;
                success_count += 1;
            }
            _ => {
                let registry = if i < registry_names.len() {
                    registry_names[i]
                } else {
                    "unknown"
                };
                println!("  Error processing {}", registry);
            }
        }
    }

    println!("\nSummary of concurrent operations:");
    println!(
        "Successfully processed {}/{} registries",
        success_count,
        registry_names.len()
    );
    println!("Total batches: {}", total_batches);
    println!("Total rows: {}", total_rows);

    Ok(())
}

/// Test async performance comparison with sync operations
#[tokio::test]
async fn test_async_vs_sync_performance() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    if !path.exists() {
        println!("AKM test file not found. Skipping test.");
        return Ok(());
    }

    // Test synchronous read
    println!("Testing synchronous read:");
    let sync_start = std::time::Instant::now();
    let sync_batches =
        par_reader::read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;
    let sync_elapsed = sync_start.elapsed();

    println!(
        "Sync read: {} batches in {:?}",
        sync_batches.len(),
        sync_elapsed
    );
    println!(
        "Sync total rows: {}",
        sync_batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // Test asynchronous read
    println!("\nTesting asynchronous read:");
    let async_start = std::time::Instant::now();
    let async_batches = read_parquet_async(&path, None, None).await?;
    let async_elapsed = async_start.elapsed();

    println!(
        "Async read: {} batches in {:?}",
        async_batches.len(),
        async_elapsed
    );
    println!(
        "Async total rows: {}",
        async_batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // Compare performance
    println!("\nPerformance comparison:");
    println!("Sync read time: {:?}", sync_elapsed);
    println!("Async read time: {:?}", async_elapsed);

    let speedup = sync_elapsed.as_micros() as f64 / async_elapsed.as_micros() as f64;
    println!("Speedup factor: {:.2}x", speedup);

    Ok(())
}
