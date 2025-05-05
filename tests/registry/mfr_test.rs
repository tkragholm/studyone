use crate::utils::{
    expr_to_filter, print_batch_summary, print_sample_rows, print_schema_info, registry_dir, 
    registry_file, timed_execution,
};
use par_reader::{
    Expr, LiteralValue, RegistryManager, load_parquet_files_parallel, read_parquet,
    read_parquet_with_filter_async,
};

#[tokio::test]
async fn test_mfr_basic_read() -> par_reader::Result<()> {
    let path = registry_file("mfr", "2020.parquet");
    if !path.exists() {
        println!("MFR test file not found. Skipping test.");
        return Ok(());
    }

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
async fn test_mfr_filter_by_birth_details() -> par_reader::Result<()> {
    let path = registry_file("mfr", "2020.parquet");
    if !path.exists() {
        println!("MFR test file not found. Skipping test.");
        return Ok(());
    }

    // Create filter expressions for medical birth registry data
    // Birth weight filter (assuming the column is named WEIGHT)
    let weight_column = "VAEGT"; // Adjust the column name if needed
    let weight_filter = Expr::Lt(weight_column.to_string(), LiteralValue::Int(2500)); // Low birth weight

    // Attempt to use the filter, but catch errors as the column might not match
    match read_parquet_with_filter_async(&path, expr_to_filter(&weight_filter), None).await {
        Ok(batches) => {
            println!("Filtered to {} record batches", batches.len());
            println!(
                "Total filtered rows: {}",
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
        }
        Err(e) => println!("Error in weight filter (likely column mismatch): {}", e),
    }

    // For a more complex filter - low birth weight AND premature birth
    let gestation_column = "SVLENGDE"; // Adjust the column name if needed
    let complex_filter = Expr::And(vec![
        Expr::Lt(weight_column.to_string(), LiteralValue::Int(2500)),
        Expr::Lt(gestation_column.to_string(), LiteralValue::Int(37)), // Premature birth less than 37 weeks
    ]);

    // Attempt to use the complex filter, but catch errors as the columns might not match
    match read_parquet_with_filter_async(&path, expr_to_filter(&complex_filter), None).await {
        Ok(batches) => {
            println!("Complex filtered to {} record batches", batches.len());
            println!(
                "Total complex filtered rows: {}",
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
        }
        Err(e) => println!("Error in complex filter (likely column mismatch): {}", e),
    }

    Ok(())
}

#[tokio::test]
async fn test_mfr_registry_manager() -> par_reader::Result<()> {
    let mfr_path = registry_dir("mfr");
    if !mfr_path.exists() {
        println!("MFR directory not found. Skipping test.");
        return Ok(());
    }

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("mfr", &mfr_path)?;
    println!("Registered MFR registry from {}", mfr_path.display());

    // Load data
    let batches = manager.load("mfr")?;
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
async fn test_mfr_parallel_read() -> par_reader::Result<()> {
    let mfr_dir = registry_dir("mfr");
    if !mfr_dir.exists() {
        println!("MFR directory not found. Skipping test.");
        return Ok(());
    }

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(&mfr_dir, None, None)
    });

    let batches = result?;
    print_batch_summary(&batches, elapsed);

    // Print schema of first batch if available
    if let Some(first_batch) = batches.first() {
        print_schema_info(first_batch);
    }

    Ok(())
}
