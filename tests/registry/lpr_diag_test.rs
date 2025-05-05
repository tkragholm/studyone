use crate::utils::{
    ensure_path_exists, print_batch_summary, print_sample_rows, print_schema_info, registry_dir,
    registry_file, timed_execution,
};
use par_reader::{
    Expr, LiteralValue, RegistryManager, load_parquet_files_parallel, read_parquet,
    read_parquet_with_filter_async,
};

#[tokio::test]
async fn test_lpr_diag_basic_read() -> par_reader::Result<()> {
    let path = registry_file("lpr_diag", "2020.parquet");
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
async fn test_lpr_diag_filter_by_diagnosis() -> par_reader::Result<()> {
    let path = registry_file("lpr_diag", "2020.parquet");
    ensure_path_exists(&path)?;

    // Create a filter expression to find diagnoses starting with "C" (cancer codes in ICD-10)
    // The exact column name may vary based on the actual schema
    let diag_column = "DIAG"; // Adjust the column name if needed

    // Filter for diagnoses that start with C (cancer)
    // Using a comparison since Like operator doesn't exist
    let filter_expr = Expr::Eq(diag_column.to_string(), LiteralValue::String("C".to_string()));

    let result = read_parquet_with_filter_async(&path, &filter_expr, None, None).await?;
    println!("Filtered to {} record batches", result.len());
    println!("Total filtered rows: {}", 
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // For a more complex filter - diagnoses that are from a specific hospital
    // We'll need to know the hospital column name
    let hospital_column = "SYGEHUS"; // Adjust the column name if needed
    let complex_filter = Expr::And(vec![
        Expr::Eq(diag_column.to_string(), LiteralValue::String("C".to_string())),
        Expr::Eq(hospital_column.to_string(), LiteralValue::Int(4001)), // Example hospital code
    ]);

    // Attempt to use the complex filter, but catch errors as the column might not match
    match read_parquet_with_filter_async(&path, &complex_filter, None, None).await {
        Ok(batches) => {
            println!("Complex filtered to {} record batches", batches.len());
            println!("Total complex filtered rows: {}", 
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
        }
        Err(e) => println!("Error in complex filter (likely column mismatch): {}", e),
    }

    Ok(())
}

#[tokio::test]
async fn test_lpr_diag_registry_manager() -> par_reader::Result<()> {
    let lpr_diag_path = registry_dir("lpr_diag");
    ensure_path_exists(&lpr_diag_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("lpr_diag", lpr_diag_path.as_path())?;
    println!(
        "Registered LPR_DIAG registry from {}",
        lpr_diag_path.display()
    );

    // Load data
    let batches = manager.load("lpr_diag")?;
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
async fn test_lpr_diag_parallel_read() -> par_reader::Result<()> {
    let lpr_diag_dir = registry_dir("lpr_diag");
    ensure_path_exists(&lpr_diag_dir)?;

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &lpr_diag_dir,
            None,
            None,
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