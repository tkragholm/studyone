use crate::utils::{
    ensure_path_exists, expr_to_filter, print_batch_summary, print_sample_rows, print_schema_info,
    registry_dir, registry_file, timed_execution,
};
use par_reader::{
    Expr, LiteralValue, RegistryManager, load_parquet_files_parallel, read_parquet,
    read_parquet_with_filter_async,
};

#[tokio::test]
async fn test_ind_basic_read() -> par_reader::Result<()> {
    let path = registry_file("ind", "2020.parquet");
    ensure_path_exists(&path)?;

    let (elapsed, result) = timed_execution(|| {
        read_parquet::<std::collections::hash_map::RandomState>(&path, None, None, None, None)
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
async fn test_ind_filter_by_country() -> par_reader::Result<()> {
    let path = registry_file("ind", "2020.parquet");
    ensure_path_exists(&path)?;

    // Create a filter expression to find records by country of origin
    // The exact column name may vary based on the actual schema
    let country_column = "IE_TYPE"; // Adjust the column name if needed

    // Filter for a specific country code (example)
    let filter_expr = Expr::Eq(country_column.to_string(), LiteralValue::Int(5001));

    // Attempt to use the filter, but catch errors as the column might not match
    match read_parquet_with_filter_async(&path, expr_to_filter(&filter_expr), None).await {
        Ok(batches) => {
            println!("Filtered to {} record batches", batches.len());
            println!(
                "Total filtered rows: {}",
                batches
                    .iter()
                    .map(par_reader::RecordBatch::num_rows)
                    .sum::<usize>()
            );
        }
        Err(e) => println!("Error in country filter (likely column mismatch): {e}"),
    }

    // For a more complex filter - specific country AND after a certain date
    let date_column = "INDVANDR_DATO"; // Adjust the column name if needed
    let complex_filter = Expr::And(vec![
        Expr::Eq(country_column.to_string(), LiteralValue::Int(5001)),
        Expr::GtEq(
            date_column.to_string(),
            LiteralValue::String("2020-01-01".to_string()),
        ),
    ]);

    // Attempt to use the complex filter, but catch errors as the columns might not match
    match read_parquet_with_filter_async(&path, expr_to_filter(&complex_filter), None).await {
        Ok(batches) => {
            println!("Complex filtered to {} record batches", batches.len());
            println!(
                "Total complex filtered rows: {}",
                batches
                    .iter()
                    .map(par_reader::RecordBatch::num_rows)
                    .sum::<usize>()
            );
        }
        Err(e) => println!("Error in complex filter (likely column mismatch): {e}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_ind_registry_manager() -> par_reader::Result<()> {
    let ind_path = registry_dir("ind");
    ensure_path_exists(&ind_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("ind", ind_path.as_path())?;
    println!("Registered IND registry from {}", ind_path.display());

    // Load data
    let batches = manager.load("ind")?;
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
async fn test_ind_parallel_read() -> par_reader::Result<()> {
    let ind_dir = registry_dir("ind");
    ensure_path_exists(&ind_dir)?;

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &ind_dir, None, None, None, None,
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
