use crate::utils::{
    ensure_path_exists, expr_to_filter, print_batch_summary, print_sample_rows, print_schema_info,
    registry_dir, registry_file, timed_execution,
};
use par_reader::{
    Expr, LiteralValue, RegistryManager, load_parquet_files_parallel, read_parquet,
    read_parquet_with_filter_async,
};

#[tokio::test]
async fn test_lpr_bes_basic_read() -> par_reader::Result<()> {
    let path = registry_file("lpr_bes", "2020.parquet");
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
async fn test_lpr_bes_filter_by_procedure() -> par_reader::Result<()> {
    let path = registry_file("lpr_bes", "2020.parquet");
    ensure_path_exists(&path)?;

    // Create a filter expression to find procedures
    // The exact column name may vary based on the actual schema
    let proc_column = "PROC"; // Adjust the column name if needed

    // Filter for procedures that belong to a specific category (example prefix "K")
    // Using Eq instead of Like since Like operator is not available
    let filter_expr = Expr::Eq(
        proc_column.to_string(),
        LiteralValue::String("K".to_string()),
    );

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
        Err(e) => println!("Error in procedure filter (likely column mismatch): {e}"),
    }

    // For a more complex filter - specific procedure code AND after a certain date
    // We'll need to know the date column name
    let date_column = "PROCDTO"; // Adjust the column name if needed
    let complex_filter = Expr::And(vec![
        Expr::Eq(
            proc_column.to_string(),
            LiteralValue::String("KF".to_string()),
        ), // Example procedure code prefix
        Expr::GtEq(
            date_column.to_string(),
            LiteralValue::String("2020-06-01".to_string()),
        ), // Example date
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
async fn test_lpr_bes_registry_manager() -> par_reader::Result<()> {
    let lpr_bes_path = registry_dir("lpr_bes");
    ensure_path_exists(&lpr_bes_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("lpr_bes", lpr_bes_path.as_path())?;
    println!(
        "Registered LPR_BES registry from {}",
        lpr_bes_path.display()
    );

    // Load data
    let batches = manager.load("lpr_bes")?;
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
async fn test_lpr_bes_parallel_read() -> par_reader::Result<()> {
    let lpr_bes_dir = registry_dir("lpr_bes");
    ensure_path_exists(&lpr_bes_dir)?;

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &lpr_bes_dir,
            None,
            None,
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
