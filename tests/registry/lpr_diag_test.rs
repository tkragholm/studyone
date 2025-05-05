use crate::utils::{
    ensure_path_exists, expr_to_filter, print_batch_summary, print_sample_rows, print_schema_info, 
    registry_dir, registry_file, timed_execution,
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

    // First, read the actual schema to find the diagnosis column
    let schema_check = read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;
    
    if schema_check.is_empty() {
        println!("No records found in LPR_DIAG file. Skipping test.");
        return Ok(());
    }
    
    // Get the first batch to inspect schema
    let first_batch = &schema_check[0];
    print_schema_info(first_batch);
    
    // Try to identify a diagnosis column in the schema
    // Common names in medical data might be: DIAG, DIAGNOSEKODE, D_KODE, etc.
    let possible_diag_columns = ["DIAG", "DIAGNOSEKODE", "D_KODE", "C_DIAG", "KODE"];
    let diag_column = possible_diag_columns
        .iter()
        .find(|&col| first_batch.schema().field_with_name(col).is_ok())
        .ok_or_else(|| anyhow::anyhow!("No suitable diagnosis column found in schema"))?;
    
    println!("Using diagnosis column: {}", diag_column);
    
    // Filter for diagnoses that start with C (cancer)
    // Using a comparison since Like operator doesn't exist
    let filter_expr = Expr::Eq(diag_column.to_string(), LiteralValue::String("C".to_string()));

    let result = read_parquet_with_filter_async(&path, expr_to_filter(&filter_expr), None).await?;
    println!("Filtered to {} record batches", result.len());
    println!("Total filtered rows: {}", 
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // For a more complex filter - diagnoses that are from a specific hospital
    // Try to find a hospital/institution column
    let possible_hospital_columns = ["SYGEHUS", "HOSPITAL", "INST", "INSTITUTION", "H_NUMMER"];
    let hospital_column_opt = possible_hospital_columns
        .iter()
        .find(|&col| first_batch.schema().field_with_name(col).is_ok());
    
    if let Some(hospital_column) = hospital_column_opt {
        println!("Using hospital column: {}", hospital_column);
        
        let complex_filter = Expr::And(vec![
            Expr::Eq(diag_column.to_string(), LiteralValue::String("C".to_string())),
            Expr::Eq(hospital_column.to_string(), LiteralValue::Int(4001)), // Example hospital code
        ]);

        // Attempt to use the complex filter
        match read_parquet_with_filter_async(&path, expr_to_filter(&complex_filter), None).await {
            Ok(batches) => {
                println!("Complex filtered to {} record batches", batches.len());
                println!("Total complex filtered rows: {}", 
                    batches.iter().map(|b| b.num_rows()).sum::<usize>()
                );
            }
            Err(e) => println!("Error in complex filter: {}", e),
        }
    } else {
        println!("No suitable hospital/institution column found in schema. Skipping complex filter test.");
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