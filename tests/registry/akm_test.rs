use crate::utils::{
    ensure_path_exists, print_batch_summary, print_sample_rows, print_schema_info, registry_dir,
    registry_file, timed_execution,
};
use par_reader::{
    Expr, LiteralValue, ParquetReader, RegistryManager, add_year_column, read_parquet,
    read_parquet_with_filter_async,
};

#[tokio::test]
async fn test_akm_basic_read() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
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
async fn test_akm_schema_validation() -> par_reader::Result<()> {
    let akm_dir = registry_dir("akm");
    ensure_path_exists(&akm_dir)?;

    let mut reader = ParquetReader::new();

    // Get a few year files
    let mut paths = Vec::new();
    for year in ["2020.parquet", "2021.parquet", "2022.parquet"] {
        let path = akm_dir.join(year);
        if path.exists() {
            paths.push(path.to_string_lossy().to_string());
            // Preload file
            match reader.read_file(&path.to_string_lossy()) {
                Ok(_) => println!("Loaded {}", path.display()),
                Err(e) => println!("Failed to load {}: {}", path.display(), e),
            }
        }
    }

    if paths.is_empty() {
        println!("No AKM files found to validate");
        return Ok(());
    }

    // Convert to string slices
    let path_refs: Vec<&str> = paths.iter().map(|s| &**s).collect();

    // Schema validation
    match reader.validate_schemas(&path_refs) {
        Ok(()) => println!("All schemas are compatible"),
        Err(e) => println!("Schema validation error: {}", e),
    }

    // Get detailed schema compatibility report
    match reader.get_schema_compatibility_report(&path_refs) {
        Ok(report) => {
            println!("Compatible: {}", report.compatible);
            if report.issues.is_empty() {
                println!("No issues found");
            } else {
                println!("Issues found:");
                for issue in report.issues {
                    println!("  - {:?}", issue);
                }
            }
        }
        Err(e) => println!("Error getting schema report: {}", e),
    }

    Ok(())
}

#[tokio::test]
async fn test_akm_filtering() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    ensure_path_exists(&path)?;

    // Simple filter: SOCIO > 200
    let filter_expr = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200));

    let result = read_parquet_with_filter_async(&path, &filter_expr, None, None).await?;
    println!("Filtered to {} record batches", result.len());
    println!("Total filtered rows: {}", result.iter().map(|batch| batch.num_rows()).sum::<usize>());

    // Complex filter: SOCIO > 200 AND CPRTYPE = 5
    let complex_filter = Expr::And(vec![
        Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200)),
        Expr::Eq("CPRTYPE".to_string(), LiteralValue::Int(5)),
    ]);

    let result = read_parquet_with_filter_async(&path, &complex_filter, None, None).await?;
    println!("Complex filtered to {} record batches", result.len());
    println!("Total complex filtered rows: {}", result.iter().map(|batch| batch.num_rows()).sum::<usize>());

    Ok(())
}

#[tokio::test]
async fn test_akm_transformation() -> par_reader::Result<()> {
    let path = registry_file("akm", "2020.parquet");
    ensure_path_exists(&path)?;

    let result = read_parquet::<std::collections::hash_map::RandomState>(&path, None, None)?;

    if let Some(first_batch) = result.first() {
        // Add year column if there's a date column
        if first_batch.schema().field_with_name("INDM_DAG").is_ok() {
            let date_col = "INDM_DAG";

            match add_year_column(first_batch, date_col) {
                Ok(transformed) => {
                    println!("Added year column successfully");
                    println!("Transformed schema:");
                    for field in transformed.schema().fields() {
                        println!("  - {} ({})", field.name(), field.data_type());
                    }

                    // Print a few rows to verify
                    print_sample_rows(&transformed, 3);
                }
                Err(e) => println!("Error adding year column: {}", e),
            }
        } else {
            println!("No date column found, skipping transformation");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_akm_registry_manager() -> par_reader::Result<()> {
    let akm_path = registry_dir("akm");
    ensure_path_exists(&akm_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("akm", akm_path.as_path())?;
    println!("Registered AKM registry from {}", akm_path.display());

    // Load data
    let batches = manager.load("akm")?;
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
