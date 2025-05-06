use crate::utils::{
    ensure_path_exists, print_batch_summary, print_sample_rows, print_schema_info, registry_dir,
    registry_file, timed_execution,
};
use par_reader::{RegistryManager, add_year_column, load_parquet_files_parallel, read_parquet};

#[tokio::test]
async fn test_lpr_adm_basic_read() -> par_reader::Result<()> {
    let path = registry_file("lpr_adm", "2020.parquet");
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
async fn test_lpr_adm_parallel_read() -> par_reader::Result<()> {
    let lpr_adm_dir = registry_dir("lpr_adm");
    ensure_path_exists(&lpr_adm_dir)?;

    let (elapsed, result) = timed_execution(|| {
        load_parquet_files_parallel::<std::collections::hash_map::RandomState>(
            &lpr_adm_dir,
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

#[tokio::test]
async fn test_lpr_adm_date_transformation() -> par_reader::Result<()> {
    let path = registry_file("lpr_adm", "2020.parquet");
    ensure_path_exists(&path)?;

    let result =
        read_parquet::<std::collections::hash_map::RandomState>(&path, None, None, None, None)?;

    if let Some(first_batch) = result.first() {
        // Check if the file has the expected date column (assuming INDLÆGGELSESDATO is the date)
        let date_col = if first_batch
            .schema()
            .field_with_name("INDLÆGGELSESDATO")
            .is_ok()
        {
            "INDLÆGGELSESDATO"
        } else if first_batch.schema().field_with_name("INDDTO").is_ok() {
            "INDDTO"
        } else {
            println!("No date column found in LPR_ADM, skipping transformation");
            return Ok(());
        };

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
            Err(e) => println!("Error adding year column: {e}"),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_lpr_adm_registry_manager() -> par_reader::Result<()> {
    let lpr_adm_path = registry_dir("lpr_adm");
    ensure_path_exists(&lpr_adm_path)?;

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data source
    manager.register("lpr_adm", lpr_adm_path.as_path())?;
    println!(
        "Registered LPR_ADM registry from {}",
        lpr_adm_path.display()
    );

    // Load data
    let batches = manager.load("lpr_adm")?;
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
