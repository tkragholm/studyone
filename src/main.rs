use par_reader::{
    // Arrow types
    Expr,
    LiteralValue,
    RecordBatch,
    // Original types
    ParquetReader,
    ParquetReaderConfig,
    Result,
    load_parquet_files_parallel,
    load_parquet_files_parallel_async,
    // Utility functions
    read_parquet,
    // Async functionality
    read_parquet_async,
    read_parquet_with_filter_async,
    // Registry functionality
    RegistryManager,
    // Registry types removed as they're unused
    add_year_column,
};

use parquet::file::metadata::ParquetMetaDataReader;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Create reader config
    let _config = ParquetReaderConfig {
        read_page_indexes: true,
        validate_schema: true,
        fail_on_schema_incompatibility: false,
        ..Default::default()
    };

    // Real files for testing - adjust these paths to your environment
    let paths = ["/Users/tobiaskragholm/generated_data/parquet/akm/2020.parquet",
        "/Users/tobiaskragholm/generated_data/parquet/akm/2021.parquet",
        "/Users/tobiaskragholm/generated_data/parquet/akm/2022.parquet"];

    // If the files don't exist, use an example message
    if !Path::new(&paths[0]).exists() {
        println!("Example files not found. This is a demo of the library's capabilities.");
        println!(
            "To use this example, adjust the file paths in the code to your own Parquet files."
        );
        return Ok(());
    }

    println!("============= ORIGINAL IMPLEMENTATION =============");

    let mut reader = ParquetReader::new();

    // Use string slices directly
    let path_refs: Vec<&str> = paths.iter().map(|s| &**s).collect();

    // Preload all files to cache their metadata
    println!("Preloading files to cache metadata...");
    for path in &path_refs {
        match reader.read_file(path) {
            Ok(_) => println!("  Loaded {path}"),
            Err(e) => println!("  Failed to load {path}: {e}"),
        }
    }

    // Schema validation
    println!("\nValidating schemas across files:");
    match reader.validate_schemas(&path_refs) {
        Ok(()) => println!("  All schemas are compatible"),
        Err(e) => println!("  Schema validation error: {e}"),
    }

    // Get detailed schema compatibility report
    println!("\nDetailed schema compatibility report:");
    match reader.get_schema_compatibility_report(&path_refs) {
        Ok(report) => {
            println!("  Compatible: {}", report.compatible);
            if report.issues.is_empty() {
                println!("  No issues found");
            } else {
                println!("  Issues found:");
                for issue in report.issues {
                    println!("    - {issue:#?}");
                }
            }
        }
        Err(e) => println!("  Error getting schema report: {e}"),
    }

    println!("\n============= NEW ARROW IMPLEMENTATION =============");

    // Example: Read a single file with Arrow
    if let Some(path) = paths.first() {
        println!("\nReading a single file with Arrow ({path}):");
        let start = Instant::now();
        match read_parquet(Path::new(path), None, None) {
            Ok(batches) => {
                println!(
                    "  Read {} record batches in {:?}",
                    batches.len(),
                    start.elapsed()
                );
                println!(
                    "  Total rows: {}",
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>()
                );

                // Print some sample data from the first batch
                if let Some(first_batch) = batches.first() {
                    println!("\n  Schema:");
                    for field in first_batch.schema().fields() {
                        println!("    - {} ({})", field.name(), field.data_type());
                    }

                    println!("\n  First 3 rows:");
                    for row_idx in 0..std::cmp::min(3, first_batch.num_rows()) {
                        print!("    Row {row_idx}: [");
                        for col_idx in 0..first_batch.num_columns() {
                            let column = first_batch.column(col_idx);
                            print!("{}: ", first_batch.schema().field(col_idx).name());

                            if column.is_null(row_idx) {
                                print!("NULL");
                            } else {
                                print!("Value"); // Simplified - actual value display would depend on column type
                            }

                            if col_idx < first_batch.num_columns() - 1 {
                                print!(", ");
                            }
                        }
                        println!("]");
                    }
                }
            }
            Err(e) => println!("  Error reading file: {e}"),
        }
    }

    // Example: Parallel reading of multiple files
    println!("\nReading multiple files in parallel:");
    let start = Instant::now();
    match load_parquet_files_parallel(Path::new(&paths[0]).parent().unwrap(), None, None) {
        Ok(batches) => {
            println!(
                "  Read {} record batches in {:?}",
                batches.len(),
                start.elapsed()
            );
            println!(
                "  Total rows: {}",
                batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            );
        }
        Err(e) => println!("  Error reading files: {e}"),
    }

    println!("\n============= FILTERING CAPABILITIES =============");

    // Example: Create and use a simple filter
    // Using actual columns from the data: SOCIO > 200
    println!("\nFiltering with a simple condition (SOCIO > 200):");
    let filter_expr = Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200));

    if let Some(path) = paths.first() {
        match read_parquet_with_filter_async(Path::new(path), &filter_expr, None, None).await {
            Ok(batches) => {
                println!("  Filtered to {} record batches", batches.len());
                println!(
                    "  Total filtered rows: {}",
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>()
                );
            }
            Err(e) => println!("  Error applying filter: {e}"),
        }
    }

    // Example: Create and use a more complex filter
    println!("\nFiltering with a complex condition (SOCIO > 200 AND CPRTYPE = 5):");
    let complex_filter = Expr::And(vec![
        Expr::Gt("SOCIO".to_string(), LiteralValue::Int(200)),
        Expr::Eq(
            "CPRTYPE".to_string(),
            LiteralValue::Int(5),
        ),
    ]);

    if let Some(path) = paths.first() {
        match read_parquet_with_filter_async(Path::new(path), &complex_filter, None, None).await {
            Ok(batches) => {
                println!("  Filtered to {} record batches", batches.len());
                println!(
                    "  Total filtered rows: {}",
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>()
                );
            }
            Err(e) => println!("  Error applying complex filter: {e}"),
        }
    }

    println!("\n============= ASYNC CAPABILITIES =============");

    // Example: Read asynchronously
    println!("\nReading a file asynchronously:");
    if let Some(path) = paths.first() {
        let start = Instant::now();
        match read_parquet_async(Path::new(path), None, None).await {
            Ok(batches) => {
                println!(
                    "  Read {} record batches in {:?}",
                    batches.len(),
                    start.elapsed()
                );
                println!(
                    "  Total rows: {}",
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>()
                );
            }
            Err(e) => println!("  Error reading file asynchronously: {e}"),
        }
    }

    // Example: Read multiple files asynchronously in parallel
    println!("\nReading multiple files asynchronously in parallel:");
    let start = Instant::now();
    match load_parquet_files_parallel_async(Path::new(&paths[0]).parent().unwrap(), None, None)
        .await
    {
        Ok(batches) => {
            println!(
                "  Read {} record batches in {:?}",
                batches.len(),
                start.elapsed()
            );
            println!(
                "  Total rows: {}",
                batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            );
        }
        Err(e) => println!("  Error reading files asynchronously: {e}"),
    }

    println!("\n============= REGISTRY SYSTEM =============");

    // Create a registry manager
    let manager = RegistryManager::new();

    // Register data sources - these paths would need to be adjusted for your environment
    let akm_path = Path::new(&paths[0]).parent().unwrap();
    let bef_path = Path::new("/Users/tobiaskragholm/generated_data/parquet/bef");
    
    println!("\nRegistering data sources:");
    
    match manager.register("akm", akm_path) {
        Ok(()) => println!("  Registered AKM registry from {}", akm_path.display()),
        Err(e) => println!("  Failed to register AKM registry: {e}"),
    }
    
    // Register BEF if it exists
    if bef_path.exists() {
        match manager.register("bef", bef_path) {
            Ok(()) => println!("  Registered BEF registry from {}", bef_path.display()),
            Err(e) => println!("  Failed to register BEF registry: {e}"),
        }
    }
    
    // Load data from AKM registry
    println!("\nLoading AKM registry data:");
    match manager.load("akm") {
        Ok(batches) => {
            println!("  Loaded {} record batches", batches.len());
            println!(
                "  Total rows: {}",
                batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            );
            
            // Sample transformation - add year column
            if let Some(first_batch) = batches.first() {
                println!("\n  Applying date transformation (add year column):");
                
                // Only perform if there's a date column
                if first_batch.schema().field_with_name("INDM_DAG").is_ok() {
                    let date_col = "INDM_DAG";
                    
                    match add_year_column(first_batch, date_col) {
                        Ok(transformed) => {
                            println!("  Added year column successfully");
                            println!("  Transformed schema:");
                            for field in transformed.schema().fields() {
                                println!("    - {} ({})", field.name(), field.data_type());
                            }
                        }
                        Err(e) => println!("  Error adding year column: {e}"),
                    }
                } else {
                    println!("  No date column found, skipping transformation");
                }
            }
        }
        Err(e) => println!("  Error loading AKM registry: {e}"),
    }
    
    // Filter by PNR if we have multiple registries
    if bef_path.exists() {
        println!("\nFiltering data by PNR across registries:");
        
        // Create a sample PNR filter with some synthetic PNRs
        let mut pnr_filter = HashSet::new();
        pnr_filter.insert("0101701234".to_string());
        pnr_filter.insert("0101801234".to_string());
        pnr_filter.insert("0101901234".to_string());
        
        match manager.filter_by_pnr(&["akm", "bef"], &pnr_filter) {
            Ok(filtered_data) => {
                for (registry, batches) in filtered_data {
                    println!("  {registry}: {} batches with {} total rows", 
                        batches.len(),
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
                    );
                }
            }
            Err(e) => println!("  Error filtering by PNR: {e}"),
        }
    }

    println!("\n============= METADATA OPERATIONS =============");

    // Example: Read metadata with page indexes
    println!("\nReading metadata with page indexes:");
    if let Some(path) = paths.first() {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error opening file {path}: {e}");
                return Err(e.into());
            }
        };

        let mut metadata_reader = ParquetMetaDataReader::new().with_page_indexes(true);

        match metadata_reader.try_parse(&file) {
            Ok(()) => {
                let metadata = metadata_reader.finish().unwrap();
                println!("Successfully read metadata with page indexes");
                println!("  Number of row groups: {}", metadata.num_row_groups());
                println!(
                    "  Number of columns: {}",
                    metadata.file_metadata().schema().get_fields().len()
                );
                println!("  Has column index: {}", metadata.column_index().is_some());
                println!("  Has offset index: {}", metadata.offset_index().is_some());
            }
            Err(e) => eprintln!("Error reading metadata: {e}"),
        }
    }

    Ok(())
}