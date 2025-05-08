use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use par_reader::{
    ParquetReaderConfig, Result,
    filter::core::BatchFilter,
    filter::expr::{Expr, ExpressionFilter},
};

//pub mod families;
//pub mod individuals;

/// Base path for test data files
#[must_use]
pub fn test_data_dir() -> PathBuf {
    match std::env::consts::OS {
        "macos" => PathBuf::from("/Users/tobiaskragholm/generated_data/parquet"),
        "linux" => PathBuf::from("/home/tkragholm/generated_data/parquet"),
        //"windows" => PathBuf::from("C:\\Users\\yourusername\\generated_data\\parquet"),
        _ => panic!("Unsupported operating system"),
    }
}

/// Create a path to a specific registry folder
#[must_use]
pub fn registry_dir(registry: &str) -> PathBuf {
    test_data_dir().join(registry)
}

/// Create a path to a specific file in a registry folder
#[must_use]
pub fn registry_file(registry: &str, filename: &str) -> PathBuf {
    registry_dir(registry).join(filename)
}

/// Ensure the given path exists
pub fn ensure_path_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow::anyhow!("Test file not found: {}", path.display()));
    }
    Ok(())
}

/// Get default test configuration for parquet reading
#[must_use]
pub fn test_config() -> ParquetReaderConfig {
    ParquetReaderConfig {
        read_page_indexes: true,
        validate_schema: true,
        fail_on_schema_incompatibility: false,
        ..Default::default()
    }
}

/// Print summary information about record batches
pub fn print_batch_summary(batches: &[RecordBatch], elapsed: std::time::Duration) {
    println!("Read {} record batches in {:?}", batches.len(), elapsed);
    println!(
        "Total rows: {}",
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
}

/// Print detailed schema information from the first batch
pub fn print_schema_info(batch: &RecordBatch) {
    println!("Schema:");
    for field in batch.schema().fields() {
        println!("  - {} ({})", field.name(), field.data_type());
    }
}

/// Print sample rows from a batch
pub fn print_sample_rows(batch: &RecordBatch, num_rows: usize) {
    println!("First {num_rows} rows:");
    for row_idx in 0..std::cmp::min(num_rows, batch.num_rows()) {
        print!("Row {row_idx}: [");
        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            print!("{}: ", batch.schema().field(col_idx).name());

            if column.is_null(row_idx) {
                print!("NULL");
            } else {
                print!("Value"); // Simplified - actual value display would depend on column type
            }

            if col_idx < batch.num_columns() - 1 {
                print!(", ");
            }
        }
        println!("]");
    }
}

/// Timed execution of a function that returns a Result<Vec<RecordBatch>>
pub fn timed_execution<F>(func: F) -> (std::time::Duration, Result<Vec<RecordBatch>>)
where
    F: FnOnce() -> Result<Vec<RecordBatch>>,
{
    let start = Instant::now();
    let result = func();
    let elapsed = start.elapsed();
    (elapsed, result)
}

/// Get all available year files from a registry directory
#[must_use]
pub fn get_available_year_files(registry: &str) -> Vec<PathBuf> {
    let dir = registry_dir(registry);
    if !dir.exists() {
        return Vec::new();
    }

    std::fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|res: std::io::Result<std::fs::DirEntry>| res.ok())
                .filter(|entry| {
                    let path = entry.path();
                    path.is_file()
                        && path.extension().is_some_and(|ext| ext == "parquet")
                        && path
                            .file_stem()
                            .is_some_and(|name| name.to_string_lossy().parse::<u32>().is_ok())
                })
                .map(|entry| entry.path())
                .collect()
        })
        .unwrap_or_default()
}

/// Convert an expression to a filter
///
/// Helper function to convert an Expr to an Arc<dyn `BatchFilter` + Send + Sync>
/// for use with the `read_parquet_with_filter_async` function.
#[must_use]
pub fn expr_to_filter(expr: &Expr) -> Arc<dyn BatchFilter + Send + Sync> {
    Arc::new(ExpressionFilter::new(expr.clone()))
}
