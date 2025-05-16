use crate::Expr;
use crate::ParquetReaderConfig;
use crate::Result;
use crate::filter::ExpressionFilter;
use crate::utils::BatchFilter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;

/// Base path for test data files
#[must_use]
pub fn data_dir() -> PathBuf {
    match std::env::consts::OS {
        "macos" => PathBuf::from("/Users/tobiaskragholm/generated_data/parquet"),
        "linux" => PathBuf::from("/home/tkragholm/generated_data/parquet"),
        "windows" => PathBuf::from("E:\\workdata\\708245\\generated_data\\parquet"),
        _ => panic!("Unsupported operating system"),
    }
}

/// Create a path to a specific registry folder
#[must_use]
pub fn registry_dir(registry: &str) -> PathBuf {
    data_dir().join(registry)
}

/// Create a path to a specific file in a registry folder
#[must_use]
pub fn registry_file(registry: &str, filename: &str) -> PathBuf {
    registry_dir(registry).join(filename)
}

/// Ensure the given path exists
pub fn ensure_path_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {}", path.display()));
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

/// Convert an expression to a filter
///
/// Helper function to convert an Expr to an Arc<dyn `BatchFilter` + Send + Sync>
/// for use with the `read_parquet_with_filter_async` function.
#[must_use]
pub fn expr_to_filter(expr: &Expr) -> Arc<dyn BatchFilter + Send + Sync> {
    Arc::new(ExpressionFilter::new(expr.clone()))
}
