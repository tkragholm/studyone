//! Test helper functions
//!
//! This module provides utilities for testing and benchmarking.

use crate::Expr;
use crate::ParquetReaderConfig;
use crate::Result;
use crate::filter::ExpressionFilter;
use crate::filter::core::BatchFilter;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;

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