//! Async Parquet batch reading operations
//! Provides functionality for reading Parquet files as Arrow RecordBatches

use std::path::Path;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use super::file_ops::open_parquet_file_async;
use crate::error::Result;
use crate::utils::{
    DEFAULT_BATCH_SIZE, get_batch_size, log_operation_complete, log_operation_start,
};

/// Read a Parquet file asynchronously into Arrow record batches
///
/// This function opens a Parquet file asynchronously and streams its
/// contents without loading the entire file into memory at once.
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `batch_size` - Optional batch size for reading (defaults to `DEFAULT_BATCH_SIZE`)
///
/// # Returns
/// A vector of `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading fails
pub async fn read_parquet_async(
    path: &Path,
    schema: Option<&Schema>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let start = std::time::Instant::now();
    log_operation_start("Reading parquet file asynchronously", path);

    // Open file asynchronously
    let file = open_parquet_file_async(path).await?;

    // Create the builder
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create parquet reader. {}", e))?;

    // Apply projection if schema is provided
    if let Some(schema) = schema {
        // Use the common projection helper
        let file_schema = builder.schema();
        let (has_projection, projection_mask) =
            crate::utils::create_projection(schema, file_schema, builder.parquet_schema());

        if has_projection {
            builder = builder.with_projection(projection_mask.unwrap());
        }
    }

    // Set batch size - use provided, then env var, then default
    let batch_size = batch_size
        .or_else(get_batch_size)
        .unwrap_or(DEFAULT_BATCH_SIZE);

    builder = builder.with_batch_size(batch_size);

    // Build the stream
    let stream = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build parquet stream {}", e))?;

    // Collect results
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read record batches {}", e))?;

    log_operation_complete("read", path, batches.len(), Some(start.elapsed()));

    Ok(batches)
}
