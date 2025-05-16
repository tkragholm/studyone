//! Async Parquet filtering operations
//!
//! This module provides asynchronous filtering operations for Parquet files.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use crate::async_io::batch_ops::read_parquet_async;
use crate::async_io::file_ops::open_parquet_file_async;
use crate::error::Result;
use crate::filter::core::BatchFilter;
use crate::filter::expr::Expr;
use crate::filter::pnr::PnrFilter;
use crate::utils::{DEFAULT_BATCH_SIZE, get_batch_size};

/// Read a Parquet file asynchronously with PNR filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `pnr_filter` - Set of PNRs to filter the data by
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of filtered `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading or filtering fails
pub async fn read_parquet_with_pnr_filter_async<S: ::std::hash::BuildHasher + Sync>(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: &HashSet<String, S>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // First read the file
    let batches = read_parquet_async(path, schema, batch_size).await?;

    // Create a PNR filter
    let filter = Arc::new(PnrFilter::new(pnr_filter, None));

    // Apply the filter to each batch
    let mut filtered_batches = Vec::new();

    for batch in batches {
        let filtered_batch = filter.filter(&batch)?;

        // Only add non-empty batches
        if filtered_batch.num_rows() > 0 {
            filtered_batches.push(filtered_batch);
        }
    }

    Ok(filtered_batches)
}

/// Read a Parquet file asynchronously with filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `filter` - Filter to apply
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of filtered `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading or filtering fails
pub async fn read_parquet_with_filter_async(
    path: &Path,
    filter: Arc<dyn BatchFilter + Send + Sync>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    log::info!(
        "Reading and filtering parquet file asynchronously: {}",
        path.display()
    );

    // Open file asynchronously
    let file = open_parquet_file_async(path).await?;

    // Create the builder
    let mut builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create parquet reader. Error: {}", e))?;

    // Get columns required by the filter
    let required_columns = filter.required_columns();

    // Create the projection from required columns if any
    if !required_columns.is_empty() {
        // Convert set to Schema for our projection helper
        let fields = required_columns
            .iter()
            .map(|name| arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true))
            .collect::<Vec<_>>();
        let projected_schema = arrow::datatypes::Schema::new(fields);

        // Use our common projection helper
        let file_schema = builder.schema();
        let (has_projection, projection_mask) = crate::utils::io::parquet::create_projection(
            &projected_schema,
            file_schema,
            builder.parquet_schema(),
        );

        if has_projection {
            builder = builder.with_projection(projection_mask.unwrap());
        }
    }

    // Set batch size
    let batch_size = batch_size
        .or_else(get_batch_size)
        .unwrap_or(DEFAULT_BATCH_SIZE);

    builder = builder.with_batch_size(batch_size);

    // Build the stream
    let stream = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build parquet stream. Error: {}", e))?;

    // Process the stream with filtering
    let mut results = Vec::new();

    tokio::pin!(stream);

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|e| anyhow::anyhow!("Failed to read record batch. Error: {}", e))?;

        // Apply the filter
        let filtered = filter.filter(&batch)?;

        // Only add non-empty batches
        if filtered.num_rows() > 0 {
            results.push(filtered);
        }
    }

    log::info!(
        "Successfully read and filtered {} batches from {}",
        results.len(),
        path.display()
    );

    Ok(results)
}

/// Read a Parquet file asynchronously with expression-based filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `expr` - Filter expression to apply
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of filtered `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading or filtering fails
pub async fn read_parquet_with_expr_async(
    path: &Path,
    expr: &Expr,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // Create a filter from the expression
    let filter = Arc::new(crate::filter::expr::ExpressionFilter::new(expr.clone()));

    // Use the common function
    read_parquet_with_filter_async(path, filter, batch_size).await
}

/// Read a single Parquet file asynchronously with optional PNR filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `pnr_filter` - Optional set of PNRs to filter the data by
///
/// # Returns
/// A vector of filtered `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading or filtering fails
pub async fn read_parquet_with_optional_pnr_filter_async<S: ::std::hash::BuildHasher + Sync>(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String, S>>,
) -> Result<Vec<RecordBatch>> {
    match pnr_filter {
        Some(filter) => read_parquet_with_pnr_filter_async(path, schema, filter, None).await,
        None => read_parquet_async(path, schema, None).await,
    }
}
