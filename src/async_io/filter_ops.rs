//! Async Parquet filtering operations
//! Provides functionality for filtering Parquet data during async reading

use std::collections::HashSet;
use std::path::Path;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use super::batch_ops::read_parquet_async;
use super::file_ops::open_parquet_file_async;
use crate::error::{ParquetReaderError, Result};
use crate::filter::{Expr, evaluate_expr, filter_record_batch};
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
    schema: Option<&arrow::datatypes::Schema>,
    pnr_filter: &HashSet<String, S>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // First read the file
    let batches = read_parquet_async(path, schema, batch_size).await?;

    // Then filter by PNR
    let mut filtered_batches = Vec::new();

    for batch in batches {
        // Find the PNR column
        let (_, pnr_idx) = crate::utils::find_pnr_column(&batch)?;

        let pnr_array = batch.column(pnr_idx);
        let str_array = pnr_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                ParquetReaderError::MetadataError("PNR column is not a string array".to_string())
            })?;

        // Create a boolean array indicating which rows match our filter
        let mut values = Vec::with_capacity(str_array.len());
        for i in 0..str_array.len() {
            if str_array.is_null(i) {
                values.push(false);
            } else {
                values.push(pnr_filter.contains(str_array.value(i)));
            }
        }
        let filter_mask = arrow::array::BooleanArray::from(values);

        // Use the common filter function
        let filtered_batch = crate::filter::filter_record_batch(&batch, &filter_mask)?;

        // Only add non-empty batches
        if filtered_batch.num_rows() > 0 {
            filtered_batches.push(filtered_batch);
        }
    }

    Ok(filtered_batches)
}

/// Read a Parquet file asynchronously with advanced filtering
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `expr` - Filter expression to apply
/// * `columns` - Optional columns to include in the result
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of filtered `RecordBatch` objects
///
/// # Errors
/// Returns an error if file reading or filtering fails
///
/// # Panics
/// Panics if the projection mask is Some but is attempted to be unwrapped as None
pub async fn read_parquet_with_filter_async(
    path: &Path,
    expr: &Expr,
    columns: Option<&[String]>,
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

    // Get all columns required by the filter expression
    let mut all_required_columns = expr.required_columns();

    // Add any additional columns requested for projection
    if let Some(cols) = columns {
        for col in cols {
            all_required_columns.insert(col.clone());
        }
    }

    // Create the projection from required columns
    if !all_required_columns.is_empty() {
        // Convert set to Schema for our projection helper
        let fields = all_required_columns
            .iter()
            .map(|name| arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true))
            .collect::<Vec<_>>();
        let projected_schema = arrow::datatypes::Schema::new(fields);

        // Use our common projection helper
        let file_schema = builder.schema();
        let (has_projection, projection_mask) = crate::utils::create_projection(
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

        // Apply the filter expression
        let mask = evaluate_expr(&batch, expr)?;

        // Filter the batch using the mask
        let filtered = filter_record_batch(&batch, &mask)?;

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
    schema: Option<&arrow::datatypes::Schema>,
    pnr_filter: Option<&HashSet<String, S>>,
) -> Result<Vec<RecordBatch>> {
    match pnr_filter {
        Some(filter) => read_parquet_with_pnr_filter_async(path, schema, filter, None).await,
        None => read_parquet_async(path, schema, None).await,
    }
}
