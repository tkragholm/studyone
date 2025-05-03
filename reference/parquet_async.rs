//! Async Parquet file loading utilities
//! Provides optimized asynchronous reading of Parquet files using Arrow

use crate::error::{IdsError, Result};
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute::filter as filter_batch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::{async_reader::ParquetRecordBatchStreamBuilder, ProjectionMask};
//use rayon::prelude::*;
use std::collections::HashSet;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};

use crate::schema::filter_expr::{evaluate_expr, filter_record_batch, Expr};

/// Default batch size for Parquet reading
pub const DEFAULT_BATCH_SIZE: usize = 16384;

/// Helper function to get batch size from environment
#[must_use] pub fn get_batch_size() -> Option<usize> {
    std::env::var("IDS_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}

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
pub async fn read_parquet_async(
    path: &Path,
    schema: Option<&Schema>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    log::info!("Reading parquet file asynchronously: {}", path.display());

    // Open file asynchronously
    let file = File::open(path).await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("Failed to open file {}: {}", path.display(), e),
        ))
    })?;

    // Create the builder
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| IdsError::Data(format!("Failed to create parquet reader: {e}")))?;

    // Apply projection if schema is provided
    if let Some(schema) = schema {
        // Convert schema to projection indices, skipping fields that don't exist
        let mut projection = Vec::new();
        let file_schema = builder.schema();

        for f in schema.fields() {
            let field_name = f.name();
            match file_schema.index_of(field_name) {
                Ok(idx) => projection.push(idx),
                Err(_) => {
                    // Skip fields that don't exist in the file
                    log::warn!("Field {field_name} not found in parquet file, skipping");
                }
            }
        }

        // If no fields matched, just read all columns
        if projection.is_empty() {
            log::warn!("No matching fields found in schema projection, reading all columns");
        } else {
            // Create projection mask and apply to builder
            let projection_mask = ProjectionMask::leaves(builder.parquet_schema(), projection);
            builder = builder.with_projection(projection_mask);
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
        .map_err(|e| IdsError::Data(format!("Failed to build parquet stream: {e}")))?;

    // Collect results
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| IdsError::Data(format!("Failed to read record batches: {e}")))?;

    log::info!(
        "Successfully read {} batches from {}",
        batches.len(),
        path.display()
    );

    Ok(batches)
}

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
pub async fn read_parquet_with_pnr_filter_async(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: &HashSet<String>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    // First read the file
    let batches = read_parquet_async(path, schema, batch_size).await?;

    // Then filter by PNR
    let mut filtered_batches = Vec::new();

    for batch in batches {
        // Find the PNR column
        let pnr_col_name = match batch.schema().field_with_name("PNR") {
            Ok(_) => "PNR",
            Err(_) => match batch.schema().field_with_name("pnr") {
                Ok(_) => "pnr",
                Err(_) => {
                    return Err(IdsError::Data(
                        "PNR column not found in record batch".to_string(),
                    ))
                }
            },
        };

        let pnr_idx = batch
            .schema()
            .index_of(pnr_col_name)
            .map_err(|e| IdsError::Data(format!("PNR column not found in record batch: {e}")))?;

        let pnr_array = batch.column(pnr_idx);
        let str_array = pnr_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IdsError::Data("PNR column is not a string array".to_string()))?;

        // Create a boolean array indicating which rows match our filter
        let mut values = Vec::with_capacity(str_array.len());
        for i in 0..str_array.len() {
            if str_array.is_null(i) {
                values.push(false);
            } else {
                values.push(pnr_filter.contains(str_array.value(i)));
            }
        }
        let filter_mask = BooleanArray::from(values);

        // Apply the filter to all columns
        let filtered_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| filter_batch(col, &filter_mask))
            .collect::<arrow::error::Result<_>>()
            .map_err(|e| IdsError::Data(format!("Failed to filter batch: {e}")))?;

        // Create a new record batch with filtered data
        let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_columns)
            .map_err(|e| IdsError::Data(format!("Failed to create filtered batch: {e}")))?;

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
    let file = File::open(path).await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("Failed to open file {}: {}", path.display(), e),
        ))
    })?;

    // Create the builder
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| IdsError::Data(format!("Failed to create parquet reader: {e}")))?;

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
        let file_schema = builder.schema();
        let mut projection = Vec::new();

        for col_name in all_required_columns {
            match file_schema.index_of(&col_name) {
                Ok(idx) => projection.push(idx),
                Err(_) => {
                    log::warn!("Column {col_name} not found in parquet file, skipping");
                }
            }
        }

        if !projection.is_empty() {
            let projection_mask = ProjectionMask::leaves(builder.parquet_schema(), projection);
            builder = builder.with_projection(projection_mask);
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
        .map_err(|e| IdsError::Data(format!("Failed to build parquet stream: {e}")))?;

    // Process the stream with filtering
    let mut results = Vec::new();

    tokio::pin!(stream);

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result
            .map_err(|e| IdsError::Data(format!("Failed to read record batch: {e}")))?;

        // Apply the filter expression
        let mask = evaluate_expr(&batch, expr)
            .map_err(|e| IdsError::Data(format!("Failed to evaluate filter: {e}")))?;

        // Filter the batch using the mask
        let filtered = filter_record_batch(&batch, &mask)
            .map_err(|e| IdsError::Data(format!("Failed to filter batch: {e}")))?;

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

/// Load Parquet files from a directory in parallel using async IO
///
/// # Arguments
/// * `dir` - Directory containing Parquet files
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of `RecordBatch` objects from all files
pub async fn load_parquet_files_parallel_async(
    dir: &Path,
    schema: Option<&Schema>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    log::info!(
        "Loading Parquet files from directory asynchronously: {}",
        dir.display()
    );

    // Check if the directory exists
    if !dir.exists() || !dir.is_dir() {
        return Err(IdsError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("Directory does not exist: {}", dir.display()),
        )));
    }

    // Find all parquet files in the directory
    let mut parquet_files = Vec::<PathBuf>::new();

    let mut entries = fs::read_dir(dir).await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::PermissionDenied,
            format!("Failed to read directory {}: {}", dir.display(), e),
        ))
    })?;

    while let Some(entry_result) = entries.next_entry().await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::Other,
            format!("Failed to read directory entry: {e}"),
        ))
    })? {
        let path = entry_result.path();
        let metadata = fs::metadata(&path).await.map_err(|e| {
            IdsError::Io(io::Error::new(
                ErrorKind::Other,
                format!("Failed to read metadata for {}: {}", path.display(), e),
            ))
        })?;

        if metadata.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            parquet_files.push(path);
        }
    }

    // If no files found, return empty result
    if parquet_files.is_empty() {
        log::warn!("No Parquet files found in directory: {}", dir.display());
        return Ok(Vec::new());
    }

    log::info!("Found {} Parquet files in directory", parquet_files.len());

    // Process each file and collect results
    let schema_arc = schema.map(|s| Arc::new(s.clone()));

    // Create futures for each file
    let futures = parquet_files.iter().map(|path| {
        let path = path.clone();
        let schema_clone = schema_arc.clone();

        async move { read_parquet_async(&path, schema_clone.as_deref(), batch_size).await }
    });

    // Run all futures and combine results
    let results = futures::future::join_all(futures).await;

    // Combine all the batches
    let mut combined_batches = Vec::new();
    for result in results {
        match result {
            Ok(batches) => combined_batches.extend(batches),
            Err(e) => {
                log::error!("Error loading parquet file: {e}");
                return Err(e);
            }
        }
    }

    log::info!(
        "Successfully loaded {} batches from {} Parquet files",
        combined_batches.len(),
        parquet_files.len()
    );

    Ok(combined_batches)
}

/// Load Parquet files from a directory in parallel with filtering
///
/// # Arguments
/// * `dir` - Directory containing Parquet files
/// * `expr` - Filter expression to apply
/// * `columns` - Optional columns to include in the result
/// * `batch_size` - Optional batch size for reading
pub async fn load_parquet_files_parallel_with_filter_async(
    dir: &Path,
    expr: &Expr,
    columns: Option<&[String]>,
    batch_size: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    log::info!(
        "Loading and filtering Parquet files from directory asynchronously: {}",
        dir.display()
    );

    // Check if the directory exists
    if !dir.exists() || !dir.is_dir() {
        return Err(IdsError::Io(io::Error::new(
            ErrorKind::NotFound,
            format!("Directory does not exist: {}", dir.display()),
        )));
    }

    // Find all parquet files in the directory (same code as in load_parquet_files_parallel_async)
    let mut parquet_files = Vec::<PathBuf>::new();

    let mut entries = fs::read_dir(dir).await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::PermissionDenied,
            format!("Failed to read directory {}: {}", dir.display(), e),
        ))
    })?;

    while let Some(entry_result) = entries.next_entry().await.map_err(|e| {
        IdsError::Io(io::Error::new(
            ErrorKind::Other,
            format!("Failed to read directory entry: {e}"),
        ))
    })? {
        let path = entry_result.path();
        let metadata = fs::metadata(&path).await.map_err(|e| {
            IdsError::Io(io::Error::new(
                ErrorKind::Other,
                format!("Failed to read metadata for {}: {}", path.display(), e),
            ))
        })?;

        if metadata.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            parquet_files.push(path);
        }
    }

    // If no files found, return empty result
    if parquet_files.is_empty() {
        log::warn!("No Parquet files found in directory: {}", dir.display());
        return Ok(Vec::new());
    }

    log::info!("Found {} Parquet files in directory", parquet_files.len());

    // Create futures for each file with filtering
    let column_vec = columns.map(<[std::string::String]>::to_vec);

    let futures = parquet_files.iter().map(|path| {
        let path = path.clone();
        let expr = expr.clone();
        let cols_clone = column_vec.clone();

        async move {
            read_parquet_with_filter_async(&path, &expr, cols_clone.as_deref(), batch_size).await
        }
    });

    // Run all futures and combine results
    let results = futures::future::join_all(futures).await;

    // Combine all the batches
    let mut combined_batches = Vec::new();
    for result in results {
        match result {
            Ok(batches) => combined_batches.extend(batches),
            Err(e) => {
                log::error!("Error loading and filtering parquet file: {e}");
                return Err(e);
            }
        }
    }

    log::info!(
        "Successfully loaded and filtered {} batches from {} Parquet files",
        combined_batches.len(),
        parquet_files.len()
    );

    Ok(combined_batches)
}
