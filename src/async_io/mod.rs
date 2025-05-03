//! Async Parquet file loading utilities
//! Provides optimized asynchronous reading of Parquet files using Arrow

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio::fs::{self, File};

use crate::error::{ParquetReaderError, Result};
use crate::filter::{Expr, evaluate_expr, filter_record_batch};
use crate::utils::{
    DEFAULT_BATCH_SIZE, create_parquet_error, get_batch_size, log_operation_complete,
    log_operation_start, log_warning, validate_directory,
};

/// Find all Parquet files in a directory asynchronously
///
/// # Arguments
/// * `dir` - Path to the directory to search
///
/// # Returns
/// A vector of paths to Parquet files
///
/// # Errors
/// Returns an error if directory reading fails
pub async fn find_parquet_files_async(dir: &Path) -> Result<Vec<PathBuf>> {
    log_operation_start("Searching for parquet files asynchronously in", dir);

    // Validate directory
    validate_directory(dir)?;

    // Find all parquet files in the directory
    let mut parquet_files = Vec::<PathBuf>::new();

    let mut entries = fs::read_dir(dir).await.map_err(|e| {
        ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Failed to read directory {}: {}", dir.display(), e),
        ))
    })?;

    while let Some(entry_result) = entries.next_entry().await.map_err(|e| {
        ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to read directory entry: {e}"),
        ))
    })? {
        let path = entry_result.path();
        let metadata = fs::metadata(&path).await.map_err(|e| {
            ParquetReaderError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read metadata for {}: {}", path.display(), e),
            ))
        })?;

        if metadata.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            parquet_files.push(path);
        }
    }

    // If no files found, log a warning
    if parquet_files.is_empty() {
        log_warning("No Parquet files found in directory", Some(dir));
    } else {
        log_operation_complete("found", dir, parquet_files.len(), None);
    }

    Ok(parquet_files)
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
    let file = File::open(path).await.map_err(|e| {
        ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to open file {}: {}", path.display(), e),
        ))
    })?;

    // Create the builder
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| create_parquet_error("Failed to create parquet reader", e))?;

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
        .map_err(|e| create_parquet_error("Failed to build parquet stream", e))?;

    // Collect results
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| create_parquet_error("Failed to read record batches", e))?;

    log_operation_complete("read", path, batches.len(), Some(start.elapsed()));

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
///
/// # Errors
/// Returns an error if file reading or filtering fails
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
        ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to open file {}: {}", path.display(), e),
        ))
    })?;

    // Create the builder
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|e| create_parquet_error("Failed to create parquet reader", e))?;

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
        let projected_schema = Schema::new(fields);

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
        .map_err(|e| create_parquet_error("Failed to build parquet stream", e))?;

    // Process the stream with filtering
    let mut results = Vec::new();

    tokio::pin!(stream);

    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| create_parquet_error("Failed to read record batch", e))?;

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

/// Load Parquet files from a directory in parallel using async IO
///
/// # Arguments
/// * `dir` - Directory containing Parquet files
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `batch_size` - Optional batch size for reading
///
/// # Returns
/// A vector of `RecordBatch` objects from all files
///
/// # Errors
/// Returns an error if directory reading or file reading fails
pub async fn load_parquet_files_parallel_async(
    dir: &Path,
    schema: Option<&Schema>,
    _batch_size: Option<usize>, // Unused but kept for API compatibility
) -> Result<Vec<RecordBatch>> {
    log::info!(
        "Loading Parquet files from directory asynchronously: {}",
        dir.display()
    );

    // Find all parquet files in the directory
    let parquet_files = find_parquet_files_async(dir).await?;

    // If no files found, return empty result
    if parquet_files.is_empty() {
        return Ok(Vec::new());
    }

    // Process each file and collect results
    let schema_arc = schema.map(|s| Arc::new(s.clone()));

    use itertools::Itertools;
    use futures::stream::{self, StreamExt};
    
    // Determine optimal parallelism based on CPU count
    let num_cpus = num_cpus::get();
    
    // Process files in optimal batches to avoid creating too many futures at once
    let results = stream::iter(parquet_files.clone()) // Clone to avoid ownership issues
        .map(|path| {
            let schema_clone = schema_arc.clone();
            async move { read_parquet_async(&path, schema_clone.as_deref(), None).await }
        })
        .buffer_unordered(num_cpus) // Process up to num_cpus files concurrently
        .collect::<Vec<_>>()
        .await;
    
    // Combine all the batches efficiently using itertools
    let combined_batches = results
        .into_iter()
        .map(|result| match result {
            Ok(batches) => Ok(batches),
            Err(e) => {
                log::error!("Error loading parquet file: {e}");
                Err(e)
            }
        })
        .collect::<Result<Vec<Vec<RecordBatch>>>>()?
        .into_iter()
        .flatten()
        .collect_vec();

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
///
/// # Returns
/// A vector of filtered `RecordBatch` objects from all files
///
/// # Errors
/// Returns an error if directory reading, file reading, or filtering fails
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

    // Find all parquet files in the directory
    let parquet_files = find_parquet_files_async(dir).await?;

    // If no files found, return empty result
    if parquet_files.is_empty() {
        return Ok(Vec::new());
    }

    // Create futures for each file with filtering
    let column_vec = columns.map(<[std::string::String]>::to_vec);
    let expr_arc = Arc::new(expr.clone());

    let futures = parquet_files.iter().map(|path| {
        let path = path.clone();
        let expr = expr_arc.clone();
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
