//! Async parallel operations for Parquet files
//! Provides functionality for processing multiple Parquet files in parallel

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures::stream::{self, StreamExt};
use itertools::Itertools;

use crate::error::Result;
use crate::filter::Expr;
use super::batch_ops::read_parquet_async;
use super::file_ops::find_parquet_files_async;
use super::filter_ops::read_parquet_with_filter_async;

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