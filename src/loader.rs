//! Parquet file loading utilities

use anyhow::Context;
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute::filter as filter_batch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::Result;
use crate::error::util::{safe_open_file, validate_directory};
use crate::filter_expression::{Expr, read_and_filter_parquet};

/// Read a parquet file into Arrow record batches
pub fn read_parquet(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    // Open the file with proper error handling
    let file = safe_open_file(path, "reading parquet file")?;

    // Create the reader
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("Failed to read parquet file: {}", path.display()))?;

    // Set the projection if schema is provided
    let reader = if let Some(schema) = schema {
        // Convert schema to projection indices, skipping fields that don't exist
        let mut projection = Vec::new();
        let file_schema = reader_builder.schema();

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
            reader_builder
                .build()
                .with_context(|| format!("Failed to build parquet reader for {}", path.display()))?
        } else {
            // Create projection mask and build reader
            let projection_mask =
                ProjectionMask::leaves(reader_builder.parquet_schema(), projection);
            reader_builder
                .with_projection(projection_mask)
                .build()
                .with_context(|| {
                    format!(
                        "Failed to build parquet reader with projection for {}",
                        path.display()
                    )
                })?
        }
    } else {
        // No projection, read all columns
        reader_builder
            .build()
            .with_context(|| format!("Failed to build parquet reader for {}", path.display()))?
    };

    // Read all batches
    let mut batches = Vec::new();

    // If we have a PNR filter, apply it
    if let Some(pnr_filter) = pnr_filter {
        for batch_result in reader {
            let batch = batch_result
                .with_context(|| format!("Failed to read record batch from {}", path.display()))?;

            // Filter the batch by PNR
            let filtered_batch = filter_batch_by_pnr(&batch, pnr_filter)
                .with_context(|| "Failed to filter batch by PNR")?;

            // Add the filtered batch if it's not empty
            if filtered_batch.num_rows() > 0 {
                batches.push(filtered_batch);
            }
        }
    } else {
        // No filter, just read all batches
        for batch_result in reader {
            let batch = batch_result
                .with_context(|| format!("Failed to read record batch from {}", path.display()))?;
            batches.push(batch);
        }
    }

    Ok(batches)
}

/// Filter a record batch by PNR
/// This function only keeps rows where the PNR column value is in the provided set
fn filter_batch_by_pnr(batch: &RecordBatch, pnr_filter: &HashSet<String>) -> Result<RecordBatch> {
    // Find the PNR column index
    let pnr_idx = batch
        .schema()
        .index_of("PNR")
        .with_context(|| "PNR column not found in record batch")?;

    let pnr_array = batch.column(pnr_idx);
    let str_array = pnr_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("PNR column is not a string array"))?;

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
        .with_context(|| "Failed to filter batch columns")?;

    // Create a new record batch with filtered data
    RecordBatch::try_new(batch.schema(), filtered_columns)
        .with_context(|| "Failed to create filtered record batch")
}

/// Reads parquet file with advanced filtering capabilities
///
/// This function applies a filter expression to a parquet file and returns
/// the filtered Arrow record batches with optional column projection.
///
/// # Arguments
/// * `path` - Path to the parquet file
/// * `expr` - Filter expression to apply
/// * `columns` - Columns to include in the result (if None, all columns will be included)
///
/// # Returns
/// A vector of filtered record batches
///
/// # Errors
/// Returns an error if file reading fails or the expression cannot be evaluated
pub fn read_parquet_with_filter(
    path: &Path,
    expr: &Expr,
    columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    // Convert to string path for the filter_expr implementation
    let path_str = path.to_string_lossy();

    // Convert Option<&[String]> to empty Vec for the filter_expr implementation
    let additional_columns = match columns {
        Some(cols) => cols.to_vec(),
        None => Vec::new(),
    };

    // Use the filter_expr implementation to read and filter
    read_and_filter_parquet(&path_str, expr, &additional_columns)
        .with_context(|| format!("Error filtering parquet file: {}", path.display()))
}

/// Load all parquet files from a directory in parallel
pub fn load_parquet_files_parallel(
    dir: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    // Validate the directory exists and is readable
    validate_directory(dir, "loading parquet files in parallel")?;

    // Find all parquet files in the directory
    let mut parquet_files = Vec::<PathBuf>::new();
    for entry_result in
        fs::read_dir(dir).with_context(|| format!("Failed to read directory: {}", dir.display()))?
    {
        let entry = entry_result
            .with_context(|| format!("Failed to read directory entry in {}", dir.display()))?;

        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            parquet_files.push(path);
        }
    }

    // If no files found, return empty result
    if parquet_files.is_empty() {
        log::info!("No parquet files found in directory: {}", dir.display());
        return Ok(Vec::new());
    }

    log::info!(
        "Found {} parquet files in {}",
        parquet_files.len(),
        dir.display()
    );

    // Clone schema and pnr_filter for sharing across threads
    let schema_arc = schema.map(|s| Arc::new(s.clone()));
    let pnr_filter_arc = pnr_filter.map(|f| Arc::new(f.clone()));

    // Process files in parallel using rayon
    let all_batches: Vec<Result<Vec<RecordBatch>>> = parquet_files
        .par_iter()
        .map(|path| {
            // Use clone of schema and pnr_filter
            let schema_ref = schema_arc.as_ref().map(std::convert::AsRef::as_ref);
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);

            read_parquet(path, schema_ref, pnr_filter_ref)
                .with_context(|| format!("Failed to read parquet file: {}", path.display()))
        })
        .collect();

    // Combine all the results, propagating any errors
    let mut combined_batches = Vec::new();
    for (idx, result) in all_batches.into_iter().enumerate() {
        let batches = result.with_context(|| {
            format!(
                "Error processing file {}",
                parquet_files
                    .get(idx)
                    .map_or_else(|| "unknown".to_string(), |p| p.display().to_string())
            )
        })?;
        combined_batches.extend(batches);
    }

    Ok(combined_batches)
}

/// Load all parquet files from a directory in parallel with advanced filtering
///
/// This function processes all parquet files in a directory, applying the
/// same filter expression to each file and combining the results.
///
/// # Arguments
/// * `dir` - Path to the directory containing parquet files
/// * `expr` - Filter expression to apply
/// * `columns` - Columns to include in the result (if None, all columns will be included)
///
/// # Returns
/// A vector of filtered record batches from all matching files
///
/// # Errors
/// Returns an error if directory reading fails or the expression cannot be evaluated
pub fn load_parquet_files_parallel_with_filter(
    dir: &Path,
    expr: &Expr,
    columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    // Validate the directory exists and is readable
    validate_directory(dir, "filtering parquet files in parallel")?;

    // Find all parquet files in the directory
    let mut parquet_files = Vec::<PathBuf>::new();
    for entry_result in
        fs::read_dir(dir).with_context(|| format!("Failed to read directory: {}", dir.display()))?
    {
        let entry = entry_result
            .with_context(|| format!("Failed to read directory entry in {}", dir.display()))?;

        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            parquet_files.push(path);
        }
    }

    // If no files found, return empty result
    if parquet_files.is_empty() {
        log::info!("No parquet files found in directory: {}", dir.display());
        return Ok(Vec::new());
    }

    log::info!(
        "Found {} parquet files in {}, applying filter expression",
        parquet_files.len(),
        dir.display()
    );

    // Process files in parallel using rayon
    let all_batches: Vec<Result<Vec<RecordBatch>>> = parquet_files
        .par_iter()
        .map(|path| {
            // Apply the filter to each file
            read_parquet_with_filter(path, expr, columns)
                .with_context(|| format!("Failed to filter parquet file: {}", path.display()))
        })
        .collect();

    // Combine all the results, propagating any errors
    let mut combined_batches = Vec::new();
    for (idx, result) in all_batches.into_iter().enumerate() {
        let batches = result.with_context(|| {
            format!(
                "Error processing file {}",
                parquet_files
                    .get(idx)
                    .map_or_else(|| "unknown".to_string(), |p| p.display().to_string())
            )
        })?;
        combined_batches.extend(batches);
    }

    Ok(combined_batches)
}
