//! Parquet file operations
//!
//! This module provides utilities for reading and processing Parquet files.
//! It includes functions for finding Parquet files, reading them into Arrow
//! record batches, and filtering data based on various criteria.

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::filter::core::BatchFilter;
use crate::schema::{DateFormatConfig, adapt_record_batch};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use rayon::prelude::*;

use crate::error::Result;
use crate::utils::logging::{log_operation_complete, log_operation_start, log_warning};

/// Default batch size for Parquet reading
pub const DEFAULT_BATCH_SIZE: usize = 16384;

/// Helper function to get batch size from environment
#[must_use]
pub fn get_batch_size() -> Option<usize> {
    std::env::var("PARQUET_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}

/// Validates that a directory exists and is a directory
///
/// # Arguments
/// * `dir` - The directory path to check
///
/// # Returns
/// `Ok(())` if the directory exists, otherwise an error
///
/// # Errors
/// Returns an error if the directory does not exist or is not a directory
pub fn validate_directory(dir: &Path) -> Result<()> {
    if !dir.exists() || !dir.is_dir() {
        return Err(anyhow::anyhow!(
            "Directory does not exist: {}",
            dir.display()
        ));
    }
    Ok(())
}

/// Helper for creating projection mask from schema
///
/// # Arguments
/// * `schema` - The Arrow schema to project
/// * `file_schema` - The Parquet file schema
/// * `parquet_schema` - The Parquet schema descriptor from the builder
///
/// # Returns
/// A tuple with:
/// - Boolean indicating if projection was applied
/// - Optional projection mask if applied
#[must_use]
pub fn create_projection(
    schema: &Schema,
    file_schema: &Schema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> (bool, Option<ProjectionMask>) {
    // Use itertools for more efficient iteration and collection
    let projection: Vec<usize> = schema
        .fields()
        .iter()
        .filter_map(|f| {
            let field_name = f.name();
            file_schema.index_of(field_name).map_or_else(
                |_| {
                    // Skip fields that don't exist in the file
                    log_warning(
                        &format!("Field {field_name} not found in parquet file, skipping"),
                        None,
                    );
                    None
                },
                Some,
            )
        })
        .collect_vec();

    // If no fields matched, just read all columns
    if projection.is_empty() {
        log_warning(
            "No matching fields found in schema projection, reading all columns",
            None,
        );
        (false, None)
    } else {
        // Create optimal projection mask
        let projection_mask = ProjectionMask::leaves(parquet_schema, projection);
        (true, Some(projection_mask))
    }
}

/// Find the PNR column name and index in a record batch
///
/// # Arguments
/// * `batch` - The record batch to search for PNR column
///
/// # Returns
/// A tuple with the PNR column name and index
///
/// # Errors
/// Returns an error if PNR column cannot be found or accessed
pub fn find_pnr_column(batch: &RecordBatch) -> Result<(String, usize)> {
    // Use the centralized implementation from the pnr module
    crate::filter::pnr::PnrFilter::find_pnr_column(batch)
}

/// Filter a record batch by PNR using vectorized operations
/// This function only keeps rows where the PNR column value is in the provided set
///
/// # Errors
/// Returns an error if the PNR column cannot be found or filtered
fn filter_batch_by_pnr<S: ::std::hash::BuildHasher + std::marker::Sync>(
    batch: &RecordBatch,
    pnr_filter: &HashSet<String, S>,
) -> Result<RecordBatch> {
    // Use the centralized PnrFilter from our new filter module
    let pnr_filter_obj = crate::filter::pnr::PnrFilter::new(pnr_filter, None);

    pnr_filter_obj.filter(batch)
}

/// Read a parquet file into Arrow record batches
///
/// # Arguments
/// * `path` - Path to the Parquet file
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `pnr_filter` - Optional set of PNRs to filter the data by
///
/// # Returns
/// A vector of `RecordBatch` objects
///
/// # Errors
/// Returns an error if the file cannot be opened or if the Parquet file is invalid
///
/// # Panics
/// Panics if the projection mask is Some but is attempted to be unwrapped as None
pub fn read_parquet<S: std::hash::BuildHasher + std::marker::Sync>(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String, S>>,
    adapt_types: Option<bool>,
    date_format_config: Option<&crate::schema::DateFormatConfig>,
) -> Result<Vec<RecordBatch>> {
    let start = std::time::Instant::now();
    log_operation_start("Reading parquet file", path);
    // Open the file
    let file = File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", path.display(), e))?;

    // Create the reader
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("Failed to read parquet file {}", e))?;

    // Create the reader with optional projection
    let reader = if let Some(schema) = schema {
        // Apply schema projection
        let file_schema = reader_builder.schema();
        let (has_projection, projection_mask) =
            create_projection(schema, file_schema, reader_builder.parquet_schema());

        if has_projection {
            // Build with projection
            reader_builder
                .with_projection(projection_mask.unwrap())
                .build()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to build parquet reader with projection. Error: {}",
                        e
                    )
                })?
        } else {
            // Build without projection (all columns)
            reader_builder
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to build parquet reader. Error: {}", e))?
        }
    } else {
        // No projection, read all columns
        reader_builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build parquet reader. Error: {}", e))?
    };

    // Collect the batches first to enable parallel processing
    let batch_results: Vec<_> = reader.collect();

    let binding = DateFormatConfig::default();
    // Use default date format config if none provided
    let date_config = date_format_config.unwrap_or(&binding);

    // Determine if type adaptation is enabled
    let should_adapt_types = adapt_types.unwrap_or(false);

    // Process the batches in parallel using rayon
    let batches = if let Some(pnr_filter) = pnr_filter {
        // Create a thread-safe collector for filtered batches
        let batches_collector = Mutex::new(Vec::with_capacity(batch_results.len()));

        // Process batches in parallel
        batch_results.par_iter().for_each(|batch_result| {
            // Handle each batch independently
            if let Ok(batch) = batch_result
                .as_ref()
                .map_err(|e| anyhow::anyhow!("Failed to read record batch. Error: {}", e))
            {
                // Filter the batch by PNR
                if let Ok(filtered_batch) = filter_batch_by_pnr(batch, pnr_filter) {
                    // Skip empty batches
                    if filtered_batch.num_rows() == 0 {
                        return;
                    }

                    // Apply type adaptation if enabled and schema is provided
                    if should_adapt_types && schema.is_some() {
                        match adapt_record_batch(&filtered_batch, schema.unwrap(), date_config) {
                            Ok(adapted_batch) => {
                                let mut batches = batches_collector.lock().unwrap();
                                batches.push(adapted_batch);
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to adapt record batch: {e}. Using original batch."
                                );
                                let mut batches = batches_collector.lock().unwrap();
                                batches.push(filtered_batch);
                            }
                        }
                    } else {
                        // No adaptation needed, use filtered batch as is
                        let mut batches = batches_collector.lock().unwrap();
                        batches.push(filtered_batch);
                    }
                }
            }
        });

        // Get the collected batches
        batches_collector.into_inner().unwrap()
    } else {
        // No PNR filter, process all batches
        if should_adapt_types && schema.is_some() {
            // With type adaptation
            let batches_collector = Mutex::new(Vec::with_capacity(batch_results.len()));

            batch_results.par_iter().for_each(|batch_result| {
                if let Ok(batch) = batch_result
                    .as_ref()
                    .map_err(|e| anyhow::anyhow!("Failed to read record batch. Error: {}", e))
                {
                    match adapt_record_batch(batch, schema.unwrap(), date_config) {
                        Ok(adapted_batch) => {
                            let mut batches = batches_collector.lock().unwrap();
                            batches.push(adapted_batch);
                        }
                        Err(e) => {
                            log::warn!("Failed to adapt record batch: {e}. Using original batch.");
                            let mut batches = batches_collector.lock().unwrap();
                            batches.push(batch.clone());
                        }
                    }
                }
            });

            batches_collector.into_inner().unwrap()
        } else {
            // No type adaptation, just process the batches
            let result: Result<Vec<RecordBatch>> = batch_results
                .into_iter()
                .map(|batch_result| {
                    batch_result
                        .map_err(|e| anyhow::anyhow!("Failed to read record batch. Error: {}", e))
                })
                .collect();

            result?
        }
    };

    log_operation_complete("read", path, batches.len(), Some(start.elapsed()));
    Ok(batches)
}

/// Find all Parquet files in a directory
///
/// # Arguments
/// * `dir` - Path to the directory to search
///
/// # Returns
/// A vector of paths to Parquet files
///
/// # Errors
/// Returns an error if directory reading fails
pub fn find_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    log_operation_start("Searching for parquet files in", dir);

    // Validate directory
    validate_directory(dir)?;

    // Find all parquet files in the directory efficiently using rayon for parallelism
    // and itertools for improved iteration performance
    let parquet_files = std::fs::read_dir(dir)
        .map_err(|e| anyhow::anyhow!("Failed to read directory {}: {}", dir.display(), e))?
        .par_bridge() // Convert to parallel iterator
        .filter_map(|entry_result| match entry_result {
            Ok(entry) => {
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
                    Some(Ok(path))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(anyhow::anyhow!("Failed to read directory entry: {e}"))),
        })
        .collect::<Result<Vec<_>>>()? // Collect errors during processing
        .into_iter()
        .sorted_by(|a, b| {
            // Sort by modification time (newest first) for better caching behavior
            std::fs::metadata(b)
                .and_then(|m| m.modified())
                .ok()
                .cmp(&std::fs::metadata(a).and_then(|m| m.modified()).ok())
        })
        .collect_vec();

    // If no files found, log a warning
    if parquet_files.is_empty() {
        log_warning("No Parquet files found in directory", Some(dir));
    } else {
        log_operation_complete("found", dir, parquet_files.len(), None);
    }

    Ok(parquet_files)
}

/// Load all parquet files from a directory in parallel
///
/// # Arguments
/// * `dir` - Path to the directory containing Parquet files
/// * `schema` - Optional Arrow Schema for projecting specific columns
/// * `pnr_filter` - Optional set of PNRs to filter the data by
///
/// # Returns
/// A vector of record batches from all files
///
/// # Errors
/// Returns an error if directory reading fails or any file cannot be read
pub fn load_parquet_files_parallel<S: ::std::hash::BuildHasher + std::marker::Sync>(
    dir: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String, S>>,
    adapt_types: Option<bool>,
    date_format_config: Option<&DateFormatConfig>,
) -> Result<Vec<RecordBatch>> {
    // Find all parquet files in the directory
    let parquet_files = find_parquet_files(dir)?;

    // If no files found, return empty result
    if parquet_files.is_empty() {
        return Ok(Vec::new());
    }

    // Clone schema and pnr_filter for sharing across threads
    let schema_arc = schema.map(|s| std::sync::Arc::new(s.clone()));
    let pnr_filter_arc = pnr_filter.map(std::sync::Arc::new);
    let date_format_config_arc = date_format_config.map(|c| std::sync::Arc::new(c.clone()));

    // Process files in parallel using rayon
    let all_batches: Vec<Result<Vec<RecordBatch>>> = parquet_files
        .par_iter()
        .map(|path| {
            // Use clone of schema and pnr_filter
            let schema_ref = schema_arc.as_ref().map(std::convert::AsRef::as_ref);
            let pnr_filter_ref = pnr_filter_arc.as_deref();
            let date_config_ref = date_format_config_arc.as_deref();

            read_parquet::<S>(
                path,
                schema_ref,
                pnr_filter_ref.map(|v| &**v),
                adapt_types,
                date_config_ref,
            )
        })
        .collect();

    // Combine all the results, propagating any errors
    let mut combined_batches = Vec::new();
    for result in all_batches {
        let batches = result?;
        combined_batches.extend(batches);
    }

    log::info!(
        "Successfully loaded {} batches from {} Parquet files",
        combined_batches.len(),
        parquet_files.len()
    );

    Ok(combined_batches)
}