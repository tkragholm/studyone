//! Utility functions for working with Parquet files

use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, BooleanArray, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use rayon::prelude::*;

use crate::error::{ParquetReaderError, Result};

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
        return Err(ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Directory does not exist: {}", dir.display()),
        )));
    }
    Ok(())
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
    // Try to find the PNR column with either uppercase or lowercase
    let pnr_col_name = match batch.schema().field_with_name("PNR") {
        Ok(_) => "PNR",
        Err(_) => match batch.schema().field_with_name("pnr") {
            Ok(_) => "pnr",
            Err(_) => {
                return Err(ParquetReaderError::MetadataError(
                    "PNR column not found in record batch".to_string(),
                ));
            }
        },
    };

    let pnr_idx = batch.schema().index_of(pnr_col_name).map_err(|e| {
        ParquetReaderError::MetadataError(format!("PNR column not found in record batch: {e}"))
    })?;

    Ok((pnr_col_name.to_string(), pnr_idx))
}

/// Default batch size for Parquet reading
pub const DEFAULT_BATCH_SIZE: usize = 16384;

/// Helper function to get batch size from environment
#[must_use]
pub fn get_batch_size() -> Option<usize> {
    std::env::var("PARQUET_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
}

/// Creates a standardized error for Parquet operations
///
/// # Arguments
/// * `message` - Base error message
/// * `error` - The original error that occurred
///
/// # Returns
/// A standardized `ParquetReaderError`
pub fn create_parquet_error<E: std::fmt::Display>(message: &str, error: E) -> ParquetReaderError {
    ParquetReaderError::ParquetError(parquet::errors::ParquetError::General(format!(
        "{message}: {error}"
    )))
}

/// Log an operation start with consistent format
///
/// # Arguments
/// * `operation` - Description of the operation
/// * `path` - Path of the file or directory being operated on
pub fn log_operation_start(operation: &str, path: &Path) {
    log::info!("{} {}", operation, path.display());
}

/// Log an operation completion with consistent format
///
/// # Arguments
/// * `operation` - Description of the operation
/// * `path` - Path of the file or directory that was operated on
/// * `items` - Number of items processed
/// * `elapsed` - Optional elapsed time
pub fn log_operation_complete(
    operation: &str,
    path: &Path,
    items: usize,
    elapsed: Option<std::time::Duration>,
) {
    if let Some(duration) = elapsed {
        log::info!(
            "Successfully {} {} items from {} in {:?}",
            operation,
            items,
            path.display(),
            duration
        );
    } else {
        log::info!(
            "Successfully {} {} items from {}",
            operation,
            items,
            path.display()
        );
    }
}

/// Log an operation warning with consistent format
///
/// # Arguments
/// * `message` - Warning message
/// * `path` - Optional path related to the warning
pub fn log_warning(message: &str, path: Option<&Path>) {
    if let Some(path) = path {
        log::warn!("{}: {}", message, path.display());
    } else {
        log::warn!("{message}");
    }
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
#[must_use] pub fn create_projection(
    schema: &Schema,
    file_schema: &Schema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> (bool, Option<ProjectionMask>) {
    use itertools::Itertools;
    
    // Use itertools for more efficient iteration and collection
    let projection: Vec<usize> = schema.fields()
        .iter()
        .filter_map(|f| {
            let field_name = f.name();
            match file_schema.index_of(field_name) {
                Ok(idx) => Some(idx),
                Err(_) => {
                    // Skip fields that don't exist in the file
                    log_warning(
                        &format!("Field {field_name} not found in parquet file, skipping"),
                        None,
                    );
                    None
                }
            }
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
pub fn read_parquet(
    path: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    let start = std::time::Instant::now();
    log_operation_start("Reading parquet file", path);
    // Open the file
    let file = File::open(path).map_err(|e| {
        ParquetReaderError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Failed to open file {}: {}", path.display(), e),
        ))
    })?;

    // Create the reader
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        create_parquet_error(
            &format!("Failed to read parquet file {}", path.display()),
            e,
        )
    })?;

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
                    create_parquet_error("Failed to build parquet reader with projection", e)
                })?
        } else {
            // Build without projection (all columns)
            reader_builder
                .build()
                .map_err(|e| create_parquet_error("Failed to build parquet reader", e))?
        }
    } else {
        // No projection, read all columns
        reader_builder
            .build()
            .map_err(|e| create_parquet_error("Failed to build parquet reader", e))?
    };

    use rayon::prelude::*;
    use std::sync::Mutex;
    
    // Collect the batches first to enable parallel processing
    let batch_results: Vec<_> = reader.collect();
    
    // Process the batches in parallel using rayon
    let batches = if let Some(pnr_filter) = pnr_filter {
        // Create a thread-safe collector for filtered batches
        let batches_collector = Mutex::new(Vec::with_capacity(batch_results.len()));
        
        // Process batches in parallel
        batch_results.par_iter()
            .for_each(|batch_result| {
                // Handle each batch independently
                if let Ok(batch) = batch_result.as_ref().map_err(|e| create_parquet_error("Failed to read record batch", e)) {
                    // Filter the batch by PNR
                    if let Ok(filtered_batch) = filter_batch_by_pnr(batch, pnr_filter) {
                        // Add non-empty batches to the collector
                        if filtered_batch.num_rows() > 0 {
                            let mut batches = batches_collector.lock().unwrap();
                            batches.push(filtered_batch);
                        }
                    }
                }
            });
            
        // Get the collected batches
        batches_collector.into_inner().unwrap()
    } else {
        // No filter, process all batches in parallel with error handling
        let result: Result<Vec<RecordBatch>> = batch_results
            .into_iter()
            .map(|batch_result| {
                batch_result.map_err(|e| create_parquet_error("Failed to read record batch", e))
            })
            .collect();
            
        result?
    };

    log_operation_complete("read", path, batches.len(), Some(start.elapsed()));
    Ok(batches)
}

/// Filter a record batch by PNR using vectorized operations
/// This function only keeps rows where the PNR column value is in the provided set
///
/// # Errors
/// Returns an error if the PNR column cannot be found or filtered
fn filter_batch_by_pnr(batch: &RecordBatch, pnr_filter: &HashSet<String>) -> Result<RecordBatch> {
    use itertools::Itertools;
    
    // Find the PNR column
    let (_, pnr_idx) = find_pnr_column(batch)?;
    let pnr_array = batch.column(pnr_idx);
    
    let str_array = pnr_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ParquetReaderError::MetadataError("PNR column is not a string array".to_string())
        })?;
    
    // Create filter mask efficiently using itertools
    // This avoids manual looping and creates the boolean array in one pass
    let filter_values = (0..str_array.len())
        .map(|i| !str_array.is_null(i) && pnr_filter.contains(str_array.value(i)))
        .collect_vec();
    
    let filter_mask = BooleanArray::from(filter_values);
    
    // Use the common filter function from the filter module
    crate::filter::filter_record_batch(batch, &filter_mask)
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
    use itertools::Itertools;
    use rayon::prelude::*;
    
    log_operation_start("Searching for parquet files in", dir);

    // Validate directory
    validate_directory(dir)?;

    // Find all parquet files in the directory efficiently using rayon for parallelism
    // and itertools for improved iteration performance
    let parquet_files = std::fs::read_dir(dir)
        .map_err(|e| {
            ParquetReaderError::IoError(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Failed to read directory {}: {}", dir.display(), e),
            ))
        })?
        .par_bridge() // Convert to parallel iterator
        .filter_map(|entry_result| {
            match entry_result {
                Ok(entry) => {
                    let path = entry.path();
                    if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
                        Some(Ok(path))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(ParquetReaderError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read directory entry: {e}"),
                )))),
            }
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
pub fn load_parquet_files_parallel(
    dir: &Path,
    schema: Option<&Schema>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<RecordBatch>> {
    // Find all parquet files in the directory
    let parquet_files = find_parquet_files(dir)?;

    // If no files found, return empty result
    if parquet_files.is_empty() {
        return Ok(Vec::new());
    }

    // Clone schema and pnr_filter for sharing across threads
    let schema_arc = schema.map(|s| std::sync::Arc::new(s.clone()));
    let pnr_filter_arc = pnr_filter.map(|f| std::sync::Arc::new(f.clone()));

    // Process files in parallel using rayon
    let all_batches: Vec<Result<Vec<RecordBatch>>> = parquet_files
        .par_iter()
        .map(|path| {
            // Use clone of schema and pnr_filter
            let schema_ref = schema_arc.as_ref().map(std::convert::AsRef::as_ref);
            let pnr_filter_ref = pnr_filter_arc.as_ref().map(std::convert::AsRef::as_ref);

            read_parquet(path, schema_ref, pnr_filter_ref)
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
