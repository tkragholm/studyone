//! Async operations for working with Parquet files
//! Provides functions for finding and reading Parquet files asynchronously

use std::path::{Path, PathBuf};
use tokio::fs::{self, File};

use crate::error::Result;
use crate::utils::{log_operation_complete, log_operation_start, log_warning, validate_directory};

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

    let mut entries = fs::read_dir(dir)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read directory {}: {}", dir.display(), e))?;

    while let Some(entry_result) = entries
        .next_entry()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read directory entry: {e}"))?
    {
        let path = entry_result.path();
        let metadata = fs::metadata(&path).await.map_err(|e| {
            anyhow::anyhow!("Failed to read metadata for {}: {}", path.display(), e)
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

/// Async helper to open a Parquet file for reading
///
/// # Arguments
/// * `path` - Path to the Parquet file
///
/// # Returns
/// An open file handle
///
/// # Errors
/// Returns an error if file opening fails
pub async fn open_parquet_file_async(path: &Path) -> Result<File> {
    File::open(path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open file {}: {}", path.display(), e))
}
