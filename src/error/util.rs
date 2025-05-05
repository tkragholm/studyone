//! Utility functions for error handling
//!
//! This module provides utility functions to make error handling more convenient.

use std::path::Path;
use std::fs;
use std::io;

use crate::error::{ParquetReaderError, Result};

/// Safely open a file with rich error information
///
/// This function attempts to open a file and provides detailed
/// error information if the operation fails.
///
/// # Arguments
/// * `path` - The path to the file to open
/// * `purpose` - Why the file is being opened (for error context)
///
/// # Returns
/// * `Result<fs::File>` - The opened file or a detailed error
pub fn safe_open_file(path: &Path, purpose: &str) -> Result<fs::File> {
    // Check if the path exists
    if !path.exists() {
        return Err(ParquetReaderError::io_error(format!("File not found"))
            .with_path(path)
            .context(format!("Needed for: {}", purpose)));
    }

    // Check if the path is a file
    if !path.is_file() {
        return Err(ParquetReaderError::io_error(format!("Path is not a file"))
            .with_path(path)
            .context(format!("Expected a file for: {}", purpose)));
    }

    // Try to open the file
    match fs::File::open(path) {
        Ok(file) => Ok(file),
        Err(e) => {
            // Provide different error messages based on the error kind
            let context = match e.kind() {
                io::ErrorKind::PermissionDenied => {
                    format!("Permission denied - check file permissions")
                }
                io::ErrorKind::NotFound => {
                    format!("File not found - it may have been deleted during operation")
                }
                _ => format!("Failed to open file for: {}", purpose),
            };

            Err(ParquetReaderError::io_error_with_source(context, e)
                .with_path(path))
        }
    }
}

/// Check if a directory exists and is readable, with rich error information
pub fn validate_directory(path: &Path, purpose: &str) -> Result<()> {
    // Check if the path exists
    if !path.exists() {
        return Err(ParquetReaderError::io_error(format!("Directory not found"))
            .with_path(path)
            .context(format!("Needed for: {}", purpose)));
    }

    // Check if the path is a directory
    if !path.is_dir() {
        return Err(ParquetReaderError::io_error(format!("Path is not a directory"))
            .with_path(path)
            .context(format!("Expected a directory for: {}", purpose)));
    }

    // Try to read the directory to check permissions
    match fs::read_dir(path) {
        Ok(_) => Ok(()),
        Err(e) => {
            let context = match e.kind() {
                io::ErrorKind::PermissionDenied => {
                    format!("Permission denied - check directory permissions")
                }
                _ => format!("Failed to access directory for: {}", purpose),
            };

            Err(ParquetReaderError::io_error_with_source(context, e)
                .with_path(path))
        }
    }
}

/// Safely read a file to string with rich error information
pub fn safe_read_to_string(path: &Path, purpose: &str) -> Result<String> {
    // Open the file with rich error information
    let mut file = safe_open_file(path, purpose)?;
    
    // Read the file content
    let mut content = String::new();
    match std::io::Read::read_to_string(&mut file, &mut content) {
        Ok(_) => Ok(content),
        Err(e) => {
            let context = match e.kind() {
                io::ErrorKind::InvalidData => {
                    format!("File contains invalid UTF-8 data - cannot read as text")
                }
                _ => format!("Failed to read file content for: {}", purpose),
            };

            Err(ParquetReaderError::io_error_with_source(context, e)
                .with_path(path))
        }
    }
}

/// Try multiple operations in sequence, returning the first success or all errors
///
/// This function attempts multiple operations and returns the first successful result.
/// If all operations fail, it returns an error with details of all failures.
///
/// # Example
/// ```
/// use par_reader::error::util::try_operations;
/// use par_reader::error::Result;
/// use std::path::Path;
///
/// fn find_config() -> Result<String> {
///     try_operations(
///         "finding config file",
///         vec![
///             || safe_read_to_string(Path::new("/etc/app/config.json"), "primary config"),
///             || safe_read_to_string(Path::new("./config.json"), "local config"),
///             || safe_read_to_string(Path::new("./default_config.json"), "default config"),
///         ],
///     )
/// }
/// ```
pub fn try_operations<T, F>(operation_name: &str, operations: Vec<F>) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let mut errors = Vec::new();

    // Try each operation in sequence
    for (i, operation) in operations.into_iter().enumerate() {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                errors.push((i, e));
            }
        }
    }

    // If we get here, all operations failed
    // Create a combined error message
    let error_details = errors
        .into_iter()
        .map(|(i, e)| format!("Attempt {}: {}", i + 1, e))
        .collect::<Vec<_>>()
        .join("\n");

    Err(ParquetReaderError::other(format!(
        "All attempts failed for operation: {}\n{}",
        operation_name, error_details
    )))
}