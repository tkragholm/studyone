//! Error handling utilities for the filter module
//!
//! This module provides consistent error handling functions for the filter module.

use anyhow::Context;
use std::path::Path;

use crate::error::{ParquetReaderError, Result};

/// Create a filter error with context
///
/// # Arguments
/// * `message` - The error message
///
/// # Returns
/// A filter error with the given message
pub fn filter_err<T>(message: impl AsRef<str>) -> Result<T> {
    Err(ParquetReaderError::filter_error(message.as_ref()).into())
}

/// Add filter context to a Result
///
/// # Arguments
/// * `result` - The result to add context to
/// * `message` - The context message
///
/// # Returns
/// The result with added context
pub fn with_filter_context<T, E: std::error::Error + Send + Sync + 'static>(
    result: std::result::Result<T, E>,
    message: impl AsRef<str>,
) -> Result<T> {
    result.with_context(|| format!("Filter error: {}", message.as_ref()))
}

/// Create a filter error with path context
///
/// # Arguments
/// * `message` - The error message
/// * `path` - The path related to the error
///
/// # Returns
/// A filter error with the given message and path
pub fn filter_path_err<T>(message: impl AsRef<str>, path: impl AsRef<Path>) -> Result<T> {
    Err(ParquetReaderError::filter_error(message.as_ref())
        .with_path(path.as_ref())
        .into())
}

/// Create a column not found error
///
/// # Arguments
/// * `column_name` - The name of the column that was not found
///
/// # Returns
/// A filter error for the missing column
pub fn column_not_found<T>(column_name: &str) -> Result<T> {
    filter_err(format!("Column '{column_name}' not found"))
}

/// Create a column type error
///
/// # Arguments
/// * `column_name` - The name of the column
/// * `expected_type` - The expected type
///
/// # Returns
/// A filter error for the type mismatch
pub fn column_type_error<T>(column_name: &str, expected_type: &str) -> Result<T> {
    filter_err(format!(
        "Column '{column_name}' is not a {expected_type} array"
    ))
}

/// Create an invalid filter expression error
///
/// # Arguments
/// * `expr` - Description of the expression
///
/// # Returns
/// A filter error for the invalid expression
pub fn invalid_expr<T>(expr: impl std::fmt::Debug) -> Result<T> {
    filter_err(format!("Unsupported filter expression: {expr:?}"))
}

/// Extension trait for Result<RecordBatch> to add filter-specific context methods
pub trait FilterResultExt<T> {
    /// Add a filter-specific context message
    fn with_filter_context(self, message: impl AsRef<str>) -> Result<T>;

    /// Add context about the column being processed
    fn with_column_context(self, column_name: &str) -> Result<T>;

    /// Add context about the expression being evaluated
    fn with_expr_context(self, expr: impl std::fmt::Debug) -> Result<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> FilterResultExt<T>
    for std::result::Result<T, E>
{
    fn with_filter_context(self, message: impl AsRef<str>) -> Result<T> {
        self.with_context(|| format!("Filter error: {}", message.as_ref()))
    }

    fn with_column_context(self, column_name: &str) -> Result<T> {
        self.with_context(|| format!("Error processing column '{column_name}'"))
    }

    fn with_expr_context(self, expr: impl std::fmt::Debug) -> Result<T> {
        self.with_context(|| format!("Error evaluating expression: {expr:?}"))
    }
}
