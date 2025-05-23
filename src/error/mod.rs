//! Error handling for the `ParquetReader`.
//!
//! This module provides a simplified error handling system based on anyhow,
//! while maintaining backward compatibility with the original error types.

use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use thiserror::Error;

pub mod util;

/// Main error enum for the `ParquetReader` library
///
/// This is kept for backward compatibility and type checking.
#[derive(Error, Debug)]
pub enum ParquetReaderError {
    /// IO error (file operations)
    #[error("IO error: {0}")]
    IoError(String),

    /// Parquet data processing error
    #[error("Parquet error: {0}")]
    ParquetError(String),

    /// Arrow data processing error
    #[error("Arrow error: {0}")]
    ArrowError(String),

    /// Schema compatibility error
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// File metadata error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Filter expression error
    #[error("Filter error: {0}")]
    FilterError(String),

    /// Async operation error
    #[error("Async error: {0}")]
    AsyncError(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Invalid operation error
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Column not found in record batch
    #[error("Column not found: {column}")]
    ColumnNotFound { column: String },

    /// Invalid data type for column
    #[error("Invalid data type for column {column}: expected {expected}")]
    InvalidDataType { column: String, expected: String },

    /// Custom error message
    #[error("Error: {message}")]
    Custom { message: String },

    /// Error for when a filter excludes an entity
    #[error("Filter excluded entity: {message}")]
    FilterExcluded { message: String },

    /// Any other error
    #[error("{0}")]
    Other(String),
}

// Alias Error to ParquetReaderError for backward compatibility
pub type Error = ParquetReaderError;

// We need to keep the old Result type for backward compatibility
/// Result type for legacy code using the old error type
pub type ParquetResult<T> = std::result::Result<T, ParquetReaderError>;

// New type alias using anyhow::Result
/// Result type for `ParquetReader` operations - this can accept any error type
pub type Result<T> = anyhow::Result<T>;

// Convenience functions for working with errors and context

/// Add path context to a Result
///
/// # Errors
/// Returns an error if the input result is an error, with the path context added
pub fn with_path_context<T, E: std::error::Error + Send + Sync + 'static>(
    result: std::result::Result<T, E>,
    path: impl AsRef<Path>,
    message: impl AsRef<str>,
) -> Result<T> {
    result.with_context(|| format!("{} (path: {})", message.as_ref(), path.as_ref().display()))
}

/// Create a new IO error Result with path context
///
/// # Errors
/// Always returns an IO error with the specified message and path
pub fn io_err<T>(message: impl AsRef<str>, path: impl AsRef<Path>) -> Result<T> {
    Err(anyhow::anyhow!(
        "IO error: {} (path: {})",
        message.as_ref(),
        path.as_ref().display()
    ))
}

/// Create a new validation error Result
///
/// # Errors
/// Always returns a validation error with the specified message
pub fn validation_err<T>(message: impl AsRef<str>) -> Result<T> {
    Err(anyhow::anyhow!("Validation error: {}", message.as_ref()))
}

// Conversions for backward compatibility

// io::Error to ParquetReaderError
impl From<io::Error> for ParquetReaderError {
    fn from(e: io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}

// ParquetError to ParquetReaderError
impl From<ParquetError> for ParquetReaderError {
    fn from(e: ParquetError) -> Self {
        Self::ParquetError(e.to_string())
    }
}

// ArrowError to ParquetReaderError
impl From<ArrowError> for ParquetReaderError {
    fn from(e: ArrowError) -> Self {
        Self::ArrowError(e.to_string())
    }
}

// String to ParquetReaderError
impl From<String> for ParquetReaderError {
    fn from(e: String) -> Self {
        Self::Other(e)
    }
}

// &str to ParquetReaderError
impl From<&str> for ParquetReaderError {
    fn from(e: &str) -> Self {
        Self::Other(e.to_string())
    }
}

// We don't need explicit ParquetReaderError -> anyhow::Error conversion
// because anyhow will automatically use the Display trait

// Add anyhow::Error -> ParquetReaderError conversion for compatibility
impl From<anyhow::Error> for ParquetReaderError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e.to_string())
    }
}

// Factory methods for backward compatibility
impl ParquetReaderError {
    /// Create a new IO error
    pub fn io_error<S: Into<String>>(message: S) -> Self {
        Self::IoError(message.into())
    }

    /// Create a new IO error with source error
    pub fn io_error_with_source<S: Into<String>>(message: S, _source: io::Error) -> Self {
        Self::IoError(message.into())
    }

    /// Create a new Parquet error
    pub fn parquet_error<S: Into<String>>(message: S) -> Self {
        Self::ParquetError(message.into())
    }

    /// Create a new Parquet error with source
    pub fn parquet_error_with_source<S: Into<String>>(message: S, _source: ParquetError) -> Self {
        Self::ParquetError(message.into())
    }

    /// Create a new Arrow error
    pub fn arrow_error<S: Into<String>>(message: S) -> Self {
        Self::ArrowError(message.into())
    }

    /// Create a new Arrow error with source
    pub fn arrow_error_with_source<S: Into<String>>(message: S, _source: ArrowError) -> Self {
        Self::ArrowError(message.into())
    }

    /// Create a new Schema error
    pub fn schema_error<S: Into<String>>(message: S) -> Self {
        Self::SchemaError(message.into())
    }

    /// Create a new Schema error with path
    pub fn schema_error_with_path<S: Into<String>, P: Into<PathBuf>>(message: S, path: P) -> Self {
        let path_str = path.into().display().to_string();
        Self::SchemaError(format!("{} (path: {})", message.into(), path_str))
    }

    /// Create a new Metadata error
    pub fn metadata_error<S: Into<String>>(message: S) -> Self {
        Self::MetadataError(message.into())
    }

    /// Create a new Metadata error with path
    pub fn metadata_error_with_path<S: Into<String>, P: Into<PathBuf>>(
        message: S,
        path: P,
    ) -> Self {
        let path_str = path.into().display().to_string();
        Self::MetadataError(format!("{} (path: {})", message.into(), path_str))
    }

    /// Create a new Filter error
    pub fn filter_error<S: Into<String>>(message: S) -> Self {
        Self::FilterError(message.into())
    }

    /// Create a new Async error
    pub fn async_error<S: Into<String>>(message: S) -> Self {
        Self::AsyncError(message.into())
    }

    /// Create a new Validation error
    pub fn validation_error<S: Into<String>>(message: S) -> Self {
        Self::ValidationError(message.into())
    }

    /// Create a new Invalid Operation error
    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation(message.into())
    }

    /// Create a general-purpose error
    pub fn other<S: Into<String>>(message: S) -> Self {
        Self::Other(message.into())
    }

    /// Create a column not found error
    pub fn column_not_found<S: Into<String>>(column: S) -> Self {
        Self::ColumnNotFound {
            column: column.into(),
        }
    }

    /// Create an invalid data type error
    pub fn invalid_data_type<S1: Into<String>, S2: Into<String>>(column: S1, expected: S2) -> Self {
        Self::InvalidDataType {
            column: column.into(),
            expected: expected.into(),
        }
    }

    /// Create a custom error
    pub fn custom<S: Into<String>>(message: S) -> Self {
        Self::Custom {
            message: message.into(),
        }
    }

    /// Create a filter excluded error
    pub fn filter_excluded<S: Into<String>>(message: S) -> Self {
        Self::FilterExcluded {
            message: message.into(),
        }
    }

    /// Add path context to an error message (for backward compatibility)
    ///
    /// # Returns
    /// Returns self with added path context
    #[must_use]
    pub fn with_path<P: Into<PathBuf>>(self, path: P) -> Self {
        let path_str = path.into().display().to_string();
        match self {
            Self::IoError(msg) => Self::IoError(format!("{msg} (path: {path_str})")),
            Self::ParquetError(msg) => Self::ParquetError(format!("{msg} (path: {path_str})")),
            Self::SchemaError(msg) => Self::SchemaError(format!("{msg} (path: {path_str})")),
            Self::MetadataError(msg) => Self::MetadataError(format!("{msg} (path: {path_str})")),
            Self::FilterError(msg) => Self::FilterError(format!("{msg} (path: {path_str})")),
            Self::AsyncError(msg) => Self::AsyncError(format!("{msg} (path: {path_str})")),
            Self::ValidationError(msg) => {
                Self::ValidationError(format!("{msg} (path: {path_str})"))
            }
            Self::InvalidOperation(msg) => {
                Self::InvalidOperation(format!("{msg} (path: {path_str})"))
            }
            Self::ArrowError(msg) => Self::ArrowError(format!("{msg} (path: {path_str})")),
            Self::ColumnNotFound { column } => Self::ColumnNotFound {
                column: format!("{column} (path: {path_str})"),
            },
            Self::InvalidDataType { column, expected } => Self::InvalidDataType {
                column: format!("{column} (path: {path_str})"),
                expected,
            },
            Self::Custom { message } => Self::Custom {
                message: format!("{message} (path: {path_str})"),
            },
            Self::FilterExcluded { message } => Self::FilterExcluded {
                message: format!("{message} (path: {path_str})"),
            },
            Self::Other(msg) => Self::Other(format!("{msg} (path: {path_str})")),
        }
    }

    /// Add additional context to an error message (for backward compatibility)
    ///
    /// # Returns
    /// Returns self with added context prefix
    #[must_use]
    pub fn context<S: Into<String>>(self, context: S) -> Self {
        let ctx = context.into();
        match self {
            Self::IoError(msg) => Self::IoError(format!("{ctx}: {msg}")),
            Self::ParquetError(msg) => Self::ParquetError(format!("{ctx}: {msg}")),
            Self::ArrowError(msg) => Self::ArrowError(format!("{ctx}: {msg}")),
            Self::SchemaError(msg) => Self::SchemaError(format!("{ctx}: {msg}")),
            Self::MetadataError(msg) => Self::MetadataError(format!("{ctx}: {msg}")),
            Self::FilterError(msg) => Self::FilterError(format!("{ctx}: {msg}")),
            Self::AsyncError(msg) => Self::AsyncError(format!("{ctx}: {msg}")),
            Self::ValidationError(msg) => Self::ValidationError(format!("{ctx}: {msg}")),
            Self::InvalidOperation(msg) => Self::InvalidOperation(format!("{ctx}: {msg}")),
            Self::ColumnNotFound { column } => Self::ColumnNotFound {
                column: format!("{ctx}: {column}"),
            },
            Self::InvalidDataType { column, expected } => Self::InvalidDataType {
                column: format!("{ctx}: {column}"),
                expected,
            },
            Self::Custom { message } => Self::Custom {
                message: format!("{ctx}: {message}"),
            },
            Self::FilterExcluded { message } => Self::FilterExcluded {
                message: format!("{ctx}: {message}"),
            },
            Self::Other(msg) => Self::Other(format!("{ctx}: {msg}")),
        }
    }
}

/// Extension traits for easy context addition to Results
pub trait ResultExt<T> {
    /// Add context to a Result
    ///
    /// # Errors
    /// Returns the original error with additional context if the result is an error
    fn with_msg<S: AsRef<str>>(self, msg: S) -> Result<T>;

    /// Add context and path to a Result
    ///
    /// # Errors
    /// Returns the original error with additional context including the path if the result is an error
    fn with_path_context<S: AsRef<str>, P: AsRef<Path>>(self, msg: S, path: P) -> Result<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> ResultExt<T> for std::result::Result<T, E> {
    fn with_msg<S: AsRef<str>>(self, msg: S) -> Result<T> {
        let msg_owned = msg.as_ref().to_owned();
        self.context(msg_owned)
    }

    fn with_path_context<S: AsRef<str>, P: AsRef<Path>>(self, msg: S, path: P) -> Result<T> {
        let msg_str = msg.as_ref();
        let path_ref = path.as_ref();
        self.with_context(move || format!("{} (path: {})", msg_str, path_ref.display()))
    }
}
