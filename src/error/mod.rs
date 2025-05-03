//! Error handling for the `ParquetReader`.

use std::io;
use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use thiserror::Error;

/// Specialized error type for the `ParquetReader`
#[derive(Error, Debug)]
pub enum ParquetReaderError {
    /// Error opening or reading a file
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    
    /// Error processing Parquet data
    #[error("Parquet error: {0}")]
    ParquetError(#[from] ParquetError),
    
    /// Error with Arrow data processing
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),
    
    /// Error with schema compatibility
    #[error("Schema error: {0}")]
    SchemaError(String),
    
    /// Error with file metadata
    #[error("Metadata error: {0}")]
    MetadataError(String),
    
    /// Error with filter expression
    #[error("Filter error: {0}")]
    FilterError(String),
    
    /// Error with async operations
    #[error("Async error: {0}")]
    AsyncError(String),
}

/// Result type for `ParquetReader` operations
pub type Result<T> = std::result::Result<T, ParquetReaderError>;
