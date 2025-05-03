//! Error handling for the ParquetReader.

use std::{io, fmt};
use parquet::errors::ParquetError;

/// Specialized error type for the ParquetReader
#[derive(Debug)]
pub enum ParquetReaderError {
    /// Error opening or reading a file
    IoError(io::Error),
    /// Error processing Parquet data
    ParquetError(ParquetError),
    /// Error with schema compatibility
    SchemaError(String),
    /// Error with file metadata
    MetadataError(String),
}

impl From<io::Error> for ParquetReaderError {
    fn from(error: io::Error) -> Self {
        ParquetReaderError::IoError(error)
    }
}

impl From<ParquetError> for ParquetReaderError {
    fn from(error: ParquetError) -> Self {
        ParquetReaderError::ParquetError(error)
    }
}

impl fmt::Display for ParquetReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParquetReaderError::IoError(e) => write!(f, "IO error: {}", e),
            ParquetReaderError::ParquetError(e) => write!(f, "Parquet error: {}", e),
            ParquetReaderError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            ParquetReaderError::MetadataError(msg) => write!(f, "Metadata error: {}", msg),
        }
    }
}

impl std::error::Error for ParquetReaderError {}

/// Result type for ParquetReader operations
pub type Result<T> = std::result::Result<T, ParquetReaderError>;
