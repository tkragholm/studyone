//! A Rust library for parsing and reading Parquet files with schema validation.

pub mod config;
pub mod error;
pub mod reader;
pub mod schema;

// Re-export the most common types for easier use
pub use config::ParquetReaderConfig;
pub use error::{ParquetReaderError, Result};
pub use reader::{ParquetReader, ParquetRowIterator};
pub use schema::{SchemaCompatibilityReport, SchemaIssue};
