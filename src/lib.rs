//! A Rust library for parsing and reading Parquet files with schema validation,
//! filtering, and async functionality.

pub mod async_io;
pub mod config;
pub mod error;
pub mod filter;
pub mod reader;
pub mod schema;
pub mod utils;

// Re-export the most common types for easier use
// Core types
pub use config::ParquetReaderConfig;
pub use error::{ParquetReaderError, Result};
pub use reader::{ParquetReader, ParquetRowIterator};
pub use schema::{SchemaCompatibilityReport, SchemaIssue};

// Arrow types
pub use arrow::datatypes::Schema as ArrowSchema;
pub use arrow::record_batch::RecordBatch;

// Filtering capabilities
pub use filter::{Expr, LiteralValue};
pub use filter::{evaluate_expr, filter_record_batch, read_parquet_with_filter};

// Utility functions
pub use utils::{read_parquet, load_parquet_files_parallel, DEFAULT_BATCH_SIZE};

// Async functionality
pub use async_io::{
    read_parquet_async, read_parquet_with_filter_async, read_parquet_with_pnr_filter_async,
    load_parquet_files_parallel_async, load_parquet_files_parallel_with_filter_async,
};
