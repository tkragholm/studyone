//! IO utilities for file operations
//!
//! This module provides utilities for working with files, paths,
//! and data formats like Parquet.

pub mod parquet;
pub mod paths;

// Re-export commonly used functions for convenience
pub use parquet::{
    find_parquet_files, load_parquet_files_parallel, read_parquet, validate_directory,
};
pub use paths::general::get_available_year_files;
pub use paths::lpr::*;