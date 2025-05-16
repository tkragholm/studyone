//! Utility functions for working with registry data and Parquet files
//!
//! This module provides a comprehensive set of utilities for working with
//! registry data, Arrow arrays, Parquet files, and helper functions for
//! logging, progress tracking, and testing.

pub mod arrow;
pub mod io;
pub mod logging;
pub mod register;
pub mod test;

// Re-export the most commonly used functions for convenience
pub use io::parquet::{
    DEFAULT_BATCH_SIZE, find_parquet_files, get_batch_size, load_parquet_files_parallel,
    read_parquet, validate_directory,
};

pub use logging::log::{log_operation_complete, log_operation_start, log_warning};
pub use logging::progress::{
    create_group_progress_bar, create_main_progress_bar, create_multi_progress, create_spinner,
    finish_and_clear, finish_progress_bar,
};
