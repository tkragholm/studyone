//! Test utilities for testing and benchmarking
//!
//! This module provides utilities for testing and benchmarking the application.

pub mod fixtures;
pub mod helpers;

// Re-export commonly used functions for convenience
pub use fixtures::{data_dir, registry_dir, registry_file};
pub use helpers::{ensure_path_exists, expr_to_filter, test_config, timed_execution};