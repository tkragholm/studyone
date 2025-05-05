//! Async Parquet filtering operations
//! Provides functionality for filtering Parquet data during async reading
//!
//! This module is a thin wrapper around the centralized filter system.

// Import only the filter functions we're using
// No imports needed as we're just re-exporting

// Re-export the async filtering functions from the centralized module
pub use crate::filter::async_filtering::read_parquet_with_filter_async;
