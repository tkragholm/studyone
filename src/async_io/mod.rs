//! Async Parquet file loading utilities
//! Provides optimized asynchronous reading of Parquet files using Arrow

// Declare submodules
mod file_ops;
mod batch_ops;
mod filter_ops;
mod parallel_ops;

// Re-export all public items from submodules
pub use file_ops::*;
pub use batch_ops::*;
pub use filter_ops::*;
pub use parallel_ops::*;