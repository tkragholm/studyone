//! Async Parquet file loading utilities
//! Provides optimized asynchronous reading of Parquet files using Arrow

// Declare submodules
pub mod batch_ops;
pub mod file_ops;
pub mod filter_ops;
pub mod loader;
pub mod parallel_ops;

// Re-export all public items from submodules
pub use batch_ops::*;
pub use file_ops::*;
pub use filter_ops::*;
pub use loader::*;
pub use parallel_ops::*;
