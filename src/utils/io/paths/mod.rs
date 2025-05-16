//! Path utilities for file and directory operations
//!
//! This module provides utilities for working with file paths and directories
//! for different registry types.

pub mod general;
pub mod lpr;

// Re-export commonly used functions for convenience
pub use general::get_available_year_files;