//! Logging utilities for output and progress tracking
//!
//! This module provides utilities for logging, console output, and progress tracking.

pub mod console;
pub mod log;
pub mod progress;

// Re-export commonly used functions for convenience
pub use log::{log_operation_complete, log_operation_start, log_warning};
pub use progress::{
    create_group_progress_bar, create_main_progress_bar, create_multi_progress,
    create_spinner, finish_and_clear, finish_progress_bar,
};