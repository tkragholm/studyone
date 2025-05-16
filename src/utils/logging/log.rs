//! Logging utilities
//!
//! This module provides standardized logging functions for operations.

use std::path::Path;

/// Log an operation start with consistent format
///
/// # Arguments
/// * `operation` - Description of the operation
/// * `path` - Path of the file or directory being operated on
pub fn log_operation_start(operation: &str, path: &Path) {
    log::info!("{} {}", operation, path.display());
}

/// Log an operation completion with consistent format
///
/// # Arguments
/// * `operation` - Description of the operation
/// * `path` - Path of the file or directory that was operated on
/// * `items` - Number of items processed
/// * `elapsed` - Optional elapsed time
pub fn log_operation_complete(
    operation: &str,
    path: &Path,
    items: usize,
    elapsed: Option<std::time::Duration>,
) {
    if let Some(duration) = elapsed {
        log::info!(
            "Successfully {} {} items from {} in {:?}",
            operation,
            items,
            path.display(),
            duration
        );
    } else {
        log::info!(
            "Successfully {} {} items from {}",
            operation,
            items,
            path.display()
        );
    }
}

/// Log an operation warning with consistent format
///
/// # Arguments
/// * `message` - Warning message
/// * `path` - Optional path related to the warning
pub fn log_warning(message: &str, path: Option<&Path>) {
    if let Some(path) = path {
        log::warn!("{}: {}", message, path.display());
    } else {
        log::warn!("{message}");
    }
}