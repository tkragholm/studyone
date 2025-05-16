//! Progress reporting utilities for long-running operations
//!
//! This module provides standardized progress reporting functionality 
//! for long-running operations, using the indicatif crate.

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

/// Default style for a main progress bar
pub const DEFAULT_MAIN_TEMPLATE: &str = "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({per_sec}) {msg}";

/// Default style for a group progress bar
pub const DEFAULT_GROUP_TEMPLATE: &str = "{spinner} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}";

/// Create a main progress bar with a standardized style
///
/// # Arguments
/// * `length` - Total length for the progress bar
/// * `description` - Optional description to display as the initial message
///
/// # Returns
/// A configured `ProgressBar`
#[must_use]
pub fn create_main_progress_bar(length: u64, description: Option<&str>) -> ProgressBar {
    let pb = ProgressBar::new(length);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(DEFAULT_MAIN_TEMPLATE)
            .unwrap()
            .progress_chars("#>-"),
    );
    
    if let Some(desc) = description {
        pb.set_message(desc.to_string());
    }
    
    pb
}

/// Create a secondary/group progress bar with a standardized style
///
/// # Arguments
/// * `length` - Total length for the progress bar
/// * `description` - Optional description to display as the initial message
///
/// # Returns
/// A configured `ProgressBar`
#[must_use]
pub fn create_group_progress_bar(length: u64, description: Option<&str>) -> ProgressBar {
    let pb = ProgressBar::new(length);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(DEFAULT_GROUP_TEMPLATE)
            .unwrap()
            .progress_chars("#>-"),
    );
    
    if let Some(desc) = description {
        pb.set_message(desc.to_string());
    }
    
    pb
}

/// Create a multi-progress setup with a main progress bar
///
/// # Arguments
/// * `total` - Total length for the main progress bar
/// * `description` - Optional description for the main progress bar
///
/// # Returns
/// A tuple containing the `MultiProgress` instance and the main `ProgressBar`
#[must_use]
pub fn create_multi_progress(total: u64, description: Option<&str>) -> (MultiProgress, ProgressBar) {
    let mp = MultiProgress::new();
    let main_pb = mp.add(create_main_progress_bar(total, description));
    
    (mp, main_pb)
}

/// Add a group progress bar to a `MultiProgress` instance
///
/// # Arguments
/// * `mp` - The `MultiProgress` instance
/// * `length` - Total length for the group progress bar
/// * `description` - Optional description for the group progress bar
///
/// # Returns
/// The configured group `ProgressBar`
#[must_use]
pub fn add_group_progress_bar(mp: &MultiProgress, length: u64, description: Option<&str>) -> ProgressBar {
    let pb = create_group_progress_bar(length, description);
    mp.add(pb)
}

/// Create a spinner progress bar for operations without a known length
///
/// # Arguments
/// * `message` - Optional message to display with the spinner
///
/// # Returns
/// A configured spinner `ProgressBar`
#[must_use]
pub fn create_spinner(message: Option<&str>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {elapsed_precise} {msg}")
            .unwrap(),
    );
    
    if let Some(msg) = message {
        pb.set_message(msg.to_string());
    }
    
    // Set reasonable tick rate
    pb.enable_steady_tick(Duration::from_millis(100));
    
    pb
}

/// Finish a progress bar with a completion message
///
/// # Arguments
/// * `pb` - The `ProgressBar` to finish
/// * `message` - Optional completion message
pub fn finish_progress_bar(pb: &ProgressBar, message: Option<&str>) {
    if let Some(msg) = message {
        pb.finish_with_message(msg.to_string());
    } else {
        pb.finish();
    }
}

/// Finish a progress bar and clear it from display
///
/// # Arguments
/// * `pb` - The `ProgressBar` to finish and clear
pub fn finish_and_clear(pb: &ProgressBar) {
    pb.finish_and_clear();
}