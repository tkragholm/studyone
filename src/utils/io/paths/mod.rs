//! Path utilities for file and directory operations
//!
//! This module provides utilities for working with file paths and directories
//! for different registry types.

pub mod general;
pub mod lpr;
pub mod temporal;
pub mod time_period;

// Re-export commonly used functions for convenience
pub use general::get_available_year_files;
pub use temporal::{
    get_registry_time_period_files, 
    filter_files_by_date_range, 
    get_files_for_year,
    get_available_years
};
pub use time_period::{
    TimePeriod,
    extract_time_period,
    find_time_period_files,
    find_files_in_period,
    get_latest_time_period
};