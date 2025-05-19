//! Temporal file path utilities
//!
//! This module provides utilities for working with time period-based data files,
//! supporting various file naming conventions like yearly (2018.parquet), 
//! monthly (201803.parquet), quarterly (2018Q1.parquet), etc.

use crate::error::Result;
use crate::models::core::individual::temporal::{TimePeriod, extract_time_period_from_filename};
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;

/// Get all parquet files with time periods for a specific registry
///
/// # Arguments
///
/// * `registry_dir` - Path to the registry directory
///
/// # Returns
///
/// A sorted map of time periods to file paths
pub fn get_registry_time_period_files(registry_dir: &Path) -> Result<BTreeMap<TimePeriod, PathBuf>> {
    // Find all parquet files in the directory
    let parquet_files = crate::utils::io::parquet::find_parquet_files(registry_dir)?;
    
    // Extract time periods for each file
    let mut period_files = BTreeMap::new();
    
    for path in parquet_files {
        if let Some(time_period) = extract_time_period_from_filename(&path) {
            period_files.insert(time_period, path);
        }
    }
    
    Ok(period_files)
}

/// Filter files to a specific time range
///
/// # Arguments
///
/// * `period_files` - Map of time periods to file paths
/// * `start_date` - Start date of the range
/// * `end_date` - End date of the range
///
/// # Returns
///
/// A filtered map of time periods to file paths within the range
pub fn filter_files_by_date_range(
    period_files: &BTreeMap<TimePeriod, PathBuf>,
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate
) -> BTreeMap<TimePeriod, PathBuf> {
    let mut filtered = BTreeMap::new();
    
    for (period, path) in period_files {
        let period_start = period.start_date();
        let period_end = period.end_date();
        
        // Check for overlap between the period and the requested range
        if period_start <= end_date && period_end >= start_date {
            filtered.insert(*period, path.clone());
        }
    }
    
    filtered
}

/// Get files for a specific year
///
/// # Arguments
///
/// * `period_files` - Map of time periods to file paths
/// * `year` - The year to filter by
///
/// # Returns
///
/// A filtered map of time periods to file paths for the specified year
pub fn get_files_for_year(
    period_files: &BTreeMap<TimePeriod, PathBuf>,
    year: i32
) -> BTreeMap<TimePeriod, PathBuf> {
    let mut filtered = BTreeMap::new();
    
    for (period, path) in period_files {
        if period.year() == year {
            filtered.insert(*period, path.clone());
        }
    }
    
    filtered
}

/// Find all available years in the time period files
///
/// # Arguments
///
/// * `period_files` - Map of time periods to file paths
///
/// # Returns
///
/// A sorted vector of years
pub fn get_available_years(period_files: &BTreeMap<TimePeriod, PathBuf>) -> Vec<i32> {
    let mut years = period_files
        .keys()
        .map(|period| period.year())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    
    years.sort_unstable();
    years
}