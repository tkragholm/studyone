//! Time period utilities for working with registry files
//!
//! This module provides utilities for working with time periods in registry files.
//! It supports various time period formats (yearly, monthly, quarterly) and provides
//! functions for extracting time periods from filenames.

use chrono::{NaiveDate, Datelike};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use regex::Regex;
use lazy_static::lazy_static;

/// Represents a time period for registry data
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimePeriod {
    /// Yearly time period (e.g., 2020)
    Year(i32),
    /// Monthly time period (e.g., 2020-01)
    Month(i32, u32), // year, month
    /// Quarterly time period (e.g., 2020-Q1)
    Quarter(i32, u32), // year, quarter (1-4)
}

impl TimePeriod {
    /// Get the start date of this time period
    pub fn start_date(&self) -> NaiveDate {
        match self {
            TimePeriod::Year(year) => NaiveDate::from_ymd_opt(*year, 1, 1).unwrap(),
            TimePeriod::Month(year, month) => NaiveDate::from_ymd_opt(*year, *month, 1).unwrap(),
            TimePeriod::Quarter(year, quarter) => {
                let month = (quarter - 1) * 3 + 1;
                NaiveDate::from_ymd_opt(*year, month, 1).unwrap()
            }
        }
    }

    /// Get the end date of this time period (inclusive)
    pub fn end_date(&self) -> NaiveDate {
        match self {
            TimePeriod::Year(year) => NaiveDate::from_ymd_opt(*year, 12, 31).unwrap(),
            TimePeriod::Month(year, month) => {
                let next_month = if *month == 12 {
                    NaiveDate::from_ymd_opt(*year + 1, 1, 1).unwrap()
                } else {
                    NaiveDate::from_ymd_opt(*year, month + 1, 1).unwrap()
                };
                next_month.pred_opt().unwrap() // Last day of current month
            }
            TimePeriod::Quarter(year, quarter) => {
                let end_month = *quarter * 3;
                let (end_year, end_month) = if end_month == 12 {
                    (*year, 12)
                } else {
                    (*year, end_month)
                };
                let next_month = if end_month == 12 {
                    NaiveDate::from_ymd_opt(end_year + 1, 1, 1).unwrap()
                } else {
                    NaiveDate::from_ymd_opt(end_year, end_month + 1, 1).unwrap()
                };
                next_month.pred_opt().unwrap() // Last day of last month in quarter
            }
        }
    }

    /// Check if this time period contains the given date
    pub fn contains(&self, date: &NaiveDate) -> bool {
        let start = self.start_date();
        let end = self.end_date();
        &start <= date && date <= &end
    }

    /// Get a human-readable string representation of this time period
    pub fn display(&self) -> String {
        match self {
            TimePeriod::Year(year) => format!("{year}"),
            TimePeriod::Month(year, month) => format!("{year}-{month:02}"),
            TimePeriod::Quarter(year, quarter) => format!("{year}-Q{quarter}"),
        }
    }
}

impl FromStr for TimePeriod {
    type Err = String;

    /// Parse a string into a TimePeriod
    /// 
    /// Supported formats:
    /// - "2020" - Year
    /// - "202001" - Year and month (YYYYMM)
    /// - "2020-01" - Year and month (YYYY-MM)
    /// - "2020Q1" - Year and quarter (YYYYQ1)
    /// - "2020-Q1" - Year and quarter (YYYY-Q1)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref YEAR_PATTERN: Regex = Regex::new(r"^(\d{4})$").unwrap();
            static ref MONTH_PATTERN: Regex = Regex::new(r"^(\d{4})[-]?(\d{2})$").unwrap();
            static ref QUARTER_PATTERN: Regex = Regex::new(r"^(\d{4})[-]?Q(\d)$").unwrap();
        }

        if let Some(caps) = YEAR_PATTERN.captures(s) {
            let year = caps.get(1).unwrap().as_str().parse::<i32>().map_err(|e| e.to_string())?;
            Ok(TimePeriod::Year(year))
        } else if let Some(caps) = MONTH_PATTERN.captures(s) {
            let year = caps.get(1).unwrap().as_str().parse::<i32>().map_err(|e| e.to_string())?;
            let month = caps.get(2).unwrap().as_str().parse::<u32>().map_err(|e| e.to_string())?;
            if month < 1 || month > 12 {
                return Err(format!("Invalid month: {month}"));
            }
            Ok(TimePeriod::Month(year, month))
        } else if let Some(caps) = QUARTER_PATTERN.captures(s) {
            let year = caps.get(1).unwrap().as_str().parse::<i32>().map_err(|e| e.to_string())?;
            let quarter = caps.get(2).unwrap().as_str().parse::<u32>().map_err(|e| e.to_string())?;
            if quarter < 1 || quarter > 4 {
                return Err(format!("Invalid quarter: {quarter}"));
            }
            Ok(TimePeriod::Quarter(year, quarter))
        } else {
            Err(format!("Invalid time period format: {s}"))
        }
    }
}

/// Extract a time period from a filename
///
/// This function tries to extract a time period from a filename using various pattern
/// matching strategies. It supports yearly, monthly, and quarterly periods.
///
/// # Arguments
/// * `filename` - The filename to extract from
///
/// # Returns
/// An Option containing the extracted TimePeriod, or None if no valid time period was found
pub fn extract_time_period(filename: &str) -> Option<TimePeriod> {
    // Remove file extension if present
    let base_name = Path::new(filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(filename);

    // Try different patterns for time periods
    if let Ok(period) = TimePeriod::from_str(base_name) {
        return Some(period);
    }

    // Handle more complex patterns with regex
    lazy_static! {
        // Match year patterns like: year_2020.parquet, 2020_data.parquet, etc.
        static ref YEAR_IN_FILENAME: Regex = Regex::new(r"(?:^|[_-])(\d{4})(?:[_-]|$)").unwrap();
        
        // Match month patterns: 202001, 2020-01, 2020_01, etc.
        static ref MONTH_IN_FILENAME: Regex = Regex::new(r"(\d{4})[_-]?(\d{2})").unwrap();
        
        // Match quarter patterns: 2020Q1, 2020-Q1, etc.
        static ref QUARTER_IN_FILENAME: Regex = Regex::new(r"(\d{4})[_-]?Q(\d)").unwrap();
    }

    // Try to extract a month (most specific)
    if let Some(caps) = MONTH_IN_FILENAME.captures(base_name) {
        let year_str = caps.get(1).unwrap().as_str();
        let month_str = caps.get(2).unwrap().as_str();
        
        if let (Ok(year), Ok(month)) = (year_str.parse::<i32>(), month_str.parse::<u32>()) {
            if month >= 1 && month <= 12 {
                return Some(TimePeriod::Month(year, month));
            }
        }
    }

    // Try to extract a quarter
    if let Some(caps) = QUARTER_IN_FILENAME.captures(base_name) {
        let year_str = caps.get(1).unwrap().as_str();
        let quarter_str = caps.get(2).unwrap().as_str();
        
        if let (Ok(year), Ok(quarter)) = (year_str.parse::<i32>(), quarter_str.parse::<u32>()) {
            if quarter >= 1 && quarter <= 4 {
                return Some(TimePeriod::Quarter(year, quarter));
            }
        }
    }

    // Try to extract a year (least specific)
    if let Some(caps) = YEAR_IN_FILENAME.captures(base_name) {
        let year_str = caps.get(1).unwrap().as_str();
        
        if let Ok(year) = year_str.parse::<i32>() {
            return Some(TimePeriod::Year(year));
        }
    }

    None
}

/// Find all registry files with time periods in a directory
///
/// # Arguments
/// * `registry_dir` - The directory to search in
///
/// # Returns
/// A vector of (PathBuf, TimePeriod) pairs for each file with a valid time period
pub fn find_time_period_files(registry_dir: &Path) -> Vec<(PathBuf, TimePeriod)> {
    let mut result = Vec::new();

    if !registry_dir.exists() || !registry_dir.is_dir() {
        return result;
    }

    if let Ok(entries) = std::fs::read_dir(registry_dir) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            
            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                if let Some(filename) = path.file_name().and_then(|name| name.to_str()) {
                    if let Some(period) = extract_time_period(filename) {
                        result.push((path, period));
                    }
                }
            }
        }
    }

    // Sort by time period (earliest first)
    result.sort_by(|a, b| a.1.cmp(&b.1));
    result
}

/// Find registry files within a specific time range
///
/// # Arguments
/// * `registry_dir` - The directory to search in
/// * `start_date` - The start date of the range (inclusive)
/// * `end_date` - The end date of the range (inclusive)
///
/// # Returns
/// A vector of (PathBuf, TimePeriod) pairs for files within the time range
pub fn find_files_in_period(
    registry_dir: &Path,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<(PathBuf, TimePeriod)> {
    let all_files = find_time_period_files(registry_dir);
    
    all_files
        .into_iter()
        .filter(|(_, period)| {
            let period_start = period.start_date();
            let period_end = period.end_date();
            
            // Check for overlap with the specified date range
            !(period_end < start_date || period_start > end_date)
        })
        .collect()
}

/// Get the time period from a date
///
/// # Arguments
/// * `date` - The date to convert
/// * `period_type` - The type of period to create ("year", "month", "quarter")
///
/// # Returns
/// The corresponding TimePeriod
pub fn time_period_from_date(date: &NaiveDate, period_type: &str) -> TimePeriod {
    match period_type.to_lowercase().as_str() {
        "month" => TimePeriod::Month(date.year(), date.month()),
        "quarter" => {
            let quarter = (date.month() - 1) / 3 + 1;
            TimePeriod::Quarter(date.year(), quarter)
        }
        _ => TimePeriod::Year(date.year()),
    }
}

/// Gets the latest time period available for a registry
///
/// # Arguments
/// * `registry_dir` - The directory to search in
///
/// # Returns
/// An Option containing the latest TimePeriod found, or None if no files with time periods exist
pub fn get_latest_time_period(registry_dir: &Path) -> Option<TimePeriod> {
    let files = find_time_period_files(registry_dir);
    files.last().map(|(_, period)| *period)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_period_from_str() {
        assert_eq!(TimePeriod::from_str("2020").unwrap(), TimePeriod::Year(2020));
        assert_eq!(TimePeriod::from_str("202001").unwrap(), TimePeriod::Month(2020, 1));
        assert_eq!(TimePeriod::from_str("2020-01").unwrap(), TimePeriod::Month(2020, 1));
        assert_eq!(TimePeriod::from_str("2020Q1").unwrap(), TimePeriod::Quarter(2020, 1));
        assert_eq!(TimePeriod::from_str("2020-Q2").unwrap(), TimePeriod::Quarter(2020, 2));
    }

    #[test]
    fn test_extract_time_period() {
        assert_eq!(extract_time_period("2020.parquet"), Some(TimePeriod::Year(2020)));
        assert_eq!(extract_time_period("202001.parquet"), Some(TimePeriod::Month(2020, 1)));
        assert_eq!(extract_time_period("2020-Q1.parquet"), Some(TimePeriod::Quarter(2020, 1)));
        assert_eq!(extract_time_period("bef_2020.parquet"), Some(TimePeriod::Year(2020)));
        assert_eq!(extract_time_period("bef_202001.parquet"), Some(TimePeriod::Month(2020, 1)));
    }

    #[test]
    fn test_time_period_dates() {
        let year = TimePeriod::Year(2020);
        assert_eq!(year.start_date(), NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert_eq!(year.end_date(), NaiveDate::from_ymd_opt(2020, 12, 31).unwrap());

        let month = TimePeriod::Month(2020, 2);
        assert_eq!(month.start_date(), NaiveDate::from_ymd_opt(2020, 2, 1).unwrap());
        assert_eq!(month.end_date(), NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()); // 2020 is a leap year

        let quarter = TimePeriod::Quarter(2020, 1);
        assert_eq!(quarter.start_date(), NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert_eq!(quarter.end_date(), NaiveDate::from_ymd_opt(2020, 3, 31).unwrap());
    }

    #[test]
    fn test_time_period_contains() {
        let year = TimePeriod::Year(2020);
        assert!(year.contains(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));
        assert!(year.contains(&NaiveDate::from_ymd_opt(2020, 6, 15).unwrap()));
        assert!(year.contains(&NaiveDate::from_ymd_opt(2020, 12, 31).unwrap()));
        assert!(!year.contains(&NaiveDate::from_ymd_opt(2019, 12, 31).unwrap()));
        assert!(!year.contains(&NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()));
    }
}