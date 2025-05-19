//! Time period module for longitudinal data
//!
//! This module provides data structures and utilities for handling time periods
//! in longitudinal registry data, allowing for tracking when data points originate from.

use crate::error::Result;
use chrono::{Datelike, NaiveDate};
use lazy_static::lazy_static;
use regex::Regex;
use std::path::Path;

use crate::models::core::individual::Individual;
use crate::models::core::traits::{HealthStatus, TemporalValidity};

// Implement TemporalValidity trait for Individual
impl TemporalValidity for Individual {
    /// Check if this entity was valid at a specific date
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        // For individuals, we consider them valid if they were born before or on the date
        // and either they haven't died yet or they died after the date
        match self.birth_date {
            Some(birth) => {
                birth <= *date
                    && match self.death_date {
                        Some(death) => death >= *date,
                        None => true, // No death date means still alive
                    }
            }
            None => false, // No birth date means we can't determine validity
        }
    }

    /// Get the start date of validity (birth date)
    fn valid_from(&self) -> NaiveDate {
        // Return birth date or a default date if not available
        self.birth_date
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap())
    }

    /// Get the end date of validity (death date if any)
    fn valid_to(&self) -> Option<NaiveDate> {
        self.death_date
    }

    /// Create a snapshot of this entity at a specific point in time
    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        if self.was_valid_at(date) {
            Some(self.clone())
        } else {
            None
        }
    }
}

// Additional temporal methods for Individual
impl Individual {
    /// Determine if this individual is a child based on age at reference date
    #[must_use]
    pub fn is_child(&self, reference_date: &NaiveDate) -> bool {
        if let Some(age) = self.age_at(reference_date) {
            age < 18
        } else {
            false
        }
    }

    /// Get the role of this individual at a reference date
    #[must_use]
    pub fn role_at(&self, reference_date: &NaiveDate, all_individuals: &[Self]) -> super::Role {
        let is_child = self.is_child(reference_date);
        let is_parent = self.is_parent_in_dataset(all_individuals);

        match (is_child, is_parent) {
            (true, true) => super::Role::ChildAndParent,
            (true, false) => super::Role::Child,
            (false, true) => super::Role::Parent,
            (false, false) => super::Role::Other,
        }
    }
}

/// Represents a time period for longitudinal data
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimePeriod {
    /// Yearly data (e.g., 2018)
    Year(i32),
    /// Monthly data (year and month, e.g., January 2018)
    Month(i32, u32),
    /// Quarterly data (year and quarter, e.g., Q1 2018)
    Quarter(i32, u32),
    /// Daily data (full date, e.g., January 1, 2018)
    Day(NaiveDate),
}

impl TimePeriod {
    /// Get the year component of the time period
    #[must_use]
    pub fn year(&self) -> i32 {
        match self {
            TimePeriod::Year(year) => *year,
            TimePeriod::Month(year, _) => *year,
            TimePeriod::Quarter(year, _) => *year,
            TimePeriod::Day(date) => date.year(),
        }
    }

    /// Get the start date of the time period
    #[must_use]
    pub fn start_date(&self) -> NaiveDate {
        match self {
            TimePeriod::Year(year) => NaiveDate::from_ymd_opt(*year, 1, 1).unwrap(),
            TimePeriod::Month(year, month) => NaiveDate::from_ymd_opt(*year, *month, 1).unwrap(),
            TimePeriod::Quarter(year, quarter) => {
                let month = (quarter - 1) * 3 + 1;
                NaiveDate::from_ymd_opt(*year, month, 1).unwrap()
            }
            TimePeriod::Day(date) => *date,
        }
    }

    /// Get the end date of the time period (inclusive)
    #[must_use]
    pub fn end_date(&self) -> NaiveDate {
        match self {
            TimePeriod::Year(year) => NaiveDate::from_ymd_opt(*year, 12, 31).unwrap(),
            TimePeriod::Month(year, month) => {
                // Calculate the last day of the month
                let last_day = match month {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    2 => {
                        // February - check for leap year
                        if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                            29
                        } else {
                            28
                        }
                    }
                    _ => unreachable!(),
                };
                NaiveDate::from_ymd_opt(*year, *month, last_day).unwrap()
            }
            TimePeriod::Quarter(year, quarter) => {
                let end_month = quarter * 3;
                let (y, m) = if end_month == 12 {
                    (*year, 12)
                } else {
                    (*year, end_month)
                };

                // Last day of the month
                let last_day = match m {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    2 => {
                        // February - check for leap year
                        if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                            29
                        } else {
                            28
                        }
                    }
                    _ => unreachable!(),
                };
                NaiveDate::from_ymd_opt(y, m, last_day).unwrap()
            }
            TimePeriod::Day(date) => *date,
        }
    }

    /// Check if a date falls within this time period
    #[must_use]
    pub fn contains(&self, date: NaiveDate) -> bool {
        let start = self.start_date();
        let end = self.end_date();
        date >= start && date <= end
    }

    /// Convert the time period to a string representation
    #[must_use]
    pub fn to_string(&self) -> String {
        match self {
            TimePeriod::Year(year) => format!("{}", year),
            TimePeriod::Month(year, month) => format!("{}{:02}", year, month),
            TimePeriod::Quarter(year, quarter) => format!("{}Q{}", year, quarter),
            TimePeriod::Day(date) => date.format("%Y%m%d").to_string(),
        }
    }
}

/// Extract time period from filename
///
/// # Arguments
///
/// * `path` - Path to the file
///
/// # Returns
///
/// Option containing the extracted TimePeriod if successful
pub fn extract_time_period_from_filename(path: &Path) -> Option<TimePeriod> {
    lazy_static! {
        // Patterns for different time period formats in filenames
        static ref YEAR_PATTERN: Regex = Regex::new(r"^(\d{4})\.parquet$").unwrap();
        static ref YEAR_MONTH_PATTERN: Regex = Regex::new(r"^(\d{4})(\d{2})\.parquet$").unwrap();
        static ref YEAR_QUARTER_PATTERN: Regex = Regex::new(r"^(\d{4})Q([1-4])\.parquet$").unwrap();
        static ref DATE_PATTERN: Regex = Regex::new(r"^(\d{4})(\d{2})(\d{2})\.parquet$").unwrap();
    }

    let filename = path.file_name()?.to_str()?;

    // Try to match different patterns
    if let Some(captures) = YEAR_PATTERN.captures(filename) {
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        return Some(TimePeriod::Year(year));
    }

    if let Some(captures) = YEAR_MONTH_PATTERN.captures(filename) {
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        let month = captures.get(2)?.as_str().parse::<u32>().ok()?;
        if month >= 1 && month <= 12 {
            return Some(TimePeriod::Month(year, month));
        }
    }

    if let Some(captures) = YEAR_QUARTER_PATTERN.captures(filename) {
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        let quarter = captures.get(2)?.as_str().parse::<u32>().ok()?;
        if quarter >= 1 && quarter <= 4 {
            return Some(TimePeriod::Quarter(year, quarter));
        }
    }

    if let Some(captures) = DATE_PATTERN.captures(filename) {
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        let month = captures.get(2)?.as_str().parse::<u32>().ok()?;
        let day = captures.get(3)?.as_str().parse::<u32>().ok()?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some(TimePeriod::Day(date));
        }
    }

    None
}

/// Get the registry type and time period from a file path
///
/// # Arguments
///
/// * `path` - Path to the file
///
/// # Returns
///
/// A tuple containing the registry type and time period if extraction is successful
pub fn extract_registry_and_time_period(path: &Path) -> Option<(String, TimePeriod)> {
    // Get directory name for registry type
    let parent = path.parent()?;
    let registry_type = parent.file_name()?.to_str()?.to_uppercase();

    // Extract time period from filename
    let time_period = extract_time_period_from_filename(path)?;

    Some((registry_type, time_period))
}

/// Get all files for a specific registry with their corresponding time periods
///
/// # Arguments
///
/// * `base_path` - Base path to the registry directory
///
/// # Returns
///
/// A Result containing a vector of tuples with file paths and their time periods
pub fn get_time_period_files(base_path: &Path) -> Result<Vec<(std::path::PathBuf, TimePeriod)>> {
    // Find all parquet files in the directory
    let parquet_files = crate::utils::io::parquet::find_parquet_files(base_path)?;

    // Extract time periods for each file
    let mut period_files = Vec::with_capacity(parquet_files.len());

    for path in parquet_files {
        if let Some(time_period) = extract_time_period_from_filename(&path) {
            period_files.push((path, time_period));
        }
    }

    // Sort by time period
    period_files.sort_by(|a, b| a.1.cmp(&b.1));

    Ok(period_files)
}
