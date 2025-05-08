//! Case grouping for parallel processing
//!
//! This module contains the `CaseGroup` struct for organizing cases into groups
//! for efficient parallel processing based on birth date ranges.

use chrono::NaiveDate;

/// Case data grouped by birth day ranges for efficient parallel processing
#[derive(Debug)]
pub struct CaseGroup {
    /// Array of case PNRs
    pub pnrs: Vec<String>,
    /// Array of birth dates
    pub birth_dates: Vec<NaiveDate>,
    /// Array of genders
    pub genders: Vec<Option<String>>,
    /// Array of family sizes
    pub family_sizes: Vec<Option<i32>>,
    /// Record batch indices for the cases
    pub indices: Vec<usize>,
    /// Birth day range (start, end)
    pub birth_day_range: (i32, i32),
}
