//! Control data structure for the matching algorithm
//!
//! This module implements an optimized struct-of-arrays data structure for controls,
//! which improves cache locality by storing each attribute in its own contiguous array.

use chrono::Datelike;
use chrono::NaiveDate;
use std::cmp::Ordering;

/// Optimized struct-of-arrays data structure for controls
/// This improves cache locality by storing each attribute in its own contiguous array
#[derive(Debug)]
pub struct ControlData {
    /// Array of control PNRs
    pub pnrs: Vec<String>,
    /// Array of birth dates stored as days since epoch for faster comparison
    birth_days: Vec<i32>,
    /// Original birth dates for output
    pub birth_dates: Vec<NaiveDate>,
    /// Array of genders
    pub genders: Vec<Option<String>>,
    /// Array of family sizes
    pub family_sizes: Vec<Option<i32>>,
    /// Record batch indices for the controls
    pub indices: Vec<usize>,
}

impl ControlData {
    /// Create a new `ControlData` from extracted control attributes
    #[must_use]
    pub fn new(
        pnrs: Vec<String>,
        birth_dates: Vec<NaiveDate>,
        genders: Vec<Option<String>>,
        family_sizes: Vec<Option<i32>>,
        indices: Vec<usize>,
    ) -> Self {
        let capacity = pnrs.len();
        let mut birth_days = Vec::with_capacity(capacity);

        // Calculate days from CE for each birth date for efficient comparison
        for date in &birth_dates {
            birth_days.push(date.num_days_from_ce());
        }

        Self {
            pnrs,
            birth_days,
            birth_dates,
            genders,
            family_sizes,
            indices,
        }
    }

    /// Sort the control data by birth days for more efficient searching
    pub fn sort_by_birth_day(&mut self) {
        // Create a vector of indices
        let mut idx_vec: Vec<usize> = (0..self.pnrs.len()).collect();

        // Sort indices by birth_days
        idx_vec.sort_unstable_by_key(|&i| self.birth_days[i]);

        // Create new arrays with sorted data
        let mut sorted_pnrs = Vec::with_capacity(self.pnrs.len());
        let mut sorted_birth_days = Vec::with_capacity(self.birth_days.len());
        let mut sorted_birth_dates = Vec::with_capacity(self.birth_dates.len());
        let mut sorted_genders = Vec::with_capacity(self.genders.len());
        let mut sorted_family_sizes = Vec::with_capacity(self.family_sizes.len());
        let mut sorted_indices = Vec::with_capacity(self.indices.len());

        for &i in &idx_vec {
            sorted_pnrs.push(self.pnrs[i].clone());
            sorted_birth_days.push(self.birth_days[i]);
            sorted_birth_dates.push(self.birth_dates[i]);
            sorted_genders.push(self.genders[i].clone());
            sorted_family_sizes.push(self.family_sizes[i]);
            sorted_indices.push(self.indices[i]);
        }

        // Replace the original arrays
        self.pnrs = sorted_pnrs;
        self.birth_days = sorted_birth_days;
        self.birth_dates = sorted_birth_dates;
        self.genders = sorted_genders;
        self.family_sizes = sorted_family_sizes;
        self.indices = sorted_indices;
    }

    /// Find the range of controls with birth days within the window
    #[must_use]
    pub fn find_birth_day_range(&self, target_birth_day: i32, window: i32) -> (usize, usize) {
        let min_birth_day = target_birth_day - window;
        let max_birth_day = target_birth_day + window;

        // Find the first index where birth_day >= min_birth_day
        let start_idx = match self.birth_days.binary_search_by(|&day| {
            if day < min_birth_day {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        // Find the first index where birth_day > max_birth_day
        let end_idx = match self.birth_days.binary_search_by(|&day| {
            if day <= max_birth_day {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        (start_idx, end_idx)
    }

    /// Get the length of the control data
    #[must_use]
    pub fn len(&self) -> usize {
        self.pnrs.len()
    }

    /// Check if the control data is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pnrs.is_empty()
    }
}
