//! Type definitions for the matching algorithm
//!
//! This module contains common types used throughout the matching algorithm.

use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::time::Duration;

/// Result of the matching process
#[derive(Debug, Clone)]
pub struct MatchingResult {
    /// Matched cases batch
    pub matched_cases: RecordBatch,
    /// Matched controls batch
    pub matched_controls: RecordBatch,
    /// Number of cases matched
    pub matched_case_count: usize,
    /// Number of controls matched
    pub matched_control_count: usize,
    /// Time taken for matching
    pub matching_time: Duration,
}

/// Pair of matched case and control
#[derive(Debug, Clone)]
pub struct MatchedPair {
    /// Case PNR (personal identification number)
    pub case_pnr: String,
    /// Case birth date
    pub case_birth_date: NaiveDate,
    /// Control PNR
    pub control_pnr: String,
    /// Control birth date
    pub control_birth_date: NaiveDate,
    /// Date when the match was made
    pub match_date: NaiveDate,
}

/// Structure to hold extracted attributes with indices
#[derive(Debug, Clone)]
pub struct ExtractedAttributes {
    /// Personal identification numbers
    pub pnrs: Vec<String>,
    /// Birth dates
    pub birth_dates: Vec<NaiveDate>,
    /// Genders (optional)
    pub genders: Vec<Option<String>>,
    /// Family sizes (optional)
    pub family_sizes: Vec<Option<i32>>,
    /// Record batch indices
    pub indices: Vec<usize>,
}

impl ExtractedAttributes {
    /// Check if the attributes are empty
    pub fn is_empty(&self) -> bool {
        self.pnrs.is_empty()
    }
}