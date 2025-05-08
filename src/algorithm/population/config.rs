//! Population configuration for research studies
//!
//! This module defines the configuration options for generating
//! population datasets for research purposes.

use chrono::NaiveDate;
use std::fmt;

/// Configuration for population generation
#[derive(Debug, Clone)]
pub struct PopulationConfig {
    /// Index date for the study (defines the point in time for assessment)
    pub index_date: NaiveDate,
    /// Minimum age for inclusion in the study population
    pub min_age: Option<u32>,
    /// Maximum age for inclusion in the study population
    pub max_age: Option<u32>,
    /// Whether to include only individuals resident in Denmark at the index date
    pub resident_only: bool,
    /// Whether to include only families with both parents
    pub two_parent_only: bool,
    /// Start date of the study period (for longitudinal data)
    pub study_start_date: Option<NaiveDate>,
    /// End date of the study period (for longitudinal data)
    pub study_end_date: Option<NaiveDate>,
}

impl Default for PopulationConfig {
    fn default() -> Self {
        Self {
            index_date: NaiveDate::from_ymd_opt(2015, 1, 1).unwrap(),
            min_age: None,
            max_age: None,
            resident_only: true,
            two_parent_only: false,
            study_start_date: None,
            study_end_date: None,
        }
    }
}

impl fmt::Display for PopulationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Population Configuration:")?;
        writeln!(f, "  Index Date: {}", self.index_date)?;
        if let Some(min_age) = self.min_age {
            writeln!(f, "  Minimum Age: {min_age}")?;
        }
        if let Some(max_age) = self.max_age {
            writeln!(f, "  Maximum Age: {max_age}")?;
        }
        writeln!(f, "  Resident Only: {}", self.resident_only)?;
        writeln!(f, "  Two Parent Only: {}", self.two_parent_only)?;
        if let Some(start) = self.study_start_date {
            writeln!(f, "  Study Start Date: {start}")?;
        }
        if let Some(end) = self.study_end_date {
            writeln!(f, "  Study End Date: {end}")?;
        }
        Ok(())
    }
}