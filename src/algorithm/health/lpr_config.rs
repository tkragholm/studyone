//! Configuration for LPR data processing
//!
//! This module defines the configuration options for processing data
//! from the Danish National Patient Registry (LPR).

use chrono::NaiveDate;
use std::collections::HashMap;

/// Configuration for LPR data processing
#[derive(Debug, Clone)]
pub struct LprConfig {
    /// Whether to include LPR2 data
    pub include_lpr2: bool,
    /// Whether to include LPR3 data
    pub include_lpr3: bool,
    /// Start date for filtering (inclusive)
    pub start_date: Option<NaiveDate>,
    /// End date for filtering (inclusive)
    pub end_date: Option<NaiveDate>,
    /// Columns to map from LPR2 (key=source column, value=target column)
    pub lpr2_column_mapping: HashMap<String, String>,
    /// Columns to map from LPR3 (key=source column, value=target column)
    pub lpr3_column_mapping: HashMap<String, String>,
}

impl Default for LprConfig {
    fn default() -> Self {
        let mut lpr2_mapping = HashMap::new();
        lpr2_mapping.insert("PNR".to_string(), "patient_id".to_string());
        lpr2_mapping.insert("C_ADIAG".to_string(), "primary_diagnosis".to_string());
        lpr2_mapping.insert("C_DIAGTYPE".to_string(), "diagnosis_type".to_string());
        lpr2_mapping.insert("D_INDDTO".to_string(), "admission_date".to_string());
        lpr2_mapping.insert("D_UDDTO".to_string(), "discharge_date".to_string());

        let mut lpr3_mapping = HashMap::new();
        lpr3_mapping.insert("cpr".to_string(), "patient_id".to_string());
        lpr3_mapping.insert("diagnosekode".to_string(), "primary_diagnosis".to_string());
        lpr3_mapping.insert("diagnose_type".to_string(), "diagnosis_type".to_string());
        lpr3_mapping.insert("starttidspunkt".to_string(), "admission_date".to_string());
        lpr3_mapping.insert("sluttidspunkt".to_string(), "discharge_date".to_string());

        Self {
            include_lpr2: true,
            include_lpr3: true,
            start_date: None,
            end_date: None,
            lpr2_column_mapping: lpr2_mapping,
            lpr3_column_mapping: lpr3_mapping,
        }
    }
}

impl LprConfig {
    /// Create a new LPR configuration with default settings
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure to use only LPR2 data
    #[must_use]
    pub fn lpr2_only(mut self) -> Self {
        self.include_lpr2 = true;
        self.include_lpr3 = false;
        self
    }

    /// Configure to use only LPR3 data
    #[must_use]
    pub fn lpr3_only(mut self) -> Self {
        self.include_lpr2 = false;
        self.include_lpr3 = true;
        self
    }

    /// Set the date range for filtering
    #[must_use]
    pub fn with_date_range(mut self, start_date: NaiveDate, end_date: NaiveDate) -> Self {
        self.start_date = Some(start_date);
        self.end_date = Some(end_date);
        self
    }

    /// Set the start date for filtering
    #[must_use]
    pub fn with_start_date(mut self, start_date: NaiveDate) -> Self {
        self.start_date = Some(start_date);
        self
    }

    /// Set the end date for filtering
    #[must_use]
    pub fn with_end_date(mut self, end_date: NaiveDate) -> Self {
        self.end_date = Some(end_date);
        self
    }

    /// Add or override a column mapping for LPR2
    pub fn add_lpr2_mapping(&mut self, source: &str, target: &str) {
        self.lpr2_column_mapping
            .insert(source.to_string(), target.to_string());
    }

    /// Add or override a column mapping for LPR3
    pub fn add_lpr3_mapping(&mut self, source: &str, target: &str) {
        self.lpr3_column_mapping
            .insert(source.to_string(), target.to_string());
    }

    /// Check if a date is within the configured range
    ///
    /// Returns true if:
    /// - No date range is configured, or
    /// - The date is within the configured range (inclusive)
    #[must_use]
    pub fn is_date_in_range(&self, date: &NaiveDate) -> bool {
        if let Some(start) = self.start_date {
            if *date < start {
                return false;
            }
        }

        if let Some(end) = self.end_date {
            if *date > end {
                return false;
            }
        }

        true
    }
}