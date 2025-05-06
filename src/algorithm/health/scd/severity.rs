//! Severity classification for SCD diagnoses
//!
//! This module implements severity classification for Severe Chronic Disease diagnoses,
//! with multiple approaches to determine disease severity.

use std::fmt;

/// Severity levels for SCD diagnoses
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SeverityLevel {
    /// Mild severity (e.g., asthma)
    Mild = 1,
    /// Moderate severity (most SCD conditions)
    Moderate = 2,
    /// Severe (e.g., cancer, organ transplantation, chromosomal anomalies)
    Severe = 3,
}

impl SeverityLevel {
    /// Convert a numeric severity level (1-3) to `SeverityLevel`
    #[must_use]
    pub const fn from_i32(level: i32) -> Self {
        match level {
            1 => Self::Mild,
            2 => Self::Moderate,
            3 => Self::Severe,
            _ => Self::Moderate, // Default to moderate for unknown values
        }
    }
    
    /// Get the numeric value for this severity level
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
    
    /// Get a descriptive name for this severity level
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::Mild => "Mild",
            Self::Moderate => "Moderate",
            Self::Severe => "Severe",
        }
    }
}

impl fmt::Display for SeverityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Calculate severity based on hospitalization count
#[must_use]
pub const fn hospitalization_severity(hospitalization_count: i32) -> SeverityLevel {
    if hospitalization_count >= 5 {
        SeverityLevel::Severe     // Very frequent hospitalizations
    } else if hospitalization_count >= 2 {
        SeverityLevel::Moderate   // Multiple hospitalizations
    } else {
        SeverityLevel::Mild       // Few or no hospitalizations
    }
}

/// Calculate severity based on the number of SCD categories present
#[must_use]
pub fn category_severity(category_count: usize) -> SeverityLevel {
    if category_count > 2 {
        SeverityLevel::Severe     // Many different systems affected
    } else if category_count == 2 {
        SeverityLevel::Moderate   // Two systems affected
    } else {
        SeverityLevel::Mild       // Only one system affected
    }
}

/// Calculate severity based on age at diagnosis
#[must_use]
pub fn age_at_diagnosis_severity(age_in_years: i32) -> SeverityLevel {
    if age_in_years < 2 {
        SeverityLevel::Severe     // Very early onset is usually more severe
    } else if age_in_years < 10 {
        SeverityLevel::Moderate   // Childhood onset
    } else {
        SeverityLevel::Mild       // Later onset
    }
}

/// Calculate a combined severity score based on multiple measures
#[must_use]
pub fn combined_severity(
    diagnosis_severity: SeverityLevel, 
    hospitalization_severity: SeverityLevel,
    category_severity: SeverityLevel,
    age_at_diagnosis_severity: Option<SeverityLevel>
) -> SeverityLevel {
    let max_severity = diagnosis_severity
        .max(hospitalization_severity)
        .max(category_severity);
    
    // If age at diagnosis is available, include it in the max calculation
    if let Some(age_severity) = age_at_diagnosis_severity {
        max_severity.max(age_severity)
    } else {
        max_severity
    }
}