//! Health data processing algorithms
//!
//! This module implements algorithms for health data processing, including
//! LPR data harmonization, diagnosis classification, and SCD algorithm.

pub mod lpr_integration;
pub mod scd;

// Re-export common types from SCD module
pub use crate::models::diagnosis::ScdResult;
pub use scd::categories::ScdCategory;
pub use scd::severity::SeverityLevel;
