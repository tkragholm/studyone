//! Health data processing algorithms
//!
//! This module implements algorithms for health data processing, including
//! LPR data harmonization, diagnosis classification, and SCD algorithm.

pub mod lpr;
pub mod diagnosis;

// Re-export common types
pub use lpr::LprConfig;
pub use diagnosis::scd::{ScdConfig, ScdResult, ScdDiseaseCodes};
