//! Diagnosis processing algorithms
//!
//! This module implements algorithms for processing medical diagnoses,
//! including secondary diagnoses and SCD classification.

pub mod secondary;
pub mod scd;

// Re-export common types
pub use secondary::SecondaryDiagnosis;
pub use scd::ScdDiseaseCodes;
