//! Health data processing algorithms
//!
//! This module implements algorithms for health data processing, including
//! LPR data harmonization, diagnosis classification, and SCD algorithm.

// LPR data processing modules
pub mod lpr2_processor;
pub mod lpr3_processor;
pub mod lpr_config;
pub mod lpr_loader;
pub mod lpr_utility;

// SCD algorithm module
pub mod scd;

// Re-export common types
pub use crate::models::health::diagnosis::ScdResult;
pub use lpr_config::LprConfig;
pub use lpr_loader::{load_diagnoses, process_lpr_data};
pub use scd::categories::ScdCategory;
pub use scd::severity::SeverityLevel;
