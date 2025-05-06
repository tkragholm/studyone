//! Population generation for research studies
//!
//! This module provides functionality for generating study populations
//! based on demographic and registry data.

pub mod core;
pub mod filters;
pub mod integration;

// Re-export commonly used items
pub use core::{Population, PopulationBuilder, PopulationConfig};
pub use filters::{PopulationFilter, FilterCriteria};
pub use integration::RegistryIntegration;