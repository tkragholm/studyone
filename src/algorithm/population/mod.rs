//! Population generation for research studies
//!
//! This module provides functionality for generating study populations
//! based on demographic and registry data.

pub mod builder;
pub mod config;
pub mod filters;
pub mod registry_loader;
pub mod statistics;

// Re-export commonly used items
pub use builder::{Population, PopulationBuilder, generate_test_population};
pub use config::PopulationConfig;
pub use filters::{PopulationFilter, FilterCriteria};
pub use registry_loader::RegistryIntegration;
pub use statistics::{PopulationStatistics, PopulationStats};