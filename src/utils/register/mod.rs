//! Registry utilities for working with data registries
//!
//! This module provides utilities for working with different registry types,
//! including field extraction, registry detection, field mapping, and integration.

pub mod detection;
pub mod extractors;
pub mod integration;
pub mod mapping;

// Re-export commonly used functions for convenience
pub use detection::{detect_registry_type, detect_registry_type_as_str, RegistryType};
pub use extractors::{DateExtractor, FloatExtractor, IntegerExtractor, Setter, StringExtractor};
pub use integration::{
    DateConversionExt, DateRangeConfig, PnrLinked, Registry, RegistryFieldMapper,
    RegistryIntegrator, RegistryTransformer,
};