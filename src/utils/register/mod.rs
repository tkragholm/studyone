//! Registry utilities for working with data registries
//!
//! This module provides utilities for working with different registry types,
//! including field extraction, registry detection, field mapping, and integration.

pub mod detection;
pub mod extractors;
pub mod integration;
pub mod longitudinal;
pub mod longitudinal_loader;
pub mod mapping;

// Re-export commonly used functions for convenience
pub use detection::{detect_registry_type, detect_registry_type_as_str, RegistryType};
pub use extractors::{DateExtractor, FloatExtractor, IntegerExtractor, Setter, StringExtractor};
pub use integration::{
    DateConversionExt, DateRangeConfig, PnrLinked, Registry, RegistryFieldMapper,
    RegistryIntegrator, RegistryTransformer,
};
pub use longitudinal::{
    LongitudinalConfig, 
    TemporalRegistryData,
    detect_registry_time_periods,
    load_longitudinal_data,
    merge_temporal_individuals,
};
pub use longitudinal_loader::{
    LongitudinalDataset,
    load_all_longitudinal_data,
    load_selected_longitudinal_data,
};