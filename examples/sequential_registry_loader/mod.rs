//! Sequential Registry Processor module
//!
//! This module implements a sequential registry processor that loads and processes
//! Danish registry data in a specific sequence to build a complete dataset.

mod processor;
mod processor_unified;
mod utils;

pub use processor::SequentialRegistryProcessor;
pub use processor_unified::UnifiedRegistryProcessor;
pub use utils::{get_registry_schema, get_string_value, map_socio_to_enum};

// Re-export the main entry point functions
pub use processor::run_sequential_registry_example;
pub use processor_unified::run_unified_registry_example;