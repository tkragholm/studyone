//! Sequential Registry Processor module
//!
//! This module implements a sequential registry processor that loads and processes
//! Danish registry data in a specific sequence to build a complete dataset.

mod processor;
mod utils;

pub use processor::SequentialRegistryProcessor;
pub use utils::{get_registry_schema, get_string_value, map_socio_to_enum};

// Re-export the main entry point function
pub use processor::run_sequential_registry_example;