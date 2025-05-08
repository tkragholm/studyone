//! Common traits used across the codebase
//!
//! This module defines traits that are used by multiple modules to avoid
//! circular dependencies and provide clear interfaces.

pub mod registry;
pub mod adapter;

// Re-export core traits for convenience
pub use registry::{
    RegistryAware, BefRegistry, IndRegistry, LprRegistry, MfrRegistry, DodRegistry
};

// Re-export adapter traits
pub use adapter::{
    RegistryAdapter, StatefulAdapter, AdapterFactory, BatchProcessor, ModelLookup
};