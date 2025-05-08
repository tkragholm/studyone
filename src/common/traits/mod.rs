//! Common traits used across the codebase
//!
//! This module defines traits that are used by multiple modules to avoid
//! circular dependencies and provide clear interfaces.

pub mod registry;
pub mod adapter;
pub mod collection;

// Re-export core traits for convenience
pub use registry::{
    RegistryAware, BefRegistry, IndRegistry, LprRegistry, MfrRegistry, DodRegistry
};

// Re-export adapter traits
pub use adapter::{
    RegistryAdapter, StatefulAdapter, AdapterFactory, BatchProcessor, ModelLookup
};

// Re-export collection traits
pub use collection::{
    ModelCollection, TemporalCollection, BatchCollection, LookupCollection, 
    RelatedCollection, CacheableCollection
};