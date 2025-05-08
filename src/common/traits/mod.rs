//! Common traits used across the codebase
//!
//! This module defines traits that are used by multiple modules to avoid
//! circular dependencies and provide clear interfaces.

pub mod registry;
pub mod adapter;
pub mod collection;
pub mod async_loading;

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

// Re-export async loading traits
pub use async_loading::{
    AsyncLoader, AsyncFilterableLoader, AsyncPnrFilterableLoader, 
    AsyncDirectoryLoader, AsyncParallelLoader
};