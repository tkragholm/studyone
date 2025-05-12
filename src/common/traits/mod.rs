//! Common traits used across the codebase
//!
//! This module defines traits that are used by multiple modules to avoid
//! circular dependencies and provide clear interfaces.

pub mod adapter;
pub mod async_loading;
pub mod collection;
pub mod registry;

// Re-export core traits for convenience
pub use registry::{
    AkmRegistry, BefRegistry, DodRegistry, IndRegistry, LprRegistry, MfrRegistry, RegistryAware,
    UddfRegistry, VndsRegistry,
};

// Re-export adapter traits
pub use adapter::{AdapterFactory, BatchProcessor, ModelLookup, RegistryAdapter, StatefulAdapter};

// Re-export collection traits
pub use collection::{
    BatchCollection, CacheableCollection, LookupCollection, ModelCollection, RelatedCollection,
    TemporalCollection,
};

// Re-export async loading traits
pub use async_loading::{
    AsyncDirectoryLoader, AsyncFilterableLoader, AsyncLoader, AsyncParallelLoader,
    AsyncPnrFilterableLoader,
};
