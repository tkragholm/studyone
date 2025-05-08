//! Registry-to-Model Adapters
//!
//! This module contains implementations of the adapter traits that convert
//! registry data to domain models. It provides a consistent interface for
//! working with different registry types and their corresponding models.

use crate::common::traits::AdapterFactory;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

// Define adapter modules
pub mod bef;
pub mod ind;
pub mod lpr;
pub mod mfr;

// Re-export common adapters
pub use bef::{BefIndividualAdapter, BefFamilyAdapter, BefCombinedAdapter};
pub use ind::{IndIncomeAdapter};
pub use lpr::{LprDiagnosisAdapter};
pub use mfr::{MfrChildAdapter};

/// Central adapter factory that provides consistent configuration for all adapters
#[derive(Debug)]
pub struct AdapterFactoryImpl {
    // Configuration shared across adapters
    registry_base_path: std::path::PathBuf,
    enable_caching: bool,
}

impl AdapterFactoryImpl {
    /// Create a new adapter factory
    ///
    /// # Arguments
    ///
    /// * `registry_base_path` - Base path to registry data files
    /// * `enable_caching` - Whether to enable caching of adapter results
    ///
    /// # Returns
    ///
    /// * `Self` - A new adapter factory
    #[must_use] pub fn new(registry_base_path: std::path::PathBuf, enable_caching: bool) -> Self {
        Self {
            registry_base_path,
            enable_caching,
        }
    }
    
    /// Get the registry base path
    #[must_use] pub fn registry_base_path(&self) -> &std::path::Path {
        &self.registry_base_path
    }
    
    /// Check if caching is enabled
    #[must_use] pub fn caching_enabled(&self) -> bool {
        self.enable_caching
    }
}

impl AdapterFactory for AdapterFactoryImpl {
    fn create_adapter<A>(&self) -> Arc<A>
    where
        A: Send + Sync + 'static
    {
        // This is just a placeholder implementation
        // A real implementation would create an adapter with the
        // correct registry configuration
        unimplemented!("Direct adapter creation not yet implemented")
    }
    
    fn create_adapter_with_config<A, C>(&self, _config: C) -> Arc<A>
    where
        A: Send + Sync + 'static,
        C: Send + Sync + 'static
    {
        // This is just a placeholder implementation
        unimplemented!("Configured adapter creation not yet implemented")
    }
}

// Implement create_lookup_with for all types that implement ModelLookup
pub fn create_lookup_with<T, K, F>(models: &[T], key_fn: F) -> HashMap<K, Arc<T>>
where
    T: Clone,
    K: std::hash::Hash + Eq + std::fmt::Debug,
    F: Fn(&T) -> K,
{
    let mut lookup = HashMap::with_capacity(models.len());
    for model in models {
        let key = key_fn(model);
        lookup.insert(key, Arc::new(model.clone()));
    }
    lookup
}