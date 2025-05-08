//! Unified Adapter Traits
//!
//! This module provides a standard interface for registry-to-model adapters.
//! It defines traits that all registry adapters should implement, ensuring
//! consistent behavior and reducing code duplication.

use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;
use std::sync::Arc;

/// Core adapter trait defining the standard functionality for all adapters
///
/// This trait provides a unified interface for converting registry data (in the form
/// of Arrow `RecordBatches`) to domain model objects. All registry adapters should
/// implement this trait to ensure consistent behavior.
pub trait RegistryAdapter<T>: Debug + Send + Sync {
    /// Convert a `RecordBatch` from a registry into domain model objects
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow `RecordBatch` containing registry data
    ///
    /// # Returns
    ///
    /// * `Result<Vec<T>>` - A vector of domain model objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<T>>;
    
    /// Apply additional transformations to model objects if needed
    ///
    /// # Arguments
    ///
    /// * `models` - A mutable slice of domain model objects to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform(_models: &mut [T]) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Extended adapter trait for stateful adapters that need access to context
///
/// Unlike the static `RegistryAdapter` trait, this trait allows adapters
/// to maintain state and configuration that affects the conversion process.
pub trait StatefulAdapter<T>: Debug + Send + Sync {
    /// Convert a `RecordBatch` from a registry into domain model objects
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow `RecordBatch` containing registry data
    ///
    /// # Returns
    ///
    /// * `Result<Vec<T>>` - A vector of domain model objects
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<T>>;
    
    /// Apply additional transformations to model objects if needed
    ///
    /// # Arguments
    ///
    /// * `models` - A mutable slice of domain model objects to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform_models(&self, _models: &mut [T]) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Process a batch and apply transformations in one step
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow `RecordBatch` containing registry data
    ///
    /// # Returns
    ///
    /// * `Result<Vec<T>>` - A vector of transformed domain model objects
    fn process_batch(&self, batch: &RecordBatch) -> Result<Vec<T>> {
        let mut models = self.convert_batch(batch)?;
        self.transform_models(&mut models)?;
        Ok(models)
    }
}

/// Factory trait for creating adapters with shared configuration
///
/// This trait enables the creation of adapters with consistent configuration
/// and dependencies, reducing the need to manually configure each adapter.
pub trait AdapterFactory: Debug + Send + Sync {
    /// Create a new adapter of the specified type
    ///
    /// # Type Parameters
    ///
    /// * `A` - The adapter type
    ///
    /// # Returns
    ///
    /// * `Arc<A>` - A reference-counted pointer to the new adapter
    fn create_adapter<A>(&self) -> Arc<A>
    where
        A: Send + Sync + 'static;
        
    /// Create a new adapter with additional configuration
    ///
    /// # Type Parameters
    ///
    /// * `A` - The adapter type
    /// * `C` - The configuration type
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration object
    ///
    /// # Returns
    ///
    /// * `Arc<A>` - A reference-counted pointer to the new adapter
    fn create_adapter_with_config<A, C>(&self, config: C) -> Arc<A>
    where
        A: Send + Sync + 'static,
        C: Send + Sync + 'static;
}

/// Trait for batch-processing adapters that handle multiple model types
///
/// This trait is useful for adapters that process related model types
/// from the same registry data, such as individuals and families from
/// BEF registry data.
pub trait BatchProcessor: Debug + Send + Sync {
    /// Process a batch and return multiple model collections
    ///
    /// This is a generic method that must be implemented by specific
    /// processor types with concrete return types. The implementation in
    /// this trait is just a marker.
    fn process_batch(&self, _batch: &RecordBatch) -> Result<()> {
        // Base implementation does nothing
        // Specific implementations will return appropriate model collections
        Ok(())
    }
}

/// Helper trait for creating model lookups by keys
///
/// This trait standardizes the creation of lookup maps from model objects,
/// which is a common operation in registry processing.
pub trait ModelLookup<T, K> {
    /// Create a lookup map from key to model
    ///
    /// # Arguments
    ///
    /// * `models` - A slice of model objects
    ///
    /// # Returns
    ///
    /// * A hash map from key to model
    fn create_lookup(models: &[T]) -> std::collections::HashMap<K, Arc<T>>;
}