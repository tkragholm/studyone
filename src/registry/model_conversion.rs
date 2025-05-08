//! Model conversion traits and implementations
//!
//! This module provides traits and utilities for direct conversion between
//! registry data and domain models, enabling tighter integration between
//! the registry and model layers.

use crate::RecordBatch;
use crate::Result;

/// Trait for bidirectional conversion between registry data and domain models
///
/// This trait enables registry types to directly convert their data to domain models
/// and vice versa, reducing the need for separate adapter modules.
pub trait ModelConversion<T> {
    /// Convert a `RecordBatch` from this registry to a vector of domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<T>>;
    
    /// Convert domain models back to a `RecordBatch` conforming to this registry's schema
    fn from_models(&self, models: &[T]) -> Result<RecordBatch>;
    
    /// Apply additional transformations to models if needed
    fn transform_models(&self, _models: &mut [T]) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Extension trait for registry loaders that support model conversion
pub trait ModelConversionExt {
    /// Load data from this registry directly as the specified model type
    ///
    /// # Type Parameters
    ///
    /// * `T` - The target model type
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory containing the registry's parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    ///
    /// * `Result<Vec<T>>` - Vector of domain models
    fn load_as<T>(&self, base_path: &std::path::Path, pnr_filter: Option<&std::collections::HashSet<String>>) -> Result<Vec<T>>
    where
        Self: ModelConversion<T> + crate::registry::RegisterLoader,
    {
        let batches = self.load(base_path, pnr_filter)?;
        let mut models = Vec::new();
        
        for batch in &batches {
            models.extend(self.to_models(batch)?);
        }
        
        // Apply any needed transformations
        self.transform_models(&mut models)?;
        
        Ok(models)
    }
    
    /// Load data from this registry directly as the specified model type asynchronously
    ///
    /// # Type Parameters
    ///
    /// * `T` - The target model type
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory containing the registry's parquet files
    /// * `pnr_filter` - Optional filter to only load data for specific PNRs
    ///
    /// # Returns
    ///
    /// * `impl Future<Output = Result<Vec<T>>>` - Future resolving to vector of domain models
    fn load_as_async<'a, T>(
        &'a self, 
        base_path: &'a std::path::Path, 
        pnr_filter: Option<&'a std::collections::HashSet<String>>
    ) -> impl std::future::Future<Output = Result<Vec<T>>> + Send + 'a
    where
        Self: ModelConversion<T> + crate::registry::RegisterLoader,
    {
        async move {
            let batches = self.load_async(base_path, pnr_filter).await?;
            let mut models = Vec::new();
            
            for batch in &batches {
                models.extend(self.to_models(batch)?);
            }
            
            // Apply any needed transformations
            self.transform_models(&mut models)?;
            
            Ok(models)
        }
    }
}

// Implement the extension trait for any type that implements RegisterLoader
impl<R> ModelConversionExt for R where R: crate::registry::RegisterLoader {}