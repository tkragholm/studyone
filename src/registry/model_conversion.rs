//! Model conversion traits and implementations
//!
//! This module provides traits and utilities for direct conversion between
//! registry data and domain models, enabling tighter integration between
//! the registry and model layers.

use crate::RecordBatch;
use crate::Result;
use crate::registry::RegisterLoader;
use std::collections::HashSet;
use std::path::Path;
use std::future::Future;
use std::pin::Pin;

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

/// Combined trait for registry loaders that support model conversion
///
/// This trait combines RegisterLoader and ModelConversion capabilities
/// into a single trait with additional convenience methods.
pub trait ModelConvertingRegisterLoader<T>: RegisterLoader {
    /// Convert a `RecordBatch` from this registry to a vector of domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<T>>;
    
    /// Convert domain models back to a `RecordBatch` conforming to this registry's schema
    fn from_models(&self, models: &[T]) -> Result<RecordBatch>;
    
    /// Apply additional transformations to models if needed
    fn transform_models(&self, _models: &mut [T]) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Load data from this registry directly as domain models
    fn load_as_models(&self, base_path: &Path, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<T>> {
        let batches = self.load(base_path, pnr_filter)?;
        let mut models = Vec::new();
        
        for batch in &batches {
            models.extend(self.to_models(batch)?);
        }
        
        // Apply any needed transformations
        self.transform_models(&mut models)?;
        
        Ok(models)
    }
    
    /// Load data from this registry directly as domain models asynchronously
    fn load_as_models_async<'a>(
        &'a self, 
        base_path: &'a Path, 
        pnr_filter: Option<&'a HashSet<String>>
    ) -> Pin<Box<dyn Future<Output = Result<Vec<T>>> + Send + 'a>> {
        Box::pin(async move {
            let batches = self.load_async(base_path, pnr_filter).await?;
            let mut models = Vec::new();
            
            for batch in &batches {
                models.extend(self.to_models(batch)?);
            }
            
            // Apply any needed transformations
            self.transform_models(&mut models)?;
            
            Ok(models)
        })
    }
}

// Implementation of ModelConvertingRegisterLoader for any type that implements
// both RegisterLoader and ModelConversion
impl<R, T> ModelConvertingRegisterLoader<T> for R 
where 
    R: RegisterLoader + ModelConversion<T>
{
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<T>> {
        ModelConversion::to_models(self, batch)
    }
    
    fn from_models(&self, models: &[T]) -> Result<RecordBatch> {
        ModelConversion::from_models(self, models)
    }
    
    fn transform_models(&self, models: &mut [T]) -> Result<()> {
        ModelConversion::transform_models(self, models)
    }
}