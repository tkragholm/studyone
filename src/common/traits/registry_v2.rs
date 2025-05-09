//! Registry-specific model conversion traits (V2)
//!
//! This module contains improved traits for converting registry data into domain models,
//! with clearer separation of concerns between registry and model modules.

use crate::error::Result;
use arrow::record_batch::RecordBatch;

/// Trait for models that can be created from registry data
///
/// `RegistryModel` provides methods for constructing models from registry data.
/// This trait is implemented by domain models, but conversion logic lives in the registry module.
pub trait RegistryModel: Sized {
    /// Get the registry name for this model's primary data source
    fn primary_registry_name() -> &'static str;
    
    /// Create a model from a registry record using provided conversion function
    ///
    /// This method uses a registry-provided conversion function to create a model.
    /// The conversion function is responsible for the actual data extraction logic.
    fn from_registry_record(
        batch: &RecordBatch, 
        row: usize,
        converter: fn(&RecordBatch, usize) -> Result<Option<Self>>
    ) -> Result<Option<Self>> {
        converter(batch, row)
    }
    
    /// Create models from an entire registry batch using provided conversion function
    ///
    /// This method uses a registry-provided batch conversion function.
    fn from_registry_batch(
        batch: &RecordBatch,
        converter: fn(&RecordBatch) -> Result<Vec<Self>>
    ) -> Result<Vec<Self>> {
        converter(batch)
    }
    
    /// Enhance this model with data from a registry record
    ///
    /// Default implementation does nothing. Specific model types should override
    /// this method to add enhancement logic.
    fn enhance_with_registry_data(
        &mut self, 
        batch: &RecordBatch, 
        row: usize,
        enhancer: fn(&mut Self, &RecordBatch, usize) -> Result<bool>
    ) -> Result<bool> {
        enhancer(self, batch, row)
    }
}

/// Registry converter that can create and enhance models
///
/// This trait is implemented by registry modules to provide the conversion
/// and enhancement logic for specific registry types.
pub trait RegistryConverter<T: RegistryModel> {
    /// Convert a registry record to a model
    fn convert_record(&self, batch: &RecordBatch, row: usize) -> Result<Option<T>>;
    
    /// Convert an entire batch to models
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<T>>;
    
    /// Enhance an existing model with registry data
    fn enhance_model(&self, model: &mut T, batch: &RecordBatch, row: usize) -> Result<bool>;
    
    /// Apply additional transformations to models
    fn transform_models(&self, _models: &mut [T]) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Registry detection utilities
pub trait RegistryDetector {
    /// Detect registry type from batch schema
    fn detect_registry_type(batch: &RecordBatch) -> &'static str;
    
    /// Check if the model's ID matches the record's ID
    fn id_matches_record<T: RegistryModel>(
        model_id: &str, 
        batch: &RecordBatch, 
        row: usize, 
        id_field: &str
    ) -> Result<bool>;
}

/// Implementation of the RegistryModel trait for a model that supports multiple registries
pub trait MultiRegistryModel: RegistryModel {
    /// Enhance this model with data from any supported registry type
    ///
    /// This method detects the registry type and applies the appropriate enhancement.
    fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>;
    
    /// Check if this model's ID matches the ID in a registry record
    fn id_matches_record(&self, batch: &RecordBatch, row: usize) -> Result<bool>;
}