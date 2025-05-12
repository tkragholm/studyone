//! Integration with the existing type adaptation system
//!
//! This module provides integration between the unified field definition system
//! and the existing type adaptation system.

use arrow::record_batch::RecordBatch;
use crate::schema::adapt::{
    DateFormatConfig, SchemaAdaptation, adapt_record_batch, check_schema_with_adaptation
};
use crate::schema::field_def::RegistrySchema;
use crate::error::Result;

/// Adapter for registry schemas to the existing type adaptation system
pub struct SchemaAdapter {
    /// Date format configuration
    date_config: DateFormatConfig,
}

impl Default for SchemaAdapter {
    fn default() -> Self {
        Self {
            date_config: DateFormatConfig::default(),
        }
    }
}

impl SchemaAdapter {
    /// Create a new schema adapter
    pub fn new(date_config: DateFormatConfig) -> Self {
        Self { date_config }
    }
    
    /// Check if a batch is compatible with a registry schema
    pub fn check_compatibility(
        &self,
        batch: &RecordBatch,
        registry_schema: &RegistrySchema,
    ) -> crate::schema::adapt::EnhancedSchemaCompatibilityReport {
        let target_schema = registry_schema.arrow_schema();
        check_schema_with_adaptation(&batch.schema(), &target_schema)
    }
    
    /// Adapt a batch to match a registry schema
    pub fn adapt_batch(
        &self,
        batch: &RecordBatch,
        registry_schema: &RegistrySchema,
    ) -> Result<RecordBatch> {
        let target_schema = registry_schema.arrow_schema();
        adapt_record_batch(batch, &target_schema, &self.date_config)
            .map_err(|e| anyhow::anyhow!("Failed to adapt record batch: {}", e))
    }
    
    /// Analyze a batch and create a compatible registry schema
    pub fn analyze_batch(
        &self,
        batch: &RecordBatch,
        original_schema: &RegistrySchema,
    ) -> (RegistrySchema, Vec<SchemaAdaptation>) {
        let source_schema = batch.schema();
        let target_schema = original_schema.arrow_schema();
        
        // Check compatibility and get adaptation information
        let compatibility = check_schema_with_adaptation(&source_schema, &target_schema);
        
        // If fully compatible, return the original schema
        if compatibility.compatible && compatibility.adaptations.is_empty() {
            return (original_schema.clone(), Vec::new());
        }
        
        // Otherwise, return the original schema and the adaptations needed
        (original_schema.clone(), compatibility.adaptations)
    }
}