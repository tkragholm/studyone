//! Generic registry deserializer
//!
//! This module provides a generic deserializer that works with the unified schema system.
//! It provides a standardized way to deserialize registry data based on field definitions.

use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::schema::RegistrySchema;
use crate::schema::adapt::{DateFormatConfig, adapt_record_batch};
use log::debug;

/// Generic deserializer for registry data
#[derive(Clone)]
pub struct GenericDeserializer {
    /// The registry schema used for deserialization
    registry_schema: RegistrySchema,
    /// Caching the field mapping for faster lookup
    field_mapping: HashMap<String, String>,
    /// Date format configuration for adaptation
    date_config: DateFormatConfig,
}

impl GenericDeserializer {
    /// Create a new generic deserializer
    pub fn new(registry_schema: RegistrySchema) -> Self {
        // Create field mapping
        let mut field_mapping = HashMap::new();
        for mapping in &registry_schema.field_mappings {
            field_mapping.insert(
                mapping.field_def.name.clone(), 
                mapping.field_def.name.clone()
            );
            
            // Add mappings for aliases
            for alias in &mapping.field_def.aliases {
                field_mapping.insert(
                    alias.clone(),
                    mapping.field_def.name.clone()
                );
            }
        }
        
        Self {
            registry_schema,
            field_mapping,
            date_config: DateFormatConfig::default(),
        }
    }
    
    /// Set a custom date format configuration
    pub fn with_date_config(mut self, date_config: DateFormatConfig) -> Self {
        self.date_config = date_config;
        self
    }
    
    /// Get the registry schema
    pub fn registry_schema(&self) -> &RegistrySchema {
        &self.registry_schema
    }
    
    /// Get the Arrow schema for this registry
    pub fn arrow_schema(&self) -> Arc<Schema> {
        self.registry_schema.arrow_schema()
    }
    
    /// Create a mapped batch with standardized field names for deserialization
    fn create_mapped_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();

        // Create new fields and columns with mapped names
        for field_idx in 0..schema.fields().len() {
            let field = schema.field(field_idx);
            let column = batch.column(field_idx);
            let field_name = field.name();

            if let Some(mapped_name) = self.field_mapping.get(field_name) {
                // Use the mapped name
                new_fields.push(Field::new(
                    mapped_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            } else {
                // If no mapping exists, keep the original name
                new_fields.push(Field::new(
                    field_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
            new_columns.push(column.clone());
        }

        // Create new schema and record batch with mapped field names
        let new_schema = Arc::new(Schema::new(new_fields));
        let new_batch = RecordBatch::try_new(new_schema, new_columns)?;

        Ok(new_batch)
    }
    
    /// Adapt the batch to match the registry schema
    pub fn adapt_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // First create a mapped batch with standardized field names
        let mapped_batch = self.create_mapped_batch(batch)?;
        
        // Then adapt the batch to match the registry schema
        adapt_record_batch(
            &mapped_batch, 
            &self.registry_schema.arrow_schema(),
            &self.date_config
        ).map_err(|e| anyhow::anyhow!("Failed to adapt record batch: {}", e))
    }
    
    /// Deserialize a single row from a record batch
    pub fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
        if row >= batch.num_rows() {
            return Ok(None);
        }
        
        // Adapt the batch to match the registry schema
        let adapted_batch = self.adapt_batch(batch)?;
        
        // Deserialize using the registry schema
        let individual = self.registry_schema.deserialize_row(&adapted_batch, row);
        
        Ok(Some(individual))
    }
    
    /// Deserialize a batch of records
    pub fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        debug!("Deserializing batch of {} rows", batch.num_rows());
        
        // Adapt the batch to match the registry schema
        let adapted_batch = self.adapt_batch(batch)?;
        
        // Deserialize using the registry schema
        let individuals = self.registry_schema.deserialize_batch(&adapted_batch);
        
        Ok(individuals)
    }
}