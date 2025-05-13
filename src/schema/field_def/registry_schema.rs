//! Registry schema definition for the unified schema system
//!
//! This module provides a centralized registry schema definition system.

use super::mapping::FieldMapping;
use crate::models::core::Individual;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// A unified schema definition for a registry
#[derive(Clone)]
pub struct RegistrySchema {
    /// The registry name
    pub name: String,
    /// Description of the registry
    pub description: String,
    /// Field mappings from registry fields to Individual model
    pub field_mappings: Vec<FieldMapping>,
    /// Cached Arrow schema
    arrow_schema: Arc<Schema>,
}

impl RegistrySchema {
    /// Create a new registry schema
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        field_mappings: Vec<FieldMapping>,
    ) -> Self {
        // Create the Arrow schema from field definitions
        let fields: Vec<arrow::datatypes::Field> = field_mappings
            .iter()
            .map(|mapping| mapping.field_def.to_arrow_field())
            .collect();

        let arrow_schema = Arc::new(Schema::new(fields));

        Self {
            name: name.into(),
            description: description.into(),
            field_mappings,
            arrow_schema,
        }
    }

    /// Get the Arrow schema for this registry
    #[must_use] pub fn arrow_schema(&self) -> Arc<Schema> {
        self.arrow_schema.clone()
    }

    /// Deserialize a record batch row into an Individual
    #[must_use] pub fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Individual {
        // Create a new Individual with minimal required information
        let pnr = self
            .extract_pnr(batch, row)
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let _gender = crate::models::core::types::Gender::Unknown;
        let birth_date = None;

        let mut individual = Individual::new(pnr, birth_date);

        // Apply each field mapping
        for mapping in &self.field_mappings {
            mapping.apply(batch, row, &mut individual);
        }

        // Handle any post-processing
        individual.compute_rural_status();

        individual
    }

    /// Extract PNR from a record batch
    fn extract_pnr(&self, batch: &RecordBatch, row: usize) -> Option<String> {
        // Find the PNR field mapping
        for mapping in &self.field_mappings {
            if mapping.field_def.name == "PNR" {
                if let Some(value) = (mapping.extractor)(batch, row) {
                    // Try to downcast to String
                    if let Ok(pnr) = value.downcast::<String>() {
                        return Some(*pnr);
                    }
                }
            }
        }
        None
    }

    /// Deserialize a batch of records into Individuals
    #[must_use] pub fn deserialize_batch(&self, batch: &RecordBatch) -> Vec<Individual> {
        (0..batch.num_rows())
            .map(|row| self.deserialize_row(batch, row))
            .collect()
    }

    /// Get a field mapping by name
    #[must_use] pub fn get_field_mapping(&self, name: &str) -> Option<&FieldMapping> {
        self.field_mappings
            .iter()
            .find(|mapping| mapping.field_def.matches_name(name))
    }

    /// Check if this schema contains a field with the given name
    #[must_use] pub fn has_field(&self, name: &str) -> bool {
        self.field_mappings
            .iter()
            .any(|mapping| mapping.field_def.matches_name(name))
    }
}

/// Create a registry schema with the given name, description, and field mappings
pub fn create_registry_schema(
    name: impl Into<String>,
    description: impl Into<String>,
    field_mappings: Vec<FieldMapping>,
) -> RegistrySchema {
    RegistrySchema::new(name, description, field_mappings)
}
