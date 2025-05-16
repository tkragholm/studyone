//! Direct Individual deserializer
//!
//! This module provides a deserializer that directly maps registry data to Individual models
//! without requiring intermediate registry-specific structs.

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::trait_deserializer::{RegistryDeserializer, RegistryFieldExtractor};
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};
use crate::schema::{RegistrySchema, create_registry_schema, field_def::FieldMapping};

/// Individual deserializer for a specific registry without intermediate struct
#[derive(Debug)]
pub struct DirectIndividualDeserializer {
    inner: Arc<dyn RegistryDeserializer>,
}

impl DirectIndividualDeserializer {
    /// Create a new deserializer for the specified registry type
    ///
    /// # Arguments
    ///
    /// * `registry_name` - The registry name (e.g., "VNDS", "BEF", etc.)
    ///
    /// # Returns
    ///
    /// A new instance of the deserializer
    pub fn new(registry_name: &str) -> Self {
        // Create a registry-specific schema based on the registry name
        let schema = match registry_name {
            "VNDS" => Self::create_vnds_schema(),
            "BEF" => Self::create_bef_schema(),
            _ => Self::create_default_schema(registry_name),
        };

        // Create a deserializer implementation with the schema
        let inner = Arc::new(
            crate::registry::trait_deserializer_impl::RegistryDeserializerImpl::new(
                registry_name,
                format!("{} registry", registry_name),
                schema,
                Some("pnr"),
            ),
        );

        Self { inner }
    }

    /// Create VNDS migration registry schema
    fn create_vnds_schema() -> RegistrySchema {
        let field_mappings = vec![
            // PNR mapping (required)
            FieldMapping::new(
                FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
                Extractors::string("PNR"),
                ModelSetters::string_setter(|individual, value| {
                    individual.pnr = value;
                }),
            ),
            // Event Type mapping
            FieldMapping::new(
                FieldDefinition::new("INDUD_KODE", "event_type", FieldType::String, true),
                Extractors::string("INDUD_KODE"),
                ModelSetters::string_setter(|individual, value| {
                    // println!("Setting event_type from VNDS: {}", value);
                    individual.event_type = Some(value);
                }),
            ),
            // Event Date mapping
            FieldMapping::new(
                FieldDefinition::new("HAEND_DATO", "event_date", FieldType::Date, true),
                // IMPORTANT: Use the for_field method to get the correct extractor type based on field definition
                // This creates a DateExtractor for FieldType::Date fields instead of a StringExtractor
                Extractors::for_field(&FieldDefinition::new("HAEND_DATO", "event_date", FieldType::Date, true)),
                
                // Use a date setter with a direct mutation function
                // This avoids any potential issues with the string conversion
                ModelSetters::date_setter(|individual, date| {
                    // Set the date directly on the individual
                    individual.event_date = Some(date);
                    println!("Set event_date directly to Some({}) on individual", date);
                }),
            ),
        ];

        create_registry_schema("VNDS", "VNDS Migration registry", field_mappings)
    }

    /// Create BEF population registry schema
    fn create_bef_schema() -> RegistrySchema {
        let field_mappings = vec![
            // PNR mapping (required)
            FieldMapping::new(
                FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
                Extractors::string("PNR"),
                ModelSetters::string_setter(|individual, value| {
                    individual.pnr = value;
                }),
            ),
            // Gender mapping
            FieldMapping::new(
                FieldDefinition::new("KOEN", "gender", FieldType::String, true),
                Extractors::string("KOEN"),
                ModelSetters::string_setter(|individual, value| {
                    println!("Setting gender from BEF: {}", value);
                    individual.gender = Some(value);
                }),
            ),
            // Add more BEF fields as needed...
        ];

        create_registry_schema("BEF", "BEF Population registry", field_mappings)
    }

    /// Create default schema for any registry type
    fn create_default_schema(registry_name: &str) -> RegistrySchema {
        let field_mappings = vec![
            // PNR mapping (required for most registries)
            FieldMapping::new(
                FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
                Extractors::string("PNR"),
                ModelSetters::string_setter(|individual, value| {
                    individual.pnr = value;
                }),
            ),
        ];

        create_registry_schema(
            registry_name,
            format!("{} registry", registry_name),
            field_mappings,
        )
    }

    /// Deserialize a record batch directly into a vector of Individual models
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow record batch to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of deserialized Individuals
    pub fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        self.inner.deserialize_batch(batch)
    }

    /// Deserialize a single row from a record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch
    /// * `row` - The row index to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing an Option with the deserialized Individual
    pub fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
        self.inner.deserialize_row(batch, row)
    }

    /// Get field extractors used by this deserializer
    pub fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
        self.inner.field_extractors()
    }

    /// Get field name mapping
    pub fn field_mapping(&self) -> HashMap<String, String> {
        self.inner.field_mapping()
    }
}
