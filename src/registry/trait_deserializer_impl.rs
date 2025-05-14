//! Implementation for generic registry deserializers
//!
//! This module provides a reusable implementation of the `RegistryDeserializer`
//! trait that can be used by procedural macros.

use std::collections::HashMap;

use crate::registry::trait_deserializer::{RegistryDeserializer, RegistryFieldExtractor};
use crate::schema::RegistrySchema;
use crate::schema::field_def::FieldType;
use crate::registry::extractors::{StringExtractor, IntegerExtractor, FloatExtractor, DateExtractor, Setter};

/// Generic implementation of a registry deserializer
#[derive(Debug)]
pub struct RegistryDeserializerImpl {
    registry_type: String,
    registry_desc: String,
    field_extractors: Vec<Box<dyn RegistryFieldExtractor>>,
    field_map: HashMap<String, String>,
}

impl RegistryDeserializerImpl {
    /// Create a new registry deserializer implementation
    pub fn new(registry_type: impl Into<String>, registry_desc: impl Into<String>, schema: RegistrySchema) -> Self {
        let registry_type = registry_type.into();
        let registry_desc = registry_desc.into();
        
        // Create field extractors from schema mappings
        let mut field_extractors: Vec<Box<dyn RegistryFieldExtractor>> = Vec::new();
        let mut field_map = HashMap::new();

        // Convert schema mappings to field extractors
        for mapping in &schema.field_mappings {
            let source_field = mapping.field_def.name.clone();
            let target_field = mapping.field_def.description.clone();

            // Add to field map
            field_map.insert(source_field.clone(), target_field.clone());

            // Create appropriate field extractor based on field type
            match &mapping.field_def.field_type {
                FieldType::String | FieldType::PNR | FieldType::Category => {
                    // Create string extractor
                    let extractor = StringExtractor::new(
                        &source_field,
                        &target_field,
                        Setter::new(mapping.setter.clone()),
                    );
                    field_extractors.push(Box::new(extractor));
                }
                FieldType::Integer => {
                    // Create integer extractor
                    let extractor = IntegerExtractor::new(
                        &source_field,
                        &target_field,
                        Setter::new(mapping.setter.clone()),
                    );
                    field_extractors.push(Box::new(extractor));
                }
                FieldType::Decimal => {
                    // Create float extractor
                    let extractor = FloatExtractor::new(
                        &source_field,
                        &target_field,
                        Setter::new(mapping.setter.clone()),
                    );
                    field_extractors.push(Box::new(extractor));
                }
                FieldType::Date => {
                    // Create date extractor
                    let extractor = DateExtractor::new(
                        &source_field,
                        &target_field,
                        Setter::new(mapping.setter.clone()),
                    );
                    field_extractors.push(Box::new(extractor));
                }
                _ => {
                    // Skip other field types
                }
            }
        }

        Self {
            registry_type,
            registry_desc,
            field_extractors,
            field_map,
        }
    }
}

impl RegistryDeserializer for RegistryDeserializerImpl {
    fn registry_type(&self) -> &str {
        &self.registry_type
    }

    fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
        &self.field_extractors
    }

    fn field_mapping(&self) -> HashMap<String, String> {
        self.field_map.clone()
    }
}