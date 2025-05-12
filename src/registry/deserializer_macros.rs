//! Macros for generating trait deserializers
//!
//! This module provides macros that generate trait-based deserializers
//! from unified schema definitions, eliminating repetitive code.

/// Macro to generate a trait deserializer from a schema
#[macro_export]
macro_rules! generate_trait_deserializer {
    ($registry:ident, $registry_type:expr, $schema_fn:expr) => {
        use arrow::record_batch::RecordBatch;
        use log::debug;
        use std::collections::HashMap;

        use crate::error::Result;
        use crate::models::core::Individual;
        use crate::registry::trait_deserializer::{RegistryDeserializer, RegistryFieldExtractor};

        pub struct $registry {
            field_extractors: Vec<Box<dyn RegistryFieldExtractor>>,
            field_map: HashMap<String, String>,
        }

        impl $registry {
            pub fn new() -> Self {
                // Get the unified schema
                let schema = $schema_fn();

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
                        crate::schema::FieldType::String
                        | crate::schema::FieldType::PNR
                        | crate::schema::FieldType::Category => {
                            // Create string extractor
                            let extractor = crate::registry::extractors::StringExtractor::new(
                                &source_field,
                                &target_field,
                                mapping.setter.clone(),
                            );
                            field_extractors.push(Box::new(extractor));
                        }
                        crate::schema::FieldType::Integer => {
                            // Create integer extractor
                            let extractor = crate::registry::extractors::IntegerExtractor::new(
                                &source_field,
                                &target_field,
                                mapping.setter.clone(),
                            );
                            field_extractors.push(Box::new(extractor));
                        }
                        crate::schema::FieldType::Decimal => {
                            // Create float extractor
                            let extractor = crate::registry::extractors::FloatExtractor::new(
                                &source_field,
                                &target_field,
                                mapping.setter.clone(),
                            );
                            field_extractors.push(Box::new(extractor));
                        }
                        crate::schema::FieldType::Date => {
                            // Create date extractor
                            let extractor = crate::registry::extractors::DateExtractor::new(
                                &source_field,
                                &target_field,
                                mapping.setter.clone(),
                            );
                            field_extractors.push(Box::new(extractor));
                        }
                        _ => {
                            // Skip other field types
                        }
                    }
                }

                Self {
                    field_extractors,
                    field_map,
                }
            }
        }

        impl RegistryDeserializer for $registry {
            fn registry_type(&self) -> &str {
                $registry_type
            }

            fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
                &self.field_extractors
            }

            fn field_mapping(&self) -> HashMap<String, String> {
                self.field_map.clone()
            }
        }

        /// Deserialize a record batch using the trait-based deserializer
        pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
            debug!(
                "Deserializing {} batch with trait-based deserializer",
                $registry_type
            );

            let deserializer = $registry::new();
            deserializer.deserialize_batch(batch)
        }

        /// Deserialize a single row from a record batch
        pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
            let deserializer = $registry::new();
            deserializer.deserialize_row(batch, row)
        }
    };
}
