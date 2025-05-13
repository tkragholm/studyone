//! Macros for generating trait deserializers
//!
//! This module provides macros that generate trait-based deserializers
//! from unified schema definitions, eliminating repetitive code.

/// Macro to generate a trait deserializer from a schema
#[macro_export]
macro_rules! generate_trait_deserializer {
    ($registry:ident, $registry_type:expr, $schema_fn:expr) => {
        // Avoid importing items at the module level to prevent collisions
        pub struct $registry {
            field_extractors: Vec<Box<dyn $crate::registry::trait_deserializer::RegistryFieldExtractor>>,
            field_map: std::collections::HashMap<String, String>,
        }

        impl Default for $registry {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $registry {
            #[must_use] pub fn new() -> Self {
                // Get the unified schema
                let schema = $schema_fn();

                // Create field extractors from schema mappings
                let mut field_extractors: Vec<Box<dyn $crate::registry::trait_deserializer::RegistryFieldExtractor>> = Vec::new();
                let mut field_map = std::collections::HashMap::new();

                // Convert schema mappings to field extractors
                for mapping in &schema.field_mappings {
                    let source_field = mapping.field_def.name.clone();
                    let target_field = mapping.field_def.description.clone();

                    // Add to field map
                    field_map.insert(source_field.clone(), target_field.clone());

                    // Create appropriate field extractor based on field type
                    match &mapping.field_def.field_type {
                        $crate::schema::FieldType::String
                        | $crate::schema::FieldType::PNR
                        | $crate::schema::FieldType::Category => {
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

            /// Deserialize a record batch using this deserializer
            pub fn deserialize_batch(&self, batch: &arrow::record_batch::RecordBatch) -> crate::error::Result<Vec<crate::models::core::Individual>> {
                log::debug!(
                    "Deserializing {} batch with trait-based deserializer",
                    $registry_type
                );

                self.deserialize_batch_impl(batch)
            }

            /// Deserialize a single row from a record batch using this deserializer
            pub fn deserialize_row(&self, batch: &arrow::record_batch::RecordBatch, row: usize) -> crate::error::Result<Option<crate::models::core::Individual>> {
                self.deserialize_row_impl(batch, row)
            }

            // Implementation methods that delegate to the trait
            fn deserialize_batch_impl(&self, batch: &arrow::record_batch::RecordBatch) -> crate::error::Result<Vec<crate::models::core::Individual>> {
                <Self as crate::registry::trait_deserializer::RegistryDeserializer>::deserialize_batch(self, batch)
            }

            fn deserialize_row_impl(&self, batch: &arrow::record_batch::RecordBatch, row: usize) -> crate::error::Result<Option<crate::models::core::Individual>> {
                <Self as crate::registry::trait_deserializer::RegistryDeserializer>::deserialize_row(self, batch, row)
            }
        }

        impl crate::registry::trait_deserializer::RegistryDeserializer for $registry {
            fn registry_type(&self) -> &str {
                $registry_type
            }

            fn field_extractors(&self) -> &[Box<dyn crate::registry::trait_deserializer::RegistryFieldExtractor>] {
                &self.field_extractors
            }

            fn field_mapping(&self) -> std::collections::HashMap<String, String> {
                self.field_map.clone()
            }
        }
    };
}
