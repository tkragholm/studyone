//! Macros for generating registry-specific code
//!
//! This module contains macros for generating registry-specific code from
//! simple struct definitions.

/// Generate a registry deserializer from a struct-like definition
///
/// This macro provides a more structured way to define registry schemas
/// and deserializers.
///
/// # Example
///
/// ```rust
/// use par_reader::registry_macros::define_registry;
/// use par_reader::schema::FieldType;
/// use chrono::NaiveDate;
///
/// define_registry! {
///     name: "VNDS",
///     description: "Migration registry containing migration information",
///     struct VndsRegistry {
///         #[field(name = "PNR", nullable = false)]
///         pnr: String,
///
///         #[field(name = "INDUD_KODE", nullable = true)]
///         event_type: Option<String>,
///
///         #[field(name = "HAEND_DATO", nullable = true)]
///         event_date: Option<NaiveDate>,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_registry {
    (
        name: $registry_name:expr,
        description: $registry_desc:expr,
        struct $struct_name:ident {
            $(
                $(#[$field_meta:meta])*
                #[field(name = $field_source:expr $(, nullable = $nullable:expr)?)]
                $field_name:ident: $field_type:ty
            ),* $(,)?
        }
    ) => {
        /// Registry schema definition
        pub struct $struct_name;

        /// Auto-generated deserializer for the registry
        #[derive(Debug)]
        pub struct $struct_name Deserializer {
            inner: std::sync::Arc<dyn $crate::registry::trait_deserializer::RegistryDeserializer + Send + Sync + std::fmt::Debug>,
        }

        impl $struct_name Deserializer {
            /// Create a new deserializer instance
            #[must_use]
            pub fn new() -> Self {
                let schema = Self::create_schema();

                // Create the deserializer using our existing macro
                let deserializer = $crate::generate_trait_deserializer!($struct_name, $registry_name, || schema);
                struct DeserializerDebugWrapper<T>(T);
                
                impl<T: $crate::registry::trait_deserializer::RegistryDeserializer + Send + Sync> std::fmt::Debug for DeserializerDebugWrapper<T> {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(f, "RegistryDeserializer({})", self.0.registry_type())
                    }
                }
                
                impl<T: $crate::registry::trait_deserializer::RegistryDeserializer + Send + Sync> $crate::registry::trait_deserializer::RegistryDeserializer for DeserializerDebugWrapper<T> {
                    fn registry_type(&self) -> &str {
                        self.0.registry_type()
                    }
                
                    fn field_extractors(&self) -> &[Box<dyn $crate::registry::trait_deserializer::RegistryFieldExtractor>] {
                        self.0.field_extractors()
                    }
                
                    fn field_mapping(&self) -> std::collections::HashMap<String, String> {
                        self.0.field_mapping()
                    }
                }

                let inner = std::sync::Arc::new(DeserializerDebugWrapper(deserializer));

                Self { inner }
            }

            /// Create the registry schema
            fn create_schema() -> $crate::schema::RegistrySchema {
                // Define field mappings
                let field_mappings = vec![
                    $(
                        {
                            // Define field parameters based on the field type
                            let (field_type, extractor_fn, setter_fn) = $crate::registry::registry_macros::get_field_info::<$field_type>();

                            // Create field mapping
                            $crate::schema::FieldMapping::new(
                                $crate::schema::FieldDefinition::new(
                                    $field_source,
                                    stringify!($field_name),
                                    field_type,
                                    $crate::registry::registry_macros::is_optional::<$field_type>(),
                                ),
                                extractor_fn($field_source),
                                setter_fn(|individual, value| {
                                    $crate::registry::registry_macros::set_field::<$field_type>(&mut individual.$field_name, value);
                                }),
                            )
                        }
                    ),*
                ];

                $crate::schema::create_registry_schema(
                    $registry_name,
                    $registry_desc,
                    field_mappings
                )
            }

            /// Deserialize a record batch
            #[must_use]
            pub fn deserialize_batch(&self, batch: &arrow::record_batch::RecordBatch) 
                -> $crate::error::Result<Vec<$crate::models::core::Individual>> {
                self.inner.deserialize_batch(batch)
            }

            /// Deserialize a single row
            #[must_use]
            pub fn deserialize_row(&self, batch: &arrow::record_batch::RecordBatch, row: usize)
                -> $crate::error::Result<Option<$crate::models::core::Individual>> {
                self.inner.deserialize_row(batch, row)
            }
        }

        impl Default for $struct_name Deserializer {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

/// Helper functions for the `define_registry` macro
pub mod helper {
    use crate::models::core::Individual;
    use crate::schema::field_def::{Extractors, ModelSetters};
    use crate::schema::FieldType;
    use std::any::Any;
    use std::marker::PhantomData;
    use std::sync::Arc;

    /// Get field information based on the field type
    #[must_use]
    pub fn get_field_info<T: 'static>() -> (FieldType, fn(&str) -> Arc<dyn Fn(&arrow::record_batch::RecordBatch, usize) -> Option<Box<dyn Any>> + Send + Sync>, fn(fn(&mut Individual, T) -> ()) -> Arc<dyn Fn(&mut dyn Any, Box<dyn Any>) + Send + Sync>) {
        let phantom: PhantomData<T> = PhantomData;

        // Match on the type to determine appropriate field info
        // This is a simplified approach - in a real implementation we would
        // need to use more sophisticated type introspection
        match std::any::type_name::<T>() {
            "std::string::String" => (FieldType::String, Extractors::string, ModelSetters::string_setter),
            "alloc::string::String" => (FieldType::String, Extractors::string, ModelSetters::string_setter),
            "chrono::naive::date::NaiveDate" => (FieldType::Date, Extractors::date, ModelSetters::date_setter),
            _ if std::any::type_name::<T>().contains("i32") => (FieldType::Integer, Extractors::integer, ModelSetters::i32_setter),
            _ if std::any::type_name::<T>().contains("f64") => (FieldType::Decimal, Extractors::decimal, ModelSetters::f64_setter),
            _ => (FieldType::String, Extractors::string, ModelSetters::string_setter),
        }
    }

    /// Check if a type is an Option<T>
    #[must_use]
    pub const fn is_optional<T: 'static>() -> bool {
        std::any::type_name::<T>().starts_with("core::option::Option")
    }

    /// Set a field value, handling Option types
    pub fn set_field<T: 'static>(field: &mut T, value: Box<dyn Any>) {
        // This is a simplified approach - in a real implementation we would
        // need to use more sophisticated type handling
        if is_optional::<T>() {
            // For Option types, we would need to unwrap the Box and convert it to the inner type
            // This is a placeholder implementation
        } else {
            // For non-Option types, we can directly assign the value
            // This is a placeholder implementation
        }
    }
}