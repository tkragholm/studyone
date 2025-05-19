//! Module for unified registry field definitions
//!
//! This module provides a centralized system for defining registry fields,
//! their data types, and mappings to the Individual model.

mod adapt;
pub mod field;
mod macros;
pub mod mapping;
mod registry_schema;

pub use adapt::SchemaAdapter;
pub use field::{FieldDefinition, FieldType};
pub use mapping::{Extractors, FieldMapping, ModelSetter, ModelSetters};
pub use registry_schema::{RegistrySchema, create_registry_schema};

// Re-export the macros to make them available to users of this module
pub use crate::field_mapping;
pub use crate::registry_schema;
