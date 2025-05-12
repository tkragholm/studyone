//! Unified VNDS schema definition
//!
//! This module provides a unified schema definition for the VNDS registry using
//! the centralized field definition system.

use std::sync::Arc;
use crate::schema::{RegistrySchema, create_registry_schema, FieldDefinition, FieldType};
use crate::schema::field_def::{FieldMapping, ModelSetters, Extractors};
use crate::registry::field_definitions::CommonMappings;

/// Create a VNDS-specific field definition
fn vnds_field(name: &str, description: &str, field_type: FieldType, nullable: bool) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified VNDS registry schema
///
/// This function creates a schema for the VNDS registry using the unified field definition system.
#[must_use]
pub fn create_vnds_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification field
        CommonMappings::pnr(),
        
        // Migration-specific fields
        FieldMapping::new(
            vnds_field("INDUD_KODE", "Migration code (in/out)", FieldType::String, true),
            Extractors::string("INDUD_KODE"),
            ModelSetters::string_setter(|individual, value| {
                let migration_type = match value.as_str() {
                    "1" => "IN", // Immigration
                    "2" => "OUT", // Emigration
                    _ => value.as_str(),
                };
                individual.migration_type = Some(migration_type.to_string());
            }),
        ),
        FieldMapping::new(
            vnds_field("HAEND_DATO", "Event date", FieldType::String, true),
            Extractors::string("HAEND_DATO"),
            ModelSetters::string_setter(|individual, value| {
                // Note: In a real implementation, we would need to parse the date string
                // and convert it to a proper date. For simplicity, we're just storing the
                // raw string value here.
                individual.migration_date = Some(value);
            }),
        ),
    ];
    
    create_registry_schema(
        "VNDS",
        "Vandringer/Migration registry containing migration information",
        field_mappings,
    )
}

/// Create the unified standardized VNDS registry schema
/// 
/// This schema is used for standardized VNDS data where dates have been converted to Date32 format
/// and migration codes have been translated to "IN" or "OUT"
#[must_use]
pub fn create_vnds_standardized_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification field
        CommonMappings::pnr(),
        
        // Standardized migration fields
        FieldMapping::new(
            vnds_field("MIGRATION_TYPE", "Migration type (IN or OUT)", FieldType::String, true),
            Extractors::string("MIGRATION_TYPE"),
            ModelSetters::string_setter(|individual, value| {
                individual.migration_type = Some(value);
            }),
        ),
        FieldMapping::new(
            vnds_field("MIGRATION_DATE", "Migration date", FieldType::Date, true),
            Extractors::date("MIGRATION_DATE"),
            ModelSetters::date_setter(|individual, value| {
                // In a real implementation, we would store this as a proper date
                // For now, we convert it to string to match the current model
                let date_str = format!("{}", value);
                individual.migration_date = Some(date_str);
            }),
        ),
    ];
    
    create_registry_schema(
        "VNDS_STANDARDIZED",
        "Standardized Vandringer/Migration registry containing migration information",
        field_mappings,
    )
}

/// Get the Arrow schema for VNDS data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn vnds_schema() -> Arc<arrow::datatypes::Schema> {
    create_vnds_schema().arrow_schema()
}

/// Get the Arrow schema for standardized VNDS data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn vnds_standardized_schema() -> Arc<arrow::datatypes::Schema> {
    create_vnds_standardized_schema().arrow_schema()
}