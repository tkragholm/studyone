//! Unified VNDS schema definition
//!
//! This module provides a unified schema definition for the VNDS registry using
//! the centralized field definition system.

use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create a VNDS-specific field definition
fn vnds_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
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
        FieldMapping::new(
            vnds_field("PNR", "Unique identifier", FieldType::String, false),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Migration-specific fields
        FieldMapping::new(
            vnds_field(
                "INDUD_KODE",
                "Migration code (in/out)",
                FieldType::String,
                true,
            ),
            Extractors::string("INDUD_KODE"),
            ModelSetters::string_setter(|individual, value| {
                let migration_type = match value.as_str() {
                    "1" => "IN",  // Immigration
                    "2" => "OUT", // Emigration
                    _ => value.as_str(),
                };
                individual.event_type = Some(migration_type.to_string());
            }),
        ),
        FieldMapping::new(
            vnds_field("HAEND_DATO", "Event date", FieldType::Date, true),
            Extractors::date("HAEND_DATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.event_date = Some(value);
            }),
        ),
    ];

    create_registry_schema(
        "VNDS",
        "Vandringer/Migration registry containing migration information",
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
