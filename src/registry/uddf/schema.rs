//! Unified UDDF schema definition
//!
//! This module provides a unified schema definition for the UDDF registry using
//! the centralized field definition system.

use crate::registry::field_definitions::CommonMappings;
use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create a UDDF-specific field definition
fn uddf_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified UDDF registry schema
///
/// This function creates a schema for the UDDF registry using the unified field definition system.
#[must_use]
pub fn create_uddf_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification field
        CommonMappings::pnr(),
        // Education-specific fields
        FieldMapping::new(
            uddf_field("CPRTJEK", "CPR check", FieldType::String, true),
            Extractors::string("CPRTJEK"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            uddf_field("CPRTYPE", "CPR type", FieldType::String, true),
            Extractors::string("CPRTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            uddf_field(
                "HFAUDD",
                "Highest completed education code",
                FieldType::String,
                true,
            ),
            Extractors::string("HFAUDD"),
            ModelSetters::string_setter(|individual, value| {
                individual.education_code = Some(value);
            }),
        ),
        FieldMapping::new(
            uddf_field(
                "HF_KILDE",
                "Source of education information",
                FieldType::String,
                true,
            ),
            Extractors::string("HF_KILDE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            uddf_field("HF_VFRA", "Valid from date", FieldType::String, true),
            Extractors::string("HF_VFRA"),
            ModelSetters::string_setter(|individual, value| {
                individual.education_valid_from = Some(value);
            }),
        ),
        FieldMapping::new(
            uddf_field("HF_VTIL", "Valid to date", FieldType::String, true),
            Extractors::string("HF_VTIL"),
            ModelSetters::string_setter(|individual, value| {
                individual.education_valid_to = Some(value);
            }),
        ),
        FieldMapping::new(
            uddf_field("INSTNR", "Institution number", FieldType::Integer, true),
            Extractors::integer("INSTNR"),
            ModelSetters::i32_setter(|individual, value| {
                individual.education_institution = Some(value);
            }),
        ),
        FieldMapping::new(
            uddf_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "UDDF",
        "Uddannelse registry containing educational information",
        field_mappings,
    )
}

/// Get the Arrow schema for UDDF data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn uddf_schema() -> Arc<arrow::datatypes::Schema> {
    create_uddf_schema().arrow_schema()
}
