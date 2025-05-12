//! Unified AKM schema definition
//!
//! This module provides a unified schema definition for the AKM registry using
//! the centralized field definition system.

use crate::registry::field_definitions::CommonMappings;
use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create an AKM-specific field definition
fn akm_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified AKM registry schema
///
/// This function creates a schema for the AKM registry using the unified field definition system.
#[must_use]
pub fn create_akm_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification fields
        CommonMappings::pnr(),
        // Employment and socioeconomic fields
        CommonMappings::socioeconomic_status(),
        // AKM-specific fields
        FieldMapping::new(
            akm_field(
                "SOCIO02",
                "Socioeconomic status (2002 definition)",
                FieldType::Category,
                true,
            ),
            Extractors::integer("SOCIO02"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            akm_field(
                "SOCIO13",
                "Socioeconomic status (2013 definition)",
                FieldType::Category,
                true,
            ),
            Extractors::integer("SOCIO13"),
            ModelSetters::i32_setter(|individual, value| {
                individual.socioeconomic_status = Some(value);
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            akm_field("CPRTJEK", "CPR check", FieldType::String, true),
            Extractors::string("CPRTJEK"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            akm_field("CPRTYPE", "CPR type", FieldType::String, true),
            Extractors::string("CPRTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            akm_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            akm_field("SENR", "SE number", FieldType::String, true),
            Extractors::string("SENR"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "AKM",
        "Arbejdsklassifikationsmodulet registry containing employment information",
        field_mappings,
    )
}

/// Get the Arrow schema for AKM data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn akm_schema() -> Arc<arrow::datatypes::Schema> {
    create_akm_schema().arrow_schema()
}
