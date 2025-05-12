//! Unified IND schema definition
//!
//! This module provides a unified schema definition for the IND registry using
//! the centralized field definition system.

use crate::models::core::registry_traits::IndFields;
use crate::registry::field_definitions::CommonMappings;
use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create a IND-specific field definition
fn ind_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified IND registry schema
///
/// This function creates a schema for the IND registry using the unified field definition system.
#[must_use]
pub fn create_ind_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification field
        CommonMappings::pnr(),
        // IND-specific fields
        FieldMapping::new(
            ind_field("PERINDKIALT_13", "Annual income", FieldType::Decimal, true),
            Extractors::decimal("PERINDKIALT_13"),
            ModelSetters::f64_setter(|individual, value| {
                let ind_fields: &mut dyn IndFields = individual;
                ind_fields.set_annual_income(Some(value));
            }),
        ),
        FieldMapping::new(
            ind_field("LOENMV_13", "Employment income", FieldType::Decimal, true),
            Extractors::decimal("LOENMV_13"),
            ModelSetters::f64_setter(|individual, value| {
                let ind_fields: &mut dyn IndFields = individual;
                ind_fields.set_employment_income(Some(value));
            }),
        ),
        FieldMapping::new(
            ind_field("AAR", "Income year", FieldType::Integer, true),
            Extractors::integer("AAR"),
            ModelSetters::i32_setter(|individual, value| {
                let ind_fields: &mut dyn IndFields = individual;
                ind_fields.set_income_year(Some(value));
            }),
        ),
        FieldMapping::new(
            ind_field("CPRTJEK", "CPR check", FieldType::String, true),
            Extractors::string("CPRTJEK"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is not mapped to the Individual model
            }),
        ),
        FieldMapping::new(
            ind_field("CPRTYPE", "CPR type", FieldType::String, true),
            Extractors::string("CPRTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is not mapped to the Individual model
            }),
        ),
        FieldMapping::new(
            ind_field(
                "PRE_SOCIO",
                "Socioeconomic status code",
                FieldType::Integer,
                true,
            ),
            Extractors::integer("PRE_SOCIO"),
            ModelSetters::i32_setter(|individual, value| {
                //individual.socioeconomic_status_code = Some(value);
            }),
        ),
        FieldMapping::new(
            ind_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is not mapped to the Individual model
            }),
        ),
    ];

    create_registry_schema(
        "IND",
        "Indkomst registry containing income and tax information",
        field_mappings,
    )
}

/// Get the Arrow schema for IND data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn ind_schema() -> Arc<arrow::datatypes::Schema> {
    create_ind_schema().arrow_schema()
}
