//! Unified BEF schema definition
//!
//! This module provides a unified schema definition for the BEF registry using
//! the centralized field definition system.

use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create a BEF-specific field definition
fn bef_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified BEF registry schema
///
/// This function creates a schema for the BEF registry using the unified field definition system.
#[must_use]
pub fn create_bef_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification field
        FieldMapping::new(
            bef_field("PNR", "Unique identifier", FieldType::String, false),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // BEF-specific fields
        FieldMapping::new(
            bef_field(
                "AEGTE_ID",
                "Spouse's personal identification number",
                FieldType::PNR,
                true,
            ),
            Extractors::string("AEGTE_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.spouse_pnr = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("ALDER", "Age in years", FieldType::Integer, true),
            Extractors::integer("ALDER"),
            ModelSetters::i32_setter(|individual, value| {
                individual.age = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field(
                "ANTBOERNF",
                "Number of children in family",
                FieldType::Integer,
                true,
            ),
            Extractors::integer("ANTBOERNF"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field(
                "ANTBOERNH",
                "Number of children in household",
                FieldType::Integer,
                true,
            ),
            Extractors::integer("ANTBOERNH"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field(
                "ANTPERSF",
                "Number of persons in family",
                FieldType::Integer,
                true,
            ),
            Extractors::integer("ANTPERSF"),
            ModelSetters::i32_setter(|individual, value| {
                individual.family_size = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field(
                "ANTPERSH",
                "Number of persons in household",
                FieldType::Integer,
                true,
            ),
            Extractors::integer("ANTPERSH"),
            ModelSetters::i32_setter(|individual, value| {
                individual.household_size = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("BOP_VFRA", "Date of residence from", FieldType::Date, true),
            Extractors::date("BOP_VFRA"),
            ModelSetters::date_setter(|individual, value| {
                individual.residence_from = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("CPRTJEK", "CPR check", FieldType::Integer, true),
            Extractors::integer("CPRTJEK"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field("CPRTYPE", "CPR type", FieldType::Integer, true),
            Extractors::integer("CPRTYPE"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field(
                "E_FAELLE_ID",
                "Registered partner PNR",
                FieldType::PNR,
                true,
            ),
            Extractors::string("E_FAELLE_ID"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field("FAMILIE_TYPE", "Family type", FieldType::Integer, true),
            Extractors::integer("FAMILIE_TYPE"),
            ModelSetters::i32_setter(|individual, value| {
                individual.family_type = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("FM_MARK", "Family marker", FieldType::Integer, true),
            Extractors::integer("FM_MARK"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field(
                "IE_TYPE",
                "Immigration/emigration type",
                FieldType::String,
                true,
            ),
            Extractors::string("IE_TYPE"),
            ModelSetters::string_setter(|individual, value| {
                individual.immigration_type = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("PLADS", "Position in family", FieldType::Integer, true),
            Extractors::integer("PLADS"),
            ModelSetters::i32_setter(|individual, value| {
                individual.position_in_family = Some(value);
            }),
        ),
        FieldMapping::new(
            bef_field("REG", "Registration type", FieldType::Integer, true),
            Extractors::integer("REG"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            bef_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "BEF",
        "Befolkning registry containing population demographic information",
        field_mappings,
    )
}

/// Get the Arrow schema for BEF data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn bef_schema() -> Arc<arrow::datatypes::Schema> {
    create_bef_schema().arrow_schema()
}
