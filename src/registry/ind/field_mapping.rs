//! Field mappings for IND registry deserialization
//!
//! This module defines the field mappings for the IND (Income) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for IND registry
#[must_use] pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // PNR mapping (required)
        FieldMapping::new(
            FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Annual income
        FieldMapping::new(
            FieldDefinition::new("PERINDKIALT_13", "annual_income", FieldType::Decimal, true),
            Extractors::decimal("PERINDKIALT_13"),
            ModelSetters::f64_setter(|individual, value| {
                individual.annual_income = Some(value);
            }),
        ),
        // Employment income
        FieldMapping::new(
            FieldDefinition::new("LOENMV_13", "employment_income", FieldType::Decimal, true),
            Extractors::decimal("LOENMV_13"),
            ModelSetters::f64_setter(|individual, value| {
                individual.employment_income = Some(value);
            }),
        ),
        // Version
        FieldMapping::new(
            FieldDefinition::new("VERSION", "version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|individual, value| {
                // Store version in properties map since there's no dedicated field
                if let version = value {
                    individual.store_property("version", Box::new(version));
                }
            }),
        ),
        // Year
        FieldMapping::new(
            FieldDefinition::new("YEAR", "income_year", FieldType::Integer, true),
            Extractors::integer("YEAR"),
            ModelSetters::i32_setter(|individual, value| {
                individual.income_year = Some(value);
            }),
        ),
    ]
}
