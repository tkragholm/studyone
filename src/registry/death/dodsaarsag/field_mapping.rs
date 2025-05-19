//! Field mappings for DODSAARSAG registry deserialization
//!
//! This module defines the field mappings for the DODSAARSAG (Cause of Death) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for DODSAARSAG registry
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
        // Death cause (ICD-10 code)
        FieldMapping::new(
            FieldDefinition::new("C_AARSAG", "death_cause", FieldType::String, true),
            Extractors::string("C_AARSAG"),
            ModelSetters::string_setter(|individual, value| {
                individual.death_cause = Some(value);
            }),
        ),
        // Death condition (ICD-10 code)
        FieldMapping::new(
            FieldDefinition::new("C_TILSTAND", "death_condition", FieldType::String, true),
            Extractors::string("C_TILSTAND"),
            ModelSetters::string_setter(|individual, value| {
                individual.underlying_death_cause = Some(value);
            }),
        ),
        // Death date
        FieldMapping::new(
            FieldDefinition::new("D_DATO", "death_date", FieldType::Date, true),
            Extractors::date("D_DATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.death_date = Some(value);
            }),
        ),
    ]
}
