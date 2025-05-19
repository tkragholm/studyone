//! Field mappings for DOD registry deserialization
//!
//! This module defines the field mappings for the DOD (Death) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for DOD registry
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
        // Death date
        FieldMapping::new(
            FieldDefinition::new("DODDATO", "death_date", FieldType::Date, true),
            Extractors::date("DODDATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.death_date = Some(value);
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
        // Underlying death cause
        FieldMapping::new(
            FieldDefinition::new(
                "C_TILSTAND",
                "underlying_death_cause",
                FieldType::String,
                true,
            ),
            Extractors::string("C_TILSTAND"),
            ModelSetters::string_setter(|individual, value| {
                individual.underlying_death_cause = Some(value);
            }),
        ),
    ]
}
