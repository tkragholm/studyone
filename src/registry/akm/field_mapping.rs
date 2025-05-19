//! Field mappings for AKM registry deserialization
//!
//! This module defines the field mappings for the AKM (labour) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for AKM registry
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
        // Socioeconomic status mapping (needs special handling for String to i32 conversion)
        FieldMapping::new(
            FieldDefinition::new("SOCIO13", "socioeconomic_status", FieldType::Integer, true),
            Extractors::integer("SOCIO13"),
            ModelSetters::i32_setter(|individual, value| {
                individual.socioeconomic_status = Some(value);
            }),
        ),
        // Add additional AKM fields as needed...
    ]
}
