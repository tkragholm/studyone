//! Field mappings for VNDS registry deserialization
//!
//! This module defines the field mappings for the VNDS (migration) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for VNDS registry
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
        // Event Type mapping
        FieldMapping::new(
            FieldDefinition::new("INDUD_KODE", "event_type", FieldType::String, true),
            Extractors::string("INDUD_KODE"),
            ModelSetters::string_setter(|individual, value| {
                individual.event_type = Some(value);
            }),
        ),
        // Event Date mapping
        FieldMapping::new(
            FieldDefinition::new("HAEND_DATO", "event_date", FieldType::Date, true),
            // IMPORTANT: Use the for_field method to get the correct extractor type based on field definition
            // This creates a DateExtractor for FieldType::Date fields instead of a StringExtractor
            Extractors::for_field(&FieldDefinition::new(
                "HAEND_DATO",
                "event_date",
                FieldType::Date,
                true,
            )),
            // Use a date setter with a direct mutation function
            // This avoids any potential issues with the string conversion
            ModelSetters::date_setter(|individual, date| {
                // Set the date directly on the individual
                individual.event_date = Some(date);
            }),
        ),
    ]
}
