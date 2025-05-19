//! Field mappings for MFR registry deserialization
//!
//! This module defines the field mappings for the MFR (Medical Birth Registry).

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for MFR registry
#[must_use] pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // PNR mapping (required)
        FieldMapping::new(
            FieldDefinition::new("CPR_BARN", "pnr", FieldType::PNR, false),
            Extractors::string("CPR_BARN"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Birth date
        FieldMapping::new(
            FieldDefinition::new("FOEDSELSDATO", "birth_date", FieldType::Date, true),
            Extractors::date("FOEDSELSDATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.birth_date = Some(value);
            }),
        ),
        // Mother's PNR
        FieldMapping::new(
            FieldDefinition::new("CPR_MODER", "mother_pnr", FieldType::String, true),
            Extractors::string("CPR_MODER"),
            ModelSetters::string_setter(|individual, value| {
                individual.mother_pnr = Some(value);
            }),
        ),
        // Father's PNR
        FieldMapping::new(
            FieldDefinition::new("CPR_FADER", "father_pnr", FieldType::String, true),
            Extractors::string("CPR_FADER"),
            ModelSetters::string_setter(|individual, value| {
                individual.father_pnr = Some(value);
            }),
        ),
    ]
}
