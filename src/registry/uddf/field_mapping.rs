//! Field mappings for UDDF registry deserialization
//!
//! This module defines the field mappings for the UDDF (Education) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for UDDF registry
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
        // CPR check
        FieldMapping::new(
            FieldDefinition::new("CPRTJEK", "cpr_check", FieldType::String, true),
            Extractors::string("CPRTJEK"),
            ModelSetters::string_setter(|individual, value| {
                // Store CPR check in properties map since there's no dedicated field
                if let cpr_check = value.as_str() {
                    individual.store_property("cpr_check", Box::new(cpr_check.to_string()));
                }
            }),
        ),
        // CPR type
        FieldMapping::new(
            FieldDefinition::new("CPRTYPE", "cpr_type", FieldType::String, true),
            Extractors::string("CPRTYPE"),
            ModelSetters::string_setter(|individual, value| {
                // Store CPR type in properties map since there's no dedicated field
                if let cpr_type = value.as_str() {
                    individual.store_property("cpr_type", Box::new(cpr_type.to_string()));
                }
            }),
        ),
        // Highest education
        FieldMapping::new(
            FieldDefinition::new("HFAUDD", "education_code", FieldType::Integer, true),
            Extractors::integer("HFAUDD"),
            ModelSetters::i32_setter(|individual, value| {
                individual.education_code = Some(value as u16);
            }),
        ),
        // Education source
        FieldMapping::new(
            FieldDefinition::new("HF_KILDE", "education_source", FieldType::String, true),
            Extractors::string("HF_KILDE"),
            ModelSetters::string_setter(|individual, value| {
                // Convert source from string to u8 if possible
                if let source = value.as_str() {
                    if let Ok(source_num) = source.parse::<u8>() {
                        individual.education_source = Some(source_num);
                    }
                }
            }),
        ),
        // Valid from date
        FieldMapping::new(
            FieldDefinition::new("HF_VFRA", "education_valid_from", FieldType::Date, true),
            Extractors::date("HF_VFRA"),
            ModelSetters::date_setter(|individual, value| {
                individual.education_valid_from = Some(value);
            }),
        ),
        // Valid to date
        FieldMapping::new(
            FieldDefinition::new("HF_VTIL", "education_valid_to", FieldType::Date, true),
            Extractors::date("HF_VTIL"),
            ModelSetters::date_setter(|individual, value| {
                individual.education_valid_to = Some(value);
            }),
        ),
        // Institution number
        FieldMapping::new(
            FieldDefinition::new("INSTNR", "education_institution", FieldType::Integer, true),
            Extractors::integer("INSTNR"),
            ModelSetters::i32_setter(|individual, value| {
                individual.education_institution = Some(value);
            }),
        ),
        // Version
        FieldMapping::new(
            FieldDefinition::new("VERSION", "version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|individual, value| {
                // Store version in properties map since there's no dedicated field
                if let version = value.as_str() {
                    individual.store_property("uddf_version", Box::new(version.to_string()));
                }
            }),
        ),
    ]
}
