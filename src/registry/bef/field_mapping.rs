//! Field mappings for BEF registry deserialization
//!
//! This module defines the field mappings for the BEF (population) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for BEF registry
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
        // Gender mapping
        FieldMapping::new(
            FieldDefinition::new("KOEN", "gender", FieldType::String, true),
            Extractors::string("KOEN"),
            ModelSetters::string_setter(|individual, value| {
                individual.gender = Some(value);
            }),
        ),
        // Birth date mapping
        FieldMapping::new(
            FieldDefinition::new("FOED_DAG", "birth_date", FieldType::Date, true),
            Extractors::for_field(&FieldDefinition::new(
                "FOED_DAG",
                "birth_date",
                FieldType::Date,
                true,
            )),
            ModelSetters::date_setter(|individual, date| {
                individual.birth_date = Some(date);
            }),
        ),
        // Mother's PNR mapping
        FieldMapping::new(
            FieldDefinition::new("MOR_ID", "mother_pnr", FieldType::PNR, true),
            Extractors::string("MOR_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.mother_pnr = Some(value);
            }),
        ),
        // Father's PNR mapping
        FieldMapping::new(
            FieldDefinition::new("FAR_ID", "father_pnr", FieldType::PNR, true),
            Extractors::string("FAR_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.father_pnr = Some(value);
            }),
        ),
        // Family ID mapping
        FieldMapping::new(
            FieldDefinition::new("FAMILIE_ID", "family_id", FieldType::String, true),
            Extractors::string("FAMILIE_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.family_id = Some(value);
            }),
        ),
        // Spouse's PNR mapping
        FieldMapping::new(
            FieldDefinition::new("AEGTE_ID", "spouse_pnr", FieldType::PNR, true),
            Extractors::string("AEGTE_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.spouse_pnr = Some(value);
            }),
        ),
        // Age mapping
        FieldMapping::new(
            FieldDefinition::new("ALDER", "age", FieldType::Integer, true),
            Extractors::for_field(&FieldDefinition::new(
                "ALDER",
                "age",
                FieldType::Integer,
                true,
            )),
            ModelSetters::i32_setter(|individual, age| {
                individual.age = Some(age);
            }),
        ),
        // Family size mapping
        FieldMapping::new(
            FieldDefinition::new("ANTPERSF", "family_size", FieldType::Integer, true),
            Extractors::for_field(&FieldDefinition::new(
                "ANTPERSF",
                "family_size",
                FieldType::Integer,
                true,
            )),
            ModelSetters::i32_setter(|individual, size| {
                individual.family_size = Some(size);
            }),
        ),
        // Household size mapping
        FieldMapping::new(
            FieldDefinition::new("ANTPERSH", "household_size", FieldType::Integer, true),
            Extractors::for_field(&FieldDefinition::new(
                "ANTPERSH",
                "household_size",
                FieldType::Integer,
                true,
            )),
            ModelSetters::i32_setter(|individual, size| {
                individual.household_size = Some(size);
            }),
        ),
        // Residence from date mapping
        FieldMapping::new(
            FieldDefinition::new("BOP_VFRA", "residence_from", FieldType::Date, true),
            Extractors::for_field(&FieldDefinition::new(
                "BOP_VFRA",
                "residence_from",
                FieldType::Date,
                true,
            )),
            ModelSetters::date_setter(|individual, date| {
                individual.residence_from = Some(date);
            }),
        ),
        // Family type mapping
        FieldMapping::new(
            FieldDefinition::new("FAMILIE_TYPE", "family_type", FieldType::Integer, true),
            Extractors::for_field(&FieldDefinition::new(
                "FAMILIE_TYPE",
                "family_type",
                FieldType::Integer,
                true,
            )),
            ModelSetters::i32_setter(|individual, family_type| {
                individual.family_type = Some(family_type);
            }),
        ),
        // Immigration type mapping
        FieldMapping::new(
            FieldDefinition::new("IE_TYPE", "immigration_type", FieldType::String, true),
            Extractors::string("IE_TYPE"),
            ModelSetters::string_setter(|individual, value| {
                individual.immigration_type = Some(value);
            }),
        ),
        // Position in family mapping
        FieldMapping::new(
            FieldDefinition::new("PLADS", "position_in_family", FieldType::Integer, true),
            Extractors::for_field(&FieldDefinition::new(
                "PLADS",
                "position_in_family",
                FieldType::Integer,
                true,
            )),
            ModelSetters::i32_setter(|individual, position| {
                individual.position_in_family = Some(position);
            }),
        ),
    ]
}
