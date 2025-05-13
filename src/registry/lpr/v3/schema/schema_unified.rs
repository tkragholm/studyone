//! Unified LPR v3 schema definitions
//!
//! This module provides unified schema definitions for LPR v3 registries using
//! the centralized field definition system.

use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create an LPR v3-specific field definition
fn lpr3_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified `LPR3_DIAGNOSER` registry schema
///
/// This function creates a schema for the `LPR3_DIAGNOSER` registry using the unified field definition system.
#[must_use]
pub fn create_lpr3_diagnoser_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        FieldMapping::new(
            lpr3_field(
                "DW_EK_KONTAKT",
                "Contact identifier",
                FieldType::String,
                true,
            ),
            Extractors::string("DW_EK_KONTAKT"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("diagnosekode", "Diagnosis code", FieldType::String, true),
            Extractors::string("diagnosekode"),
            ModelSetters::string_setter(|individual, value| {
                if let Some(diagnoses) = &mut individual.diagnoses {
                    diagnoses.push(value);
                } else {
                    individual.diagnoses = Some(vec![value]);
                }
            }),
        ),
        FieldMapping::new(
            lpr3_field("diagnosetype", "Diagnosis type", FieldType::String, true),
            Extractors::string("diagnosetype"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "senere_afkraeftet",
                "Later disproved",
                FieldType::String,
                true,
            ),
            Extractors::string("senere_afkraeftet"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "diagnosekode_parent",
                "Parent diagnosis code",
                FieldType::String,
                true,
            ),
            Extractors::string("diagnosekode_parent"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "diagnosetype_parent",
                "Parent diagnosis type",
                FieldType::String,
                true,
            ),
            Extractors::string("diagnosetype_parent"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "lprindberetningssystem",
                "Reporting system",
                FieldType::String,
                true,
            ),
            Extractors::string("lprindberetningssystem"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "LPR3_DIAGNOSER",
        "Landspatientregistret v3 diagnosis records",
        field_mappings,
    )
}

/// Create the unified `LPR3_KONTAKTER` registry schema
///
/// This function creates a schema for the `LPR3_KONTAKTER` registry using the unified field definition system.
#[must_use]
pub fn create_lpr3_kontakter_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Identification field - CPR is the same as PNR
        FieldMapping::new(
            lpr3_field(
                "CPR",
                "Personal identification number",
                FieldType::PNR,
                false,
            ),
            Extractors::string("CPR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Contact fields
        FieldMapping::new(
            lpr3_field(
                "DW_EK_KONTAKT",
                "Contact identifier",
                FieldType::String,
                true,
            ),
            Extractors::string("DW_EK_KONTAKT"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "DW_EK_FORLOEB",
                "Course identifier",
                FieldType::String,
                true,
            ),
            Extractors::string("DW_EK_FORLOEB"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "SORENHED_IND",
                "Admitting hospital unit",
                FieldType::String,
                true,
            ),
            Extractors::string("SORENHED_IND"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "SORENHED_HEN",
                "Referring hospital unit",
                FieldType::String,
                true,
            ),
            Extractors::string("SORENHED_HEN"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "SORENHED_ANS",
                "Responsible hospital unit",
                FieldType::String,
                true,
            ),
            Extractors::string("SORENHED_ANS"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        // Date and time fields
        FieldMapping::new(
            lpr3_field("dato_start", "Start date", FieldType::Date, true),
            Extractors::date("dato_start"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("tidspunkt_start", "Start time", FieldType::Time, true),
            Extractors::time("tidspunkt_start"),
            ModelSetters::time_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("dato_slut", "End date", FieldType::Date, true),
            Extractors::date("dato_slut"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("tidspunkt_slut", "End time", FieldType::Time, true),
            Extractors::time("tidspunkt_slut"),
            ModelSetters::time_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "dato_behandling_start",
                "Treatment start date",
                FieldType::Date,
                true,
            ),
            Extractors::date("dato_behandling_start"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "tidspunkt_behandling_start",
                "Treatment start time",
                FieldType::Time,
                true,
            ),
            Extractors::time("tidspunkt_behandling_start"),
            ModelSetters::time_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("dato_indberetning", "Reporting date", FieldType::Date, true),
            Extractors::date("dato_indberetning"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        // Medical information
        FieldMapping::new(
            lpr3_field(
                "aktionsdiagnose",
                "Action diagnosis",
                FieldType::String,
                true,
            ),
            Extractors::string("aktionsdiagnose"),
            ModelSetters::string_setter(|individual, value| {
                if let Some(diagnoses) = &mut individual.diagnoses {
                    diagnoses.push(value);
                } else {
                    individual.diagnoses = Some(vec![value]);
                }
            }),
        ),
        FieldMapping::new(
            lpr3_field("kontaktaarsag", "Contact reason", FieldType::String, true),
            Extractors::string("kontaktaarsag"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("prioritet", "Priority", FieldType::String, true),
            Extractors::string("prioritet"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field("kontakttype", "Contact type", FieldType::String, true),
            Extractors::string("kontakttype"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "henvisningsaarsag",
                "Referral reason",
                FieldType::String,
                true,
            ),
            Extractors::string("henvisningsaarsag"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "henvisningsmaade",
                "Referral method",
                FieldType::String,
                true,
            ),
            Extractors::string("henvisningsmaade"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr3_field(
                "lprindberetningssytem",
                "Reporting system",
                FieldType::String,
                true,
            ),
            Extractors::string("lprindberetningssytem"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "LPR3_KONTAKTER",
        "Landspatientregistret v3 contact records",
        field_mappings,
    )
}

/// Get the Arrow schema for `LPR3_DIAGNOSER` data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn lpr3_diagnoser_schema() -> Arc<arrow::datatypes::Schema> {
    create_lpr3_diagnoser_schema().arrow_schema()
}

/// Get the Arrow schema for `LPR3_KONTAKTER` data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn lpr3_kontakter_schema() -> Arc<arrow::datatypes::Schema> {
    create_lpr3_kontakter_schema().arrow_schema()
}
