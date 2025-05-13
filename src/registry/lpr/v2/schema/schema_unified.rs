//! Unified LPR v2 schema definitions
//!
//! This module provides unified schema definitions for LPR v2 registries using
//! the centralized field definition system.

use crate::schema::field_def::{Extractors, FieldMapping, ModelSetters};
use crate::schema::{FieldDefinition, FieldType, RegistrySchema, create_registry_schema};
use std::sync::Arc;

/// Create an LPR-specific field definition
fn lpr_field(
    name: &str,
    description: &str,
    field_type: FieldType,
    nullable: bool,
) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified LPR_ADM registry schema
///
/// This function creates a schema for the LPR_ADM registry using the unified field definition system.
#[must_use]
pub fn create_lpr_adm_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Core identification fields

        // Admission-related fields
        FieldMapping::new(
            lpr_field("C_ADIAG", "Action diagnosis", FieldType::String, true),
            Extractors::string("C_ADIAG"),
            ModelSetters::string_setter(|individual, value| {
                if let Some(diagnoses) = &mut individual.diagnoses {
                    diagnoses.push(value);
                } else {
                    individual.diagnoses = Some(vec![value]);
                }
            }),
        ),
        FieldMapping::new(
            lpr_field("C_AFD", "Department code", FieldType::String, true),
            Extractors::string("C_AFD"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_HAFD", "Referring department", FieldType::String, true),
            Extractors::string("C_HAFD"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_HENM", "Referral method", FieldType::String, true),
            Extractors::string("C_HENM"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_HSGH", "Referring hospital", FieldType::String, true),
            Extractors::string("C_HSGH"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_INDM", "Admission method", FieldType::String, true),
            Extractors::string("C_INDM"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_KOM", "Municipality code", FieldType::String, true),
            Extractors::string("C_KOM"),
            ModelSetters::string_setter(|individual, value| {
                individual.municipality_code = Some(value);
            }),
        ),
        FieldMapping::new(
            lpr_field("C_KONTAARS", "Contact reason", FieldType::String, true),
            Extractors::string("C_KONTAARS"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_PATTYPE", "Patient type", FieldType::String, true),
            Extractors::string("C_PATTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_SGH", "Hospital code", FieldType::String, true),
            Extractors::string("C_SGH"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_SPEC", "Speciality code", FieldType::String, true),
            Extractors::string("C_SPEC"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_UDM", "Discharge method", FieldType::String, true),
            Extractors::string("C_UDM"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("CPRTJEK", "CPR check", FieldType::String, true),
            Extractors::string("CPRTJEK"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("CPRTYPE", "CPR type", FieldType::String, true),
            Extractors::string("CPRTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("D_HENDTO", "Referral date", FieldType::Date, true),
            Extractors::date("D_HENDTO"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("D_INDDTO", "Admission date", FieldType::Date, true),
            Extractors::date("D_INDDTO"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("D_UDDTO", "Discharge date", FieldType::Date, true),
            Extractors::date("D_UDDTO"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("K_AFD", "Department identifier", FieldType::String, true),
            Extractors::string("K_AFD"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("RECNUM", "Record number", FieldType::String, true),
            Extractors::string("RECNUM"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("V_ALDDG", "Age in days", FieldType::Integer, true),
            Extractors::integer("V_ALDDG"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("V_ALDER", "Age in years", FieldType::Integer, true),
            Extractors::integer("V_ALDER"),
            ModelSetters::i32_setter(|individual, value| {
                individual.age = Some(value);
            }),
        ),
        FieldMapping::new(
            lpr_field("V_INDMINUT", "Admission minute", FieldType::Integer, true),
            Extractors::integer("V_INDMINUT"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("V_INDTIME", "Admission hour", FieldType::Integer, true),
            Extractors::integer("V_INDTIME"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("V_SENGDAGE", "Hospital stay days", FieldType::Integer, true),
            Extractors::integer("V_SENGDAGE"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("V_UDTIME", "Discharge hour", FieldType::Integer, true),
            Extractors::integer("V_UDTIME"),
            ModelSetters::i32_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "LPR_ADM",
        "Landspatientregistret admission records",
        field_mappings,
    )
}

/// Create the unified LPR_DIAG registry schema
///
/// This function creates a schema for the LPR_DIAG registry using the unified field definition system.
#[must_use]
pub fn create_lpr_diag_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        FieldMapping::new(
            lpr_field("C_DIAG", "Diagnosis code", FieldType::String, true),
            Extractors::string("C_DIAG"),
            ModelSetters::string_setter(|individual, value| {
                if let Some(diagnoses) = &mut individual.diagnoses {
                    diagnoses.push(value);
                } else {
                    individual.diagnoses = Some(vec![value]);
                }
            }),
        ),
        FieldMapping::new(
            lpr_field("C_DIAGTYPE", "Diagnosis type", FieldType::String, true),
            Extractors::string("C_DIAGTYPE"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("C_TILDIAG", "Additional diagnosis", FieldType::String, true),
            Extractors::string("C_TILDIAG"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("LEVERANCEDATO", "Delivery date", FieldType::Date, true),
            Extractors::date("LEVERANCEDATO"),
            ModelSetters::date_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("RECNUM", "Record number", FieldType::String, true),
            Extractors::string("RECNUM"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
        FieldMapping::new(
            lpr_field("VERSION", "Version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|_individual, _value| {
                // This field is currently not mapped to the Individual model
                // It's included for completeness in the schema
            }),
        ),
    ];

    create_registry_schema(
        "LPR_DIAG",
        "Landspatientregistret diagnosis records",
        field_mappings,
    )
}

/// Create the unified LPR_BES registry schema
///
/// This function creates a schema for the LPR_BES registry using the unified field definition system.
#[must_use]
pub fn create_lpr_bes_schema() -> RegistrySchema {
    // The LPR BES schema implementation would go here
    // For now, we'll create a minimal schema to be expanded later
    create_registry_schema("LPR_BES", "Landspatientregistret treatment records", vec![])
}

/// Get the Arrow schema for LPR_ADM data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn lpr_adm_schema() -> Arc<arrow::datatypes::Schema> {
    create_lpr_adm_schema().arrow_schema()
}

/// Get the Arrow schema for LPR_DIAG data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn lpr_diag_schema() -> Arc<arrow::datatypes::Schema> {
    create_lpr_diag_schema().arrow_schema()
}

/// Get the Arrow schema for LPR_BES data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn lpr_bes_schema() -> Arc<arrow::datatypes::Schema> {
    create_lpr_bes_schema().arrow_schema()
}
