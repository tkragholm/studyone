//! Unified MFR schema definition
//!
//! This module provides a unified schema definition for the MFR registry using
//! the centralized field definition system.

use std::sync::Arc;
use crate::schema::{RegistrySchema, create_registry_schema, FieldDefinition, FieldType};
use crate::schema::field_def::{FieldMapping, ModelSetters, Extractors};
use crate::registry::field_definitions::CommonMappings;

/// Create an MFR-specific field definition
fn mfr_field(name: &str, description: &str, field_type: FieldType, nullable: bool) -> FieldDefinition {
    FieldDefinition::new(name, description, field_type, nullable)
}

/// Create the unified MFR registry schema
///
/// This function creates a schema for the MFR registry using the unified field definition system.
#[must_use]
pub fn create_mfr_schema() -> RegistrySchema {
    // Create field mappings using common definitions where possible
    let field_mappings = vec![
        // Child's CPR number (maps to PNR)
        FieldMapping::new(
            mfr_field("CPR_BARN", "Child's personal identification number", FieldType::PNR, false),
            Extractors::string("CPR_BARN"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        
        // Birth date (maps to FOED_DAG)
        FieldMapping::new(
            mfr_field("FOEDSELSDATO", "Birth date", FieldType::Date, true),
            Extractors::date("FOEDSELSDATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.birth_date = Some(value);
            }),
        ),
        
        // Mother's CPR number (maps to MOR_ID)
        FieldMapping::new(
            mfr_field("CPR_MODER", "Mother's personal identification number", FieldType::PNR, true),
            Extractors::string("CPR_MODER"),
            ModelSetters::string_setter(|individual, value| {
                individual.mother_pnr = Some(value);
            }),
        ),
        
        // Father's CPR number (maps to FAR_ID)
        FieldMapping::new(
            mfr_field("CPR_FADER", "Father's personal identification number", FieldType::PNR, true),
            Extractors::string("CPR_FADER"),
            ModelSetters::string_setter(|individual, value| {
                individual.father_pnr = Some(value);
            }),
        ),
    ];
    
    create_registry_schema(
        "MFR",
        "Medical Birth Registry containing birth information",
        field_mappings,
    )
}

/// Get the Arrow schema for MFR data
///
/// This function is provided for backward compatibility with the existing code.
#[must_use]
pub fn mfr_schema() -> Arc<arrow::datatypes::Schema> {
    create_mfr_schema().arrow_schema()
}