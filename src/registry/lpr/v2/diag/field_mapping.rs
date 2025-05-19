//! Field mappings for LPR v2 DIAG registry deserialization
//!
//! This module defines the field mappings for the LPR v2 DIAG (Diagnoses) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for LPR v2 DIAG registry
#[must_use]
pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // Record number (required, used as ID)
        FieldMapping::new(
            FieldDefinition::new("RECNUM", "record_number", FieldType::String, false),
            Extractors::string("RECNUM"),
            ModelSetters::string_setter(|individual, value| {
                // Store record number in properties map
                let record_num = value.as_str();
                {
                    individual
                        .store_property("lpr_diag_record_number", Box::new(record_num.to_string()));
                }
            }),
        ),
        // Diagnosis code
        FieldMapping::new(
            FieldDefinition::new("C_DIAG", "diagnosis_code", FieldType::String, true),
            Extractors::string("C_DIAG"),
            ModelSetters::string_setter(|individual, value| {
                let diagnosis = value.as_str();
                {
                    // Initialize the diagnoses vector if it doesn't exist
                    if individual.diagnoses.is_none() {
                        individual.diagnoses = Some(Vec::new());
                    }

                    // Add the diagnosis to the list if not already present
                    if let Some(diagnoses) = &mut individual.diagnoses {
                        if !diagnoses.contains(&diagnosis.to_string()) {
                            diagnoses.push(diagnosis.to_string());
                        }
                    }
                }
            }),
        ),
        // Diagnosis type
        FieldMapping::new(
            FieldDefinition::new("C_DIAGTYPE", "diagnosis_type", FieldType::String, true),
            Extractors::string("C_DIAGTYPE"),
            ModelSetters::string_setter(|individual, value| {
                // Store diagnosis type in properties map
                let diag_type = value.as_str();
                {
                    individual.store_property("diagnosis_type", Box::new(diag_type.to_string()));
                }
            }),
        ),
        // Additional diagnosis
        FieldMapping::new(
            FieldDefinition::new("C_TILDIAG", "additional_diagnosis", FieldType::String, true),
            Extractors::string("C_TILDIAG"),
            ModelSetters::string_setter(|individual, value| {
                let additional_diag = value.as_str();
                {
                    // Skip empty values
                    if additional_diag.trim().is_empty() {
                        return;
                    }

                    // Initialize the diagnoses vector if it doesn't exist
                    if individual.diagnoses.is_none() {
                        individual.diagnoses = Some(Vec::new());
                    }

                    // Add the additional diagnosis to the list if not already present
                    if let Some(diagnoses) = &mut individual.diagnoses {
                        if !diagnoses.contains(&additional_diag.to_string()) {
                            diagnoses.push(additional_diag.to_string());
                        }
                    }
                }
            }),
        ),
        // Delivery date
        FieldMapping::new(
            FieldDefinition::new("LEVERANCEDATO", "delivery_date", FieldType::Date, true),
            Extractors::date("LEVERANCEDATO"),
            ModelSetters::string_setter(|individual, value| {
                // Store the delivery date in properties map
                let date = value;
                {
                    individual.store_property("lpr_diag_delivery_date", Box::new(date));
                }
            }),
        ),
        // Version
        FieldMapping::new(
            FieldDefinition::new("VERSION", "version", FieldType::String, true),
            Extractors::string("VERSION"),
            ModelSetters::string_setter(|individual, value| {
                // Store version in properties map
                let version = value.as_str();
                {
                    individual.store_property("lpr_diag_version", Box::new(version.to_string()));
                }
            }),
        ),
    ]
}
