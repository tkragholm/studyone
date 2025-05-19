//! Field mappings for LPR v3 DIAGNOSER registry deserialization
//!
//! This module defines the field mappings for the LPR v3 DIAGNOSER (Diagnoses) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for LPR v3 DIAGNOSER registry
#[must_use]
pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // Contact ID (required, used as ID)
        FieldMapping::new(
            FieldDefinition::new("DW_EK_KONTAKT", "contact_id", FieldType::String, false),
            Extractors::string("DW_EK_KONTAKT"),
            ModelSetters::string_setter(|individual, value| {
                // Store contact ID in properties map
                // This is used for lookup/join with kontakter, but not directly stored in Individual
                let contact_id = value.as_str();
                {
                    individual
                        .store_property("lpr3_diag_contact_id", Box::new(contact_id.to_string()));
                }
            }),
        ),
        // Diagnosis code
        FieldMapping::new(
            FieldDefinition::new("diagnosekode", "diagnosis_code", FieldType::String, true),
            Extractors::string("diagnosekode"),
            ModelSetters::string_setter(|individual, value| {
                let diagnosis = value.as_str();
                {
                    // Skip empty values
                    if diagnosis.trim().is_empty() {
                        return;
                    }

                    // Initialize the diagnoses vector if it doesn't exist
                    if individual.diagnoses.is_none() {
                        individual.diagnoses = Some(Vec::new());
                    }

                    // Add the diagnosis to the list if not already present
                    if let Some(diagnoses) = &mut individual.diagnoses {
                        // Only add if the diagnosis isn't already in the list
                        if !diagnoses.contains(&diagnosis.to_string()) {
                            diagnoses.push(diagnosis.to_string());
                        }
                    }
                }
            }),
        ),
        // Diagnosis type
        FieldMapping::new(
            FieldDefinition::new("diagnosetype", "diagnosis_type", FieldType::String, true),
            Extractors::string("diagnosetype"),
            ModelSetters::string_setter(|individual, value| {
                // Store diagnosis type in properties map as a list of types
                let diag_type = value.as_str();
                {
                    if diag_type.trim().is_empty() {
                        return;
                    }

                    let prop_name = "lpr3_diagnosis_types";

                    // Check if we already have a vector of diagnosis types
                    let types = if let Some(props) = &mut individual.properties {
                        if let Some(existing) = props.get_mut(prop_name) {
                            if let Some(types) = existing.downcast_mut::<Vec<String>>() {
                                types.push(diag_type.to_string());
                                None // We've already updated the existing vector
                            } else {
                                Some(vec![diag_type.to_string()]) // Wrong type, create new
                            }
                        } else {
                            Some(vec![diag_type.to_string()]) // Not found, create new
                        }
                    } else {
                        // No properties map yet, create one
                        individual.properties = Some(std::collections::HashMap::new());
                        Some(vec![diag_type.to_string()])
                    };

                    // If we created a new vector, store it
                    if let Some(new_types) = types {
                        if let Some(props) = &mut individual.properties {
                            props.insert(prop_name.to_string(), Box::new(new_types));
                        }
                    }
                }
            }),
        ),
        // Later disproved flag
        FieldMapping::new(
            FieldDefinition::new(
                "senere_afkraeftet",
                "later_disproved",
                FieldType::String,
                true,
            ),
            Extractors::string("senere_afkraeftet"),
            ModelSetters::string_setter(|individual, value| {
                // Store whether diagnosis was disproved
                let disproved = value.as_str();
                {
                    if disproved.trim().is_empty() {
                        return;
                    }

                    individual.store_property(
                        "lpr3_diagnosis_disproved",
                        Box::new(disproved.to_string()),
                    );
                }
            }),
        ),
        // Parent diagnosis code
        FieldMapping::new(
            FieldDefinition::new(
                "diagnosekode_parent",
                "parent_diagnosis_code",
                FieldType::String,
                true,
            ),
            Extractors::string("diagnosekode_parent"),
            ModelSetters::string_setter(|individual, value| {
                let parent_diagnosis = value.as_str();
                {
                    // Skip empty values
                    if parent_diagnosis.trim().is_empty() {
                        return;
                    }

                    // Store parent diagnosis code in properties map
                    individual.store_property(
                        "lpr3_parent_diagnosis",
                        Box::new(parent_diagnosis.to_string()),
                    );
                }
            }),
        ),
        // Parent diagnosis type
        FieldMapping::new(
            FieldDefinition::new(
                "diagnosetype_parent",
                "parent_diagnosis_type",
                FieldType::String,
                true,
            ),
            Extractors::string("diagnosetype_parent"),
            ModelSetters::string_setter(|individual, value| {
                let parent_type = value.as_str();
                {
                    // Skip empty values
                    if parent_type.trim().is_empty() {
                        return;
                    }

                    // Store parent diagnosis type in properties map
                    individual.store_property(
                        "lpr3_parent_diagnosis_type",
                        Box::new(parent_type.to_string()),
                    );
                }
            }),
        ),
        // Reporting system
        FieldMapping::new(
            FieldDefinition::new(
                "lprindberetningssystem",
                "reporting_system",
                FieldType::String,
                true,
            ),
            Extractors::string("lprindberetningssystem"),
            ModelSetters::string_setter(|individual, value| {
                let system = value.as_str();
                {
                    individual
                        .store_property("lpr3_diag_reporting_system", Box::new(system.to_string()));
                }
            }),
        ),
    ]
}
