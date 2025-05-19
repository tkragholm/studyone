//! Field mappings for LPR v3 KONTAKTER registry deserialization
//!
//! This module defines the field mappings for the LPR v3 KONTAKTER (Contacts) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for LPR v3 KONTAKTER registry
#[must_use] pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // PNR mapping (required)
        FieldMapping::new(
            FieldDefinition::new("CPR", "pnr", FieldType::PNR, false),
            Extractors::string("CPR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Contact ID
        FieldMapping::new(
            FieldDefinition::new("DW_EK_KONTAKT", "contact_id", FieldType::String, true),
            Extractors::string("DW_EK_KONTAKT"),
            ModelSetters::string_setter(|individual, value| {
                // Store contact ID in properties map
                if let contact_id = value.as_str() {
                    individual.store_property("lpr3_contact_id", Box::new(contact_id.to_string()));
                }
            }),
        ),
        // Course ID
        FieldMapping::new(
            FieldDefinition::new("DW_EK_FORLOEB", "course_id", FieldType::String, true),
            Extractors::string("DW_EK_FORLOEB"),
            ModelSetters::string_setter(|individual, value| {
                // Store course ID in properties map
                if let course_id = value.as_str() {
                    individual.store_property("lpr3_course_id", Box::new(course_id.to_string()));
                }
            }),
        ),
        // Organization units
        FieldMapping::new(
            FieldDefinition::new(
                "SORENHED_IND",
                "org_unit_admission",
                FieldType::String,
                true,
            ),
            Extractors::string("SORENHED_IND"),
            ModelSetters::string_setter(|individual, value| {
                // Store org unit in properties map
                if let org_unit = value.as_str() {
                    individual
                        .store_property("lpr3_org_unit_admission", Box::new(org_unit.to_string()));
                }
            }),
        ),
        FieldMapping::new(
            FieldDefinition::new("SORENHED_HEN", "org_unit_referral", FieldType::String, true),
            Extractors::string("SORENHED_HEN"),
            ModelSetters::string_setter(|individual, value| {
                // Store org unit in properties map
                if let org_unit = value.as_str() {
                    individual
                        .store_property("lpr3_org_unit_referral", Box::new(org_unit.to_string()));
                }
            }),
        ),
        FieldMapping::new(
            FieldDefinition::new(
                "SORENHED_ANS",
                "org_unit_responsible",
                FieldType::String,
                true,
            ),
            Extractors::string("SORENHED_ANS"),
            ModelSetters::string_setter(|individual, value| {
                // Store org unit in properties map
                if let org_unit = value.as_str() {
                    individual.store_property(
                        "lpr3_org_unit_responsible",
                        Box::new(org_unit.to_string()),
                    );
                }
            }),
        ),
        // Start date
        FieldMapping::new(
            FieldDefinition::new("dato_start", "start_date", FieldType::Date, true),
            Extractors::date("dato_start"),
            ModelSetters::date_setter(|individual, value| {
                // Set as hospital admission date
                if let date = value {
                    // Update last hospital admission date
                    if individual.last_hospital_admission_date.is_none()
                        || individual.last_hospital_admission_date.unwrap() < date
                    {
                        individual.last_hospital_admission_date = Some(date);
                    }

                    // Add to hospital admissions list
                    if individual.hospital_admissions.is_none() {
                        individual.hospital_admissions = Some(Vec::new());
                    }

                    if let Some(admissions) = &mut individual.hospital_admissions {
                        admissions.push(date);
                    }

                    // Increment hospital admissions count
                    let current_count = individual.hospital_admissions_count.unwrap_or(0);
                    individual.hospital_admissions_count = Some(current_count + 1);
                }
            }),
        ),
        // End date
        FieldMapping::new(
            FieldDefinition::new("dato_slut", "end_date", FieldType::Date, true),
            Extractors::date("dato_slut"),
            ModelSetters::date_setter(|individual, value| {
                // Add to discharge dates list
                if let date = value {
                    if individual.discharge_dates.is_none() {
                        individual.discharge_dates = Some(Vec::new());
                    }

                    if let Some(discharges) = &mut individual.discharge_dates {
                        discharges.push(date);
                    }
                }
            }),
        ),
        // Primary diagnosis
        FieldMapping::new(
            FieldDefinition::new(
                "aktionsdiagnose",
                "primary_diagnosis",
                FieldType::String,
                true,
            ),
            Extractors::string("aktionsdiagnose"),
            ModelSetters::string_setter(|individual, value| {
                if let diagnosis = value.as_str() {
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
                        if !diagnoses.contains(&diagnosis.to_string()) {
                            diagnoses.push(diagnosis.to_string());
                        }
                    }
                }
            }),
        ),
        // Contact type
        FieldMapping::new(
            FieldDefinition::new("kontakttype", "contact_type", FieldType::String, true),
            Extractors::string("kontakttype"),
            ModelSetters::string_setter(|individual, value| {
                if let contact_type = value.as_str() {
                    // Check if this is an emergency visit
                    if contact_type.contains("akut") || contact_type.contains("emergency") {
                        // Increment emergency visits count
                        let current_count = individual.emergency_visits_count.unwrap_or(0);
                        individual.emergency_visits_count = Some(current_count + 1);
                    }

                    // Store contact type in properties
                    individual
                        .store_property("lpr3_contact_type", Box::new(contact_type.to_string()));
                }
            }),
        ),
        // Additional informational fields
        FieldMapping::new(
            FieldDefinition::new("kontaktaarsag", "contact_reason", FieldType::String, true),
            Extractors::string("kontaktaarsag"),
            ModelSetters::string_setter(|individual, value| {
                if let reason = value.as_str() {
                    individual.store_property("lpr3_contact_reason", Box::new(reason.to_string()));
                }
            }),
        ),
        FieldMapping::new(
            FieldDefinition::new("prioritet", "priority", FieldType::String, true),
            Extractors::string("prioritet"),
            ModelSetters::string_setter(|individual, value| {
                if let priority = value.as_str() {
                    individual.store_property("lpr3_priority", Box::new(priority.to_string()));
                }
            }),
        ),
        FieldMapping::new(
            FieldDefinition::new(
                "henvisningsaarsag",
                "referral_reason",
                FieldType::String,
                true,
            ),
            Extractors::string("henvisningsaarsag"),
            ModelSetters::string_setter(|individual, value| {
                if let reason = value.as_str() {
                    individual.store_property("lpr3_referral_reason", Box::new(reason.to_string()));
                }
            }),
        ),
        FieldMapping::new(
            FieldDefinition::new(
                "henvisningsmaade",
                "referral_method",
                FieldType::String,
                true,
            ),
            Extractors::string("henvisningsmaade"),
            ModelSetters::string_setter(|individual, value| {
                if let method = value.as_str() {
                    individual.store_property("lpr3_referral_method", Box::new(method.to_string()));
                }
            }),
        ),
    ]
}
