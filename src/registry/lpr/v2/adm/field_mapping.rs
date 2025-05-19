//! Field mappings for LPR v2 ADM registry deserialization
//!
//! This module defines the field mappings for the LPR v2 ADM (Admissions) registry.

use crate::schema::field_def::FieldMapping;
use crate::schema::field_def::{
    FieldDefinition, FieldType,
    mapping::{Extractors, ModelSetters},
};

/// Create field mappings for LPR v2 ADM registry
#[must_use]
pub fn create_field_mappings() -> Vec<FieldMapping> {
    vec![
        // PNR mapping (required)
        FieldMapping::new(
            FieldDefinition::new("PNR", "pnr", FieldType::PNR, false),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        ),
        // Action diagnosis
        FieldMapping::new(
            FieldDefinition::new("C_ADIAG", "action_diagnosis", FieldType::String, true),
            Extractors::string("C_ADIAG"),
            ModelSetters::string_setter(|individual, value| {
                let diagnosis = value.as_str();
                // Initialize the diagnoses vector if it doesn't exist
                if individual.diagnoses.is_none() {
                    individual.diagnoses = Some(Vec::new());
                }

                // Add the diagnosis to the list
                if let Some(diagnoses) = &mut individual.diagnoses {
                    diagnoses.push(diagnosis.to_string());
                }
            }),
        ),
        // Department code
        FieldMapping::new(
            FieldDefinition::new("C_AFD", "department_code", FieldType::String, true),
            Extractors::string("C_AFD"),
            ModelSetters::string_setter(|individual, value| {
                // Store department code in properties map
                let code = value.as_str();
                individual.store_property("department_code", Box::new(code.to_string()));
            }),
        ),
        // Municipality code
        FieldMapping::new(
            FieldDefinition::new("C_KOM", "municipality_code", FieldType::String, true),
            Extractors::string("C_KOM"),
            ModelSetters::string_setter(|individual, value| {
                individual.municipality_code = Some(value);
            }),
        ),
        // Admission date
        FieldMapping::new(
            FieldDefinition::new("D_INDDTO", "admission_date", FieldType::Date, true),
            Extractors::date("D_INDDTO"),
            ModelSetters::date_setter(|individual, value| {
                // Set as last hospital admission date
                individual.last_hospital_admission_date = Some(value);

                // Also add to hospital admissions list
                let date = value;
                if individual.hospital_admissions.is_none() {
                    individual.hospital_admissions = Some(Vec::new());
                }

                if let Some(admissions) = &mut individual.hospital_admissions {
                    admissions.push(date);
                }
            }),
        ),
        // Discharge date
        FieldMapping::new(
            FieldDefinition::new("D_UDDTO", "discharge_date", FieldType::Date, true),
            Extractors::date("D_UDDTO"),
            ModelSetters::date_setter(|individual, value| {
                // Add to discharge dates list
                let date = value;
                if individual.discharge_dates.is_none() {
                    individual.discharge_dates = Some(Vec::new());
                }

                if let Some(discharges) = &mut individual.discharge_dates {
                    discharges.push(date);
                }
            }),
        ),
        // Age
        FieldMapping::new(
            FieldDefinition::new("V_ALDER", "age", FieldType::Integer, true),
            Extractors::integer("V_ALDER"),
            ModelSetters::i32_setter(|individual, value| {
                individual.age = Some(value);
            }),
        ),
        // Length of stay
        FieldMapping::new(
            FieldDefinition::new("V_SENGDAGE", "length_of_stay", FieldType::Integer, true),
            Extractors::integer("V_SENGDAGE"),
            ModelSetters::i32_setter(|individual, value| {
                individual.length_of_stay = Some(value);

                // Also add to total hospitalization days
                if individual.hospitalization_days.is_none() {
                    individual.hospitalization_days = Some(value);
                } else if let Some(days) = individual.hospitalization_days {
                    individual.hospitalization_days = Some(days + value);
                }
            }),
        ),
        // Record number
        FieldMapping::new(
            FieldDefinition::new("RECNUM", "record_number", FieldType::String, true),
            Extractors::string("RECNUM"),
            ModelSetters::string_setter(|individual, value| {
                // Store record number in properties map
                let record_num = value.as_str();
                individual.store_property("lpr_record_number", Box::new(record_num.to_string()));
            }),
        ),
    ]
}
