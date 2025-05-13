//! LPR v3 registry trait-based deserializer
//!
//! This module provides functionality for deserializing LPR v3 registry data
//! using the trait-based field access system.

use arrow::record_batch::RecordBatch;
use log::debug;

use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::registry_traits::LprFields;
use crate::registry::lpr::v3::schema::schema_unified::{
    create_lpr3_diagnoser_schema, create_lpr3_kontakter_schema,
};

/// Generate trait deserializers for LPR v3 registries

// Generate trait deserializer for LPR3_KONTAKTER
crate::generate_trait_deserializer!(
    Lpr3KontakterTraitDeserializer,
    "LPR3_KONTAKTER",
    create_lpr3_kontakter_schema
);

// Generate trait deserializer for LPR3_DIAGNOSER
crate::generate_trait_deserializer!(
    Lpr3DiagnoserTraitDeserializer,
    "LPR3_DIAGNOSER",
    create_lpr3_diagnoser_schema
);

/// Deserialize an LPR3 KONTAKTER record batch using the trait-based deserializer
pub fn deserialize_kontakter_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR3 KONTAKTER batch with trait-based deserializer");
    let deserializer = Lpr3KontakterTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR3 KONTAKTER record batch
pub fn deserialize_kontakter_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = Lpr3KontakterTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// Deserialize an LPR3 DIAGNOSER record batch using the trait-based deserializer
pub fn deserialize_diagnoser_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR3 DIAGNOSER batch with trait-based deserializer");
    let deserializer = Lpr3DiagnoserTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR3 DIAGNOSER record batch
pub fn deserialize_diagnoser_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = Lpr3DiagnoserTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// Enhance individuals with diagnosis information from an LPR3_DIAGNOSER batch
///
/// This function takes a slice of Individual models and an LPR3_DIAGNOSER record batch,
/// and adds diagnosis codes to individuals where available.
pub fn enhance_individuals_with_diagnoses(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    let mut count = 0;

    // Create a deserializer
    let deserializer = Lpr3DiagnoserTraitDeserializer::new();

    // Create a map of PNRs to individuals for fast lookup
    let mut pnr_map = std::collections::HashMap::new();
    for (idx, individual) in individuals.iter().enumerate() {
        pnr_map.insert(individual.pnr.clone(), idx);
    }

    // Deserialize each row and add diagnoses to individuals
    for row in 0..batch.num_rows() {
        if let Some(diagnosis_individual) = deserializer.deserialize_row(batch, row)? {
            // Extract diagnosis info
            let lpr_fields: &dyn LprFields = &diagnosis_individual;
            if let Some(diagnoses) = lpr_fields.diagnoses() {
                if let Some(&idx) = pnr_map.get(&diagnosis_individual.pnr) {
                    // Add diagnoses to the corresponding individual
                    let target_individual = &mut individuals[idx];
                    let target_lpr_fields: &mut dyn LprFields = target_individual;

                    for diagnosis in diagnoses {
                        target_lpr_fields.add_diagnosis(diagnosis.to_string());
                    }

                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Enhance individuals with contact information from an LPR3_KONTAKTER batch
///
/// This function takes a slice of Individual models and an LPR3_KONTAKTER record batch,
/// and adds hospital admission dates and other contact information to individuals.
pub fn enhance_individuals_with_contacts(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    let mut count = 0;

    // Create a deserializer
    let deserializer = Lpr3KontakterTraitDeserializer::new();

    // Create a map of PNRs to individuals for fast lookup
    let mut pnr_map = std::collections::HashMap::new();
    for (idx, individual) in individuals.iter().enumerate() {
        pnr_map.insert(individual.pnr.clone(), idx);
    }

    // Deserialize each row and add contact info to individuals
    for row in 0..batch.num_rows() {
        if let Some(contact_individual) = deserializer.deserialize_row(batch, row)? {
            if let Some(&idx) = pnr_map.get(&contact_individual.pnr) {
                // Extract contact info
                let lpr_fields: &dyn LprFields = &contact_individual;
                let target_individual = &mut individuals[idx];
                let target_lpr_fields: &mut dyn LprFields = target_individual;

                // Copy hospital admissions
                if let Some(admissions) = lpr_fields.hospital_admissions() {
                    for admission_date in admissions {
                        target_lpr_fields.add_hospital_admission(*admission_date);
                    }
                }

                // Copy discharge dates
                if let Some(discharges) = lpr_fields.discharge_dates() {
                    for discharge_date in discharges {
                        target_lpr_fields.add_discharge_date(*discharge_date);
                    }
                }

                // Copy length of stay
                if let Some(los) = lpr_fields.length_of_stay() {
                    target_lpr_fields.set_length_of_stay(Some(los));
                }

                // Also copy diagnoses from action diagnosis
                if let Some(diagnoses) = lpr_fields.diagnoses() {
                    for diagnosis in diagnoses {
                        target_lpr_fields.add_diagnosis(diagnosis.to_string());
                    }
                }

                count += 1;
            }
        }
    }

    Ok(count)
}
