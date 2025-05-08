//! LPR3 data processing module
//!
//! This module handles the integration and processing of LPR3 format data
//! from the Danish National Patient Registry (LPR).

use crate::algorithm::health::lpr_config::LprConfig;
use crate::error::{ParquetReaderError, Result};
use crate::models::diagnosis::{Diagnosis, DiagnosisCollection, DiagnosisType};
use crate::utils::arrow_utils::arrow_date_to_naive_date;

use arrow::array::{Array, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Integrate LPR3 components (`LPR3_KONTAKTER` and `LPR3_DIAGNOSER`)
///
/// This function combines data from the LPR3 contacts and diagnoses files
/// to create a comprehensive collection of diagnoses.
pub fn integrate_lpr3_components(
    lpr3_kontakter: &RecordBatch,
    lpr3_diagnoser: &RecordBatch,
    config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let mut diagnosis_collection = DiagnosisCollection::new();

    // Extract required columns from LPR3_KONTAKTER
    let kontakt_id_col = lpr3_kontakter
        .column_by_name("kontakt_id")
        .ok_or_else(|| ParquetReaderError::column_not_found("kontakt_id"))?;
    let kontakt_id_array = kontakt_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "kontakt_id".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let pnr_col = lpr3_kontakter
        .column_by_name("cpr")
        .ok_or_else(|| ParquetReaderError::column_not_found("cpr"))?;
    let pnr_array = pnr_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "cpr".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let date_col = lpr3_kontakter
        .column_by_name("starttidspunkt")
        .ok_or_else(|| ParquetReaderError::column_not_found("starttidspunkt"))?;
    let date_array = date_col
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "starttidspunkt".to_string(),
            expected: "Date32Array".to_string(),
        })?;

    // Extract required columns from LPR3_DIAGNOSER
    let diag_kontakt_id_col = lpr3_diagnoser
        .column_by_name("kontakt_id")
        .ok_or_else(|| ParquetReaderError::column_not_found("kontakt_id"))?;
    let diag_kontakt_id_array = diag_kontakt_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "kontakt_id".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_col = lpr3_diagnoser
        .column_by_name("diagnosekode")
        .ok_or_else(|| ParquetReaderError::column_not_found("diagnosekode"))?;
    let diag_array = diag_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "diagnosekode".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_type_col = lpr3_diagnoser
        .column_by_name("diagnose_type")
        .ok_or_else(|| ParquetReaderError::column_not_found("diagnose_type"))?;
    let diag_type_array = diag_type_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "diagnose_type".to_string(),
            expected: "StringArray".to_string(),
        })?;

    // Create a map of diagnoses by contact ID
    let mut diagnoses_by_kontakt_id: HashMap<String, Vec<(String, DiagnosisType)>> = HashMap::new();

    for i in 0..diag_kontakt_id_array.len() {
        if diag_kontakt_id_array.is_null(i) || diag_array.is_null(i) {
            continue;
        }

        let kontakt_id = diag_kontakt_id_array.value(i).to_string();
        let diagnosis = diag_array.value(i).to_string();
        let diag_type = if diag_type_array.is_null(i) {
            DiagnosisType::Other
        } else {
            let type_str = diag_type_array.value(i);
            // In LPR3, 'A' is the action diagnosis (primary)
            if type_str == "A" {
                DiagnosisType::Primary
            } else {
                DiagnosisType::from(type_str)
            }
        };

        diagnoses_by_kontakt_id
            .entry(kontakt_id)
            .or_default()
            .push((diagnosis, diag_type));
    }

    // Create index from kontakt_id to row for faster lookup
    let mut kontakt_id_to_row: HashMap<String, usize> = HashMap::new();
    for i in 0..kontakt_id_array.len() {
        if !kontakt_id_array.is_null(i) {
            kontakt_id_to_row.insert(kontakt_id_array.value(i).to_string(), i);
        }
    }

    // Process each diagnosis
    for (kontakt_id, diagnoses) in &diagnoses_by_kontakt_id {
        if let Some(&row_idx) = kontakt_id_to_row.get(kontakt_id) {
            if pnr_array.is_null(row_idx) {
                continue;
            }

            let pnr = pnr_array.value(row_idx).to_string();

            // Get diagnosis date
            let diagnosis_date = if date_array.is_null(row_idx) {
                None
            } else {
                let record_date = arrow_date_to_naive_date(date_array.value(row_idx));
                
                // Skip if outside date range
                if let Some(start_date) = config.start_date {
                    if record_date < start_date {
                        continue;
                    }
                }

                if let Some(end_date) = config.end_date {
                    if record_date > end_date {
                        continue;
                    }
                }
                
                Some(record_date)
            };

            // Add all diagnoses for this contact
            for (diagnosis_code, diagnosis_type) in diagnoses {
                let diagnosis = Diagnosis::new(
                    pnr.clone(),
                    diagnosis_code.clone(),
                    *diagnosis_type,
                    diagnosis_date,
                );

                diagnosis_collection.add_diagnosis(diagnosis);
            }
        }
    }

    Ok(diagnosis_collection)
}

/// Create an index mapping contact IDs to row indices for faster lookups
#[must_use]
pub fn create_contact_id_index(batch: &RecordBatch) -> HashMap<String, usize> {
    let mut index = HashMap::new();
    
    if let Some(id_col) = batch.column_by_name("kontakt_id") {
        if let Some(id_array) = id_col.as_any().downcast_ref::<StringArray>() {
            for i in 0..id_array.len() {
                if !id_array.is_null(i) {
                    index.insert(id_array.value(i).to_string(), i);
                }
            }
        }
    }
    
    index
}

/// Extract diagnoses from an LPR3 diagnosis record batch
pub fn extract_diagnoses_from_lpr3(
    batch: &RecordBatch,
) -> Result<HashMap<String, Vec<(String, DiagnosisType)>>> {
    let mut diagnoses_by_kontakt_id: HashMap<String, Vec<(String, DiagnosisType)>> = HashMap::new();
    
    // Extract required columns
    let kontakt_id_col = batch
        .column_by_name("kontakt_id")
        .ok_or_else(|| ParquetReaderError::column_not_found("kontakt_id"))?;
    let kontakt_id_array = kontakt_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "kontakt_id".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_col = batch
        .column_by_name("diagnosekode")
        .ok_or_else(|| ParquetReaderError::column_not_found("diagnosekode"))?;
    let diag_array = diag_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "diagnosekode".to_string(),
            expected: "StringArray".to_string(),
        })?;

    // Diagnosis type is optional
    let diag_type_array = if let Some(type_col) = batch.column_by_name("diagnose_type") {
        type_col.as_any().downcast_ref::<StringArray>()
    } else {
        None
    };
    
    // Process each row
    for i in 0..batch.num_rows() {
        if kontakt_id_array.is_null(i) || diag_array.is_null(i) {
            continue;
        }
        
        let kontakt_id = kontakt_id_array.value(i).to_string();
        let diagnosis = diag_array.value(i).to_string();
        
        let diag_type = if let Some(array) = diag_type_array {
            if i < array.len() && !array.is_null(i) {
                let type_str = array.value(i);
                // In LPR3, 'A' is the action diagnosis (primary)
                if type_str == "A" {
                    DiagnosisType::Primary
                } else {
                    DiagnosisType::from(type_str)
                }
            } else {
                DiagnosisType::Other
            }
        } else {
            DiagnosisType::Other
        };
        
        diagnoses_by_kontakt_id
            .entry(kontakt_id)
            .or_default()
            .push((diagnosis, diag_type));
    }
    
    Ok(diagnoses_by_kontakt_id)
}

/// Check if a diagnosis code is valid according to LPR3 format rules
#[must_use]
pub fn is_valid_lpr3_diagnosis(code: &str) -> bool {
    // LPR3 diagnoses typically follow ICD-10 format with a letter followed by digits
    // Some additional validation may be needed for LPR3-specific formats
    if code.is_empty() {
        return false;
    }
    
    let first_char = code.chars().next().unwrap();
    first_char.is_ascii_alphabetic() && code.len() >= 3
}

/// Format a diagnosis code according to LPR3 standards
#[must_use]
pub fn format_lpr3_diagnosis(code: &str) -> String {
    // Standardize format (uppercase with no spaces)
    code.trim().to_uppercase().replace(' ', "")
}