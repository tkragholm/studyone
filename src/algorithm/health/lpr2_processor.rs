//! LPR2 data processing module
//!
//! This module handles the integration and processing of LPR2 format data
//! from the Danish National Patient Registry (LPR).

use crate::algorithm::health::lpr_config::LprConfig;
use crate::error::{ParquetReaderError, Result};
use crate::models::DiagnosisType;
use crate::models::collections::ModelCollection;
use crate::models::health::diagnosis::{Diagnosis, DiagnosisCollection};
use crate::utils::array_utils::get_column;
use crate::utils::arrow_utils::arrow_array_to_date;

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Integrate LPR2 components (`LPR_ADM`, `LPR_DIAG`, and optionally `LPR_BES`)
///
/// This function combines data from the LPR2 admission and diagnosis files
/// to create a comprehensive collection of diagnoses.
pub fn integrate_lpr2_components(
    lpr_adm: &RecordBatch,
    lpr_diag: &RecordBatch,
    lpr_bes: Option<&RecordBatch>,
    config: &LprConfig,
) -> Result<DiagnosisCollection> {
    let mut diagnosis_collection = DiagnosisCollection::new();

    // Extract required columns from LPR_ADM using schema adaptation utilities
    let pnr_col_opt = get_column(lpr_adm, "PNR", &DataType::Utf8, true)?;

    let pnr_array = pnr_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "PNR".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let primary_diag_col_opt = get_column(lpr_adm, "C_ADIAG", &DataType::Utf8, false)?;

    let primary_diag_array = if let Some(col) = &primary_diag_col_opt {
        col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: "C_ADIAG".to_string(),
                expected: "StringArray".to_string(),
            }
        })?
    } else {
        log::warn!("C_ADIAG column not found in LPR_ADM data");
        // Create an empty array as fallback
        &StringArray::from(Vec::<Option<&str>>::new())
    };

    // For date columns, use schema adaptation utilities for better handling
    let date_col_opt = get_column(lpr_adm, "D_INDDTO", &DataType::Date32, false)?;

    // Extract required columns from LPR_DIAG using schema adaptation utilities
    let diag_recnum_col_opt = get_column(lpr_diag, "RECNUM", &DataType::Utf8, true)?;

    let diag_recnum_array = diag_recnum_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "RECNUM".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_col_opt = get_column(lpr_diag, "C_DIAG", &DataType::Utf8, true)?;

    let diag_array = diag_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "C_DIAG".to_string(),
            expected: "StringArray".to_string(),
        })?;

    let diag_type_col_opt = get_column(lpr_diag, "C_DIAGTYPE", &DataType::Utf8, false)?;

    let diag_type_array = if let Some(col) = &diag_type_col_opt {
        col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            ParquetReaderError::InvalidDataType {
                column: "C_DIAGTYPE".to_string(),
                expected: "StringArray".to_string(),
            }
        })?
    } else {
        log::warn!("C_DIAGTYPE column not found in LPR_DIAG data");
        // Create an empty array as fallback
        &StringArray::from(Vec::<Option<&str>>::new())
    };

    // Get record number column from LPR_ADM to link with LPR_DIAG
    let adm_recnum_col_opt = get_column(lpr_adm, "RECNUM", &DataType::Utf8, true)?;

    let adm_recnum_array = adm_recnum_col_opt
        .as_ref()
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| ParquetReaderError::InvalidDataType {
            column: "RECNUM".to_string(),
            expected: "StringArray".to_string(),
        })?;

    // Create a map of diagnoses by record number
    let mut diagnoses_by_recnum: HashMap<String, Vec<(String, DiagnosisType)>> = HashMap::new();

    for i in 0..diag_recnum_array.len() {
        if diag_recnum_array.is_null(i) || diag_array.is_null(i) {
            continue;
        }

        let recnum = diag_recnum_array.value(i).to_string();
        let diagnosis = diag_array.value(i).to_string();
        let diag_type = if i >= diag_type_array.len() || diag_type_array.is_null(i) {
            DiagnosisType::Other
        } else {
            DiagnosisType::from(diag_type_array.value(i))
        };

        diagnoses_by_recnum
            .entry(recnum)
            .or_default()
            .push((diagnosis, diag_type));
    }

    // Process each admission record
    for i in 0..lpr_adm.num_rows() {
        if pnr_array.is_null(i) {
            continue;
        }

        let pnr = pnr_array.value(i).to_string();

        // Extract date using arrow_utils which handles null values and type conversion
        let diagnosis_date = match &date_col_opt {
            Some(date_col) => arrow_array_to_date(date_col, i),
            None => None,
        };

        // Skip if outside date range
        if let Some(date) = diagnosis_date {
            if !config.is_date_in_range(&date) {
                continue;
            }
        }

        // Add primary diagnosis if available
        if i < primary_diag_array.len() && !primary_diag_array.is_null(i) {
            let primary_diagnosis = primary_diag_array.value(i).to_string();

            let diagnosis = Diagnosis::new(
                pnr.clone(),
                primary_diagnosis,
                DiagnosisType::Primary,
                diagnosis_date,
            );

            diagnosis_collection.add(diagnosis);
        }

        // Add secondary diagnoses if available
        if !adm_recnum_array.is_null(i) {
            let recnum = adm_recnum_array.value(i).to_string();

            if let Some(diagnoses) = diagnoses_by_recnum.get(&recnum) {
                for (diagnosis_code, diagnosis_type) in diagnoses {
                    // Skip if it's already the primary diagnosis
                    if i < primary_diag_array.len()
                        && !primary_diag_array.is_null(i)
                        && primary_diag_array.value(i) == diagnosis_code
                    {
                        continue;
                    }

                    let diagnosis = Diagnosis::new(
                        pnr.clone(),
                        diagnosis_code.clone(),
                        *diagnosis_type,
                        diagnosis_date,
                    );

                    diagnosis_collection.add(diagnosis);
                }
            }
        }
    }

    // Process LPR_BES (procedure) data if available
    if let Some(_lpr_bes) = lpr_bes {
        // Implementation for procedure data would go here
        log::info!("LPR_BES data provided but not currently used in diagnosis processing");
    }

    Ok(diagnosis_collection)
}

/// Create an index mapping record numbers to row indices for faster lookups
#[must_use]
pub fn create_recnum_index(batch: &RecordBatch) -> HashMap<String, usize> {
    let mut index = HashMap::new();

    // Try to get the RECNUM column
    if let Ok(Some(recnum_col)) = get_column(batch, "RECNUM", &DataType::Utf8, false) {
        if let Some(recnum_array) = recnum_col.as_any().downcast_ref::<StringArray>() {
            for i in 0..recnum_array.len() {
                if !recnum_array.is_null(i) {
                    index.insert(recnum_array.value(i).to_string(), i);
                }
            }
        }
    }

    index
}

/// Check if a diagnosis code is valid according to LPR2 format rules
#[must_use]
pub fn is_valid_lpr2_diagnosis(code: &str) -> bool {
    // LPR2 diagnoses typically follow ICD-10 format with a letter followed by digits
    // This is a simplified validation - in practice, more complex rules may apply
    if code.is_empty() {
        return false;
    }

    let first_char = code.chars().next().unwrap();
    first_char.is_ascii_alphabetic() && code.len() >= 3
}

/// Format a diagnosis code according to LPR2 standards
#[must_use]
pub fn format_lpr2_diagnosis(code: &str) -> String {
    // Standardize format (uppercase with no spaces)
    code.trim().to_uppercase().replace(' ', "")
}
