//! Schema-aware constructors for Diagnosis models
//!
//! This module extends the Diagnosis model with schema-aware constructors
//! for different registry types, enabling direct conversion between
//! registry data and diagnosis models.

use crate::models::diagnosis::{Diagnosis, DiagnosisType, ScdCriteria};
use crate::error::Result;
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::{Array, Date32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;

impl Diagnosis {
    /// Create a Diagnosis directly from an LPR2 DIAG schema record
    ///
    /// This constructor understands the LPR2 DIAG registry schema and can extract
    /// appropriate fields to create a Diagnosis object. It handles field
    /// extraction, type conversion, and SCD classification automatically.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR2 DIAG schema
    /// * `row` - The row index to extract
    /// * `pnr_lookup` - Mapping from record IDs to individual PNRs
    /// * `scd_criteria` - Optional SCD classification criteria
    ///
    /// # Returns
    ///
    /// * `Result<Option<Diagnosis>>` - The created Diagnosis (or None if PNR not found) or an error
    pub fn from_lpr2_diag_record(
        batch: &RecordBatch,
        row: usize,
        pnr_lookup: &HashMap<String, String>,
        scd_criteria: Option<&ScdCriteria>,
    ) -> Result<Option<Self>> {
        // Get columns with automatic type adaptation
        let record_array_opt = get_column(batch, "RECNUM", &DataType::Utf8, true)?;
        
        let record_array = match &record_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "RECNUM", "String")?,
            None => return Ok(None), // Column not found
        };
        
        if row >= record_array.len() {
            return Ok(None); // Row index out of bounds
        }
        
        let record_id = record_array.value(row).to_string();
        
        // Skip if we don't have this record in the PNR lookup
        let individual_pnr = match pnr_lookup.get(&record_id) {
            Some(pnr) => pnr.clone(),
            None => return Ok(None), // PNR not found
        };
        
        // Get diagnosis code
        let diag_array_opt = get_column(batch, "C_DIAG", &DataType::Utf8, true)?;
        
        let diag_array = match &diag_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "C_DIAG", "String")?,
            None => return Ok(None), // Column not found
        };
        
        if row >= diag_array.len() || diag_array.is_null(row) {
            return Ok(None); // No diagnosis code
        }
        
        let diagnosis_code = diag_array.value(row).to_string();
        
        // Get diagnosis type
        let diag_type_array_opt = get_column(batch, "C_DIAGTYPE", &DataType::Utf8, true)?;
        
        let diagnosis_type = if let Some(array) = &diag_type_array_opt {
            let diag_type_array = downcast_array::<StringArray>(array, "C_DIAGTYPE", "String")?;
            if row < diag_type_array.len() && !diag_type_array.is_null(row) {
                DiagnosisType::from(diag_type_array.value(row))
            } else {
                DiagnosisType::Other
            }
        } else {
            DiagnosisType::Other
        };
        
        // Get date
        let date_array_opt = get_column(batch, "LEVERANCEDATO", &DataType::Date32, false)?;
        
        let diagnosis_date = if let Some(array) = &date_array_opt {
            let date_array = downcast_array::<Date32Array>(array, "LEVERANCEDATO", "Date32")?;
            if row < date_array.len() && !date_array.is_null(row) {
                // Convert Date32 to NaiveDate (days since Unix epoch)
                let days = date_array.value(row);
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days as u64)))
            } else {
                None
            }
        } else {
            None
        };
        
        // Create diagnosis
        let mut diagnosis = Diagnosis::new(
            individual_pnr,
            diagnosis_code.clone(),
            diagnosis_type,
            diagnosis_date,
        );
        
        // Apply SCD classification if criteria provided
        if let Some(criteria) = scd_criteria {
            if criteria.is_scd(&diagnosis_code) {
                let severity = criteria.get_severity(&diagnosis_code);
                diagnosis = diagnosis.as_scd(severity);
            }
        }
        
        Ok(Some(diagnosis))
    }
    
    /// Create a collection of Diagnoses from an LPR2 DIAG record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR2 DIAG schema
    /// * `pnr_lookup` - Mapping from record IDs to individual PNRs
    /// * `scd_criteria` - Optional SCD classification criteria
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Diagnosis>>` - The created Diagnoses or an error
    pub fn from_lpr2_diag_batch(
        batch: &RecordBatch,
        pnr_lookup: &HashMap<String, String>,
        scd_criteria: Option<&ScdCriteria>,
    ) -> Result<Vec<Self>> {
        let mut diagnoses = Vec::with_capacity(batch.num_rows());
        
        for i in 0..batch.num_rows() {
            if let Some(diagnosis) = Self::from_lpr2_diag_record(batch, i, pnr_lookup, scd_criteria)? {
                diagnoses.push(diagnosis);
            }
        }
        
        Ok(diagnoses)
    }
    
    /// Create a Diagnosis directly from an LPR3 DIAGNOSER schema record
    ///
    /// This constructor understands the LPR3 DIAGNOSER registry schema and can extract
    /// appropriate fields to create a Diagnosis object. It handles field
    /// extraction, type conversion, and SCD classification automatically.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR3 DIAGNOSER schema
    /// * `row` - The row index to extract
    /// * `pnr_lookup` - Mapping from kontakt IDs to individual PNRs
    /// * `scd_criteria` - Optional SCD classification criteria
    ///
    /// # Returns
    ///
    /// * `Result<Option<Diagnosis>>` - The created Diagnosis (or None if PNR not found) or an error
    pub fn from_lpr3_diagnoser_record(
        batch: &RecordBatch,
        row: usize,
        pnr_lookup: &HashMap<String, String>,
        scd_criteria: Option<&ScdCriteria>,
    ) -> Result<Option<Self>> {
        // Get kontakt ID column
        let kontakt_array_opt = get_column(batch, "DW_EK_KONTAKT", &DataType::Utf8, true)?;
        
        let kontakt_array = match &kontakt_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "DW_EK_KONTAKT", "String")?,
            None => return Ok(None), // Column not found
        };
        
        if row >= kontakt_array.len() {
            return Ok(None); // Row index out of bounds
        }
        
        let kontakt_id = kontakt_array.value(row).to_string();
        
        // Skip if we don't have this kontakt ID in the PNR lookup
        let individual_pnr = match pnr_lookup.get(&kontakt_id) {
            Some(pnr) => pnr.clone(),
            None => return Ok(None), // PNR not found
        };
        
        // Check if diagnosis was later disproven
        let afkraeftet_array_opt = get_column(batch, "senere_afkraeftet", &DataType::Utf8, false)?;
        
        if let Some(array) = &afkraeftet_array_opt {
            let afkraeftet_array = downcast_array::<StringArray>(array, "senere_afkraeftet", "String")?;
            if row < afkraeftet_array.len() && !afkraeftet_array.is_null(row) && afkraeftet_array.value(row) == "JA" {
                return Ok(None); // Skip disproven diagnoses
            }
        }
        
        // Get diagnosis code
        let diag_array_opt = get_column(batch, "diagnosekode", &DataType::Utf8, true)?;
        
        let diag_array = match &diag_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "diagnosekode", "String")?,
            None => return Ok(None), // Column not found
        };
        
        if row >= diag_array.len() || diag_array.is_null(row) {
            return Ok(None); // No diagnosis code
        }
        
        let diagnosis_code = diag_array.value(row).to_string();
        
        // Get diagnosis type
        let diag_type_array_opt = get_column(batch, "diagnosetype", &DataType::Utf8, true)?;
        
        let diagnosis_type = if let Some(array) = &diag_type_array_opt {
            let diag_type_array = downcast_array::<StringArray>(array, "diagnosetype", "String")?;
            if row < diag_type_array.len() && !diag_type_array.is_null(row) {
                match diag_type_array.value(row) {
                    "A" => DiagnosisType::Primary,
                    "B" => DiagnosisType::Secondary,
                    _ => DiagnosisType::Other,
                }
            } else {
                DiagnosisType::Other
            }
        } else {
            DiagnosisType::Other
        };
        
        // LPR3 doesn't have direct date fields in the diagnoser table
        // We would need to join with kontakter table to get the date
        let diagnosis_date = None;
        
        // Create diagnosis
        let mut diagnosis = Diagnosis::new(
            individual_pnr,
            diagnosis_code.clone(),
            diagnosis_type,
            diagnosis_date,
        );
        
        // Apply SCD classification if criteria provided
        if let Some(criteria) = scd_criteria {
            if criteria.is_scd(&diagnosis_code) {
                let severity = criteria.get_severity(&diagnosis_code);
                diagnosis = diagnosis.as_scd(severity);
            }
        }
        
        Ok(Some(diagnosis))
    }
    
    /// Create a collection of Diagnoses from an LPR3 DIAGNOSER record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR3 DIAGNOSER schema
    /// * `pnr_lookup` - Mapping from kontakt IDs to individual PNRs
    /// * `scd_criteria` - Optional SCD classification criteria
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Diagnosis>>` - The created Diagnoses or an error
    pub fn from_lpr3_diagnoser_batch(
        batch: &RecordBatch,
        pnr_lookup: &HashMap<String, String>,
        scd_criteria: Option<&ScdCriteria>,
    ) -> Result<Vec<Self>> {
        let mut diagnoses = Vec::with_capacity(batch.num_rows());
        
        for i in 0..batch.num_rows() {
            if let Some(diagnosis) = Self::from_lpr3_diagnoser_record(batch, i, pnr_lookup, scd_criteria)? {
                diagnoses.push(diagnosis);
            }
        }
        
        Ok(diagnoses)
    }
}