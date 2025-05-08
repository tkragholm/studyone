//! Schema-aware constructors for Individual models
//!
//! This module extends the Individual model with schema-specific constructors
//! that understand how to build Individual objects directly from registry schemas.

use crate::error::{Error, Result};
use crate::models::individual::{Gender, Individual, Origin, EducationLevel};
use crate::utils::array_utils::downcast_array;
use arrow::array::{Array, Date32Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{Days, NaiveDate};
use std::collections::HashMap;

impl Individual {
    /// Create an Individual from an IND registry record
    ///
    /// # Arguments
    /// * `batch` - Record batch containing IND registry data
    /// * `row` - Row index in the batch
    ///
    /// # Returns
    /// * `Result<Option<Individual>>` - Individual object if data is valid
    pub fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| Error::ColumnNotFound {
                column: "PNR".to_string(),
            })?;

        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PNR".to_string(),
                expected: "String".to_string(),
            })?;

        // Skip if row is out of bounds
        if row >= pnr_array.len() {
            return Ok(None);
        }

        // Get individual's PNR
        let pnr = pnr_array.value(row).to_string();
        
        // Extract gender
        let gender = Self::extract_gender_from_ind(batch, row)?;
        
        // Extract birth date
        let birth_date = Self::extract_birth_date_from_ind(batch, row)?;
        
        // Create a basic Individual
        let mut individual = Self::new(
            pnr, 
            gender.unwrap_or(Gender::Unknown), 
            birth_date
        );
        
        // Extract death date if available
        if let Some(death_date) = Self::extract_death_date_from_ind(batch, row)? {
            individual.death_date = Some(death_date);
        }
        
        // Extract origin
        if let Some(origin) = Self::extract_origin_from_ind(batch, row)? {
            individual.origin = origin;
        }
        
        // Extract education level
        if let Some(education) = Self::extract_education_from_ind(batch, row)? {
            individual.education_level = education;
        }
        
        // Extract municipality code and rural status
        if let Some(municipality) = Self::extract_municipality_from_ind(batch, row)? {
            individual.municipality_code = Some(municipality.clone());
            // Municipalities with codes below 100 are typically rural (simplified example)
            if let Ok(code) = municipality.parse::<i32>() {
                individual.is_rural = code < 100;
            }
        }
        
        // Extract parent PNRs if available
        if let Some(mother_pnr) = Self::extract_mother_pnr_from_ind(batch, row)? {
            individual.mother_pnr = Some(mother_pnr);
        }
        
        if let Some(father_pnr) = Self::extract_father_pnr_from_ind(batch, row)? {
            individual.father_pnr = Some(father_pnr);
        }
        
        // Extract family ID if available
        if let Some(family_id) = Self::extract_family_id_from_ind(batch, row)? {
            individual.family_id = Some(family_id);
        }
        
        // Extract migration dates if available
        if let Some(emigration_date) = Self::extract_emigration_date_from_ind(batch, row)? {
            individual.emigration_date = Some(emigration_date);
        }
        
        if let Some(immigration_date) = Self::extract_immigration_date_from_ind(batch, row)? {
            individual.immigration_date = Some(immigration_date);
        }
        
        Ok(Some(individual))
    }
    
    /// Extract gender from IND record
    fn extract_gender_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<Gender>> {
        let gender_idx = match batch.schema().index_of("KOEN") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        // Try as string first
        if let Ok(array) = downcast_array::<StringArray>(batch.column(gender_idx), "KOEN", "String") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(Gender::from(array.value(row))));
            }
        }
        
        // Try as integer if string failed
        if let Ok(array) = downcast_array::<Int32Array>(batch.column(gender_idx), "KOEN", "Int32") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(Gender::from(array.value(row))));
            }
        }
        
        Ok(None)
    }
    
    /// Extract birth date from IND record
    fn extract_birth_date_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<NaiveDate>> {
        // First try FOED_DATO column
        let birth_date_idx = match batch.schema().index_of("FOED_DATO") {
            Ok(idx) => idx,
            Err(_) => {
                // If not found, try FOD_DAG
                match batch.schema().index_of("FOD_DAG") {
                    Ok(idx) => idx,
                    Err(_) => return Ok(None), // No birth date column found
                }
            }
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(birth_date_idx), "Birth Date", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract death date from IND record
    fn extract_death_date_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<NaiveDate>> {
        let death_date_idx = match batch.schema().index_of("DOD_DATO") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(death_date_idx), "DOD_DATO", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract origin from IND record
    fn extract_origin_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<Origin>> {
        let origin_idx = match batch.schema().index_of("ORIGIN_TYPE") {
            Ok(idx) => idx,
            Err(_) => {
                // Try alternative column name
                match batch.schema().index_of("IE_TYPE") {
                    Ok(idx) => idx,
                    Err(_) => return Ok(None), // Column not found
                }
            }
        };

        // Try as integer
        if let Ok(array) = downcast_array::<Int32Array>(batch.column(origin_idx), "Origin", "Int32") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(Origin::from(array.value(row))));
            }
        }
        
        // Try as string
        if let Ok(array) = downcast_array::<StringArray>(batch.column(origin_idx), "Origin", "String") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(Origin::from(array.value(row))));
            }
        }
        
        Ok(None)
    }
    
    /// Extract education level from IND record
    fn extract_education_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<EducationLevel>> {
        let education_idx = match batch.schema().index_of("HFUDD") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        // Try as integer
        if let Ok(array) = downcast_array::<Int32Array>(batch.column(education_idx), "HFUDD", "Int32") {
            if row < array.len() && !array.is_null(row) {
                let educ_value = array.value(row);
                // Map ISCED codes to our education levels
                return Ok(Some(match educ_value {
                    0..=2 => EducationLevel::Low,     // ISCED 0-2
                    3..=4 => EducationLevel::Medium,  // ISCED 3-4
                    5..=8 => EducationLevel::High,    // ISCED 5-8
                    _ => EducationLevel::Unknown,
                }));
            }
        }
        
        Ok(None)
    }
    
    /// Extract municipality code from IND record
    fn extract_municipality_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
        let municipality_idx = match batch.schema().index_of("KOM") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        // Try as string
        if let Ok(array) = downcast_array::<StringArray>(batch.column(municipality_idx), "KOM", "String") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(array.value(row).to_string()));
            }
        }
        
        // Try as integer
        if let Ok(array) = downcast_array::<Int32Array>(batch.column(municipality_idx), "KOM", "Int32") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(array.value(row).to_string()));
            }
        }
        
        Ok(None)
    }
    
    /// Extract mother's PNR from IND record
    fn extract_mother_pnr_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
        let mother_idx = match batch.schema().index_of("MOR_PNR") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let mother_array = match downcast_array::<StringArray>(batch.column(mother_idx), "MOR_PNR", "String") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < mother_array.len() && !mother_array.is_null(row) {
            Ok(Some(mother_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract father's PNR from IND record
    fn extract_father_pnr_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
        let father_idx = match batch.schema().index_of("FAR_PNR") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let father_array = match downcast_array::<StringArray>(batch.column(father_idx), "FAR_PNR", "String") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < father_array.len() && !father_array.is_null(row) {
            Ok(Some(father_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract family ID from IND record
    fn extract_family_id_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
        let family_idx = match batch.schema().index_of("FAMILIE_ID") {
            Ok(idx) => idx,
            Err(_) => {
                // Try alternative name
                match batch.schema().index_of("FAMILIE_NR") {
                    Ok(idx) => idx,
                    Err(_) => return Ok(None), // Column not found
                }
            }
        };

        let family_array = if let Ok(arr) = downcast_array::<StringArray>(batch.column(family_idx), "Family ID", "String") { arr } else {
            // Try as integer
            let int_array = match downcast_array::<Int32Array>(batch.column(family_idx), "Family ID", "Int32") {
                Ok(arr) => arr,
                Err(_) => return Ok(None), // Invalid data type
            };
            
            if row < int_array.len() && !int_array.is_null(row) {
                return Ok(Some(int_array.value(row).to_string()));
            }
            return Ok(None);
        };

        if row < family_array.len() && !family_array.is_null(row) {
            Ok(Some(family_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract emigration date from IND record
    fn extract_emigration_date_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<NaiveDate>> {
        let emigration_idx = match batch.schema().index_of("UDVA_DATO") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(emigration_idx), "UDVA_DATO", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract immigration date from IND record
    fn extract_immigration_date_from_ind(batch: &RecordBatch, row: usize) -> Result<Option<NaiveDate>> {
        let immigration_idx = match batch.schema().index_of("INVA_DATO") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(immigration_idx), "INVA_DATO", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Create Individual models from an entire IND record batch
    pub fn from_ind_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        
        // Process each row in the batch
        for row in 0..batch.num_rows() {
            if let Ok(Some(individual)) = Self::from_ind_record(batch, row) {
                individuals.push(individual);
            }
        }
        
        Ok(individuals)
    }
    
    /// Create a mapping from PNR to Individual from a collection of individuals
    #[must_use] pub fn create_pnr_lookup(individuals: &[Self]) -> HashMap<String, Self> {
        let mut lookup = HashMap::with_capacity(individuals.len());
        for individual in individuals {
            lookup.insert(individual.pnr.clone(), individual.clone());
        }
        lookup
    }
    
    /// Create Individual from DOD (death registry) record
    pub fn enhance_with_death_data(
        &mut self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<bool> {
        // Extract PNR to ensure we're enhancing the correct individual
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| Error::ColumnNotFound {
                column: "PNR".to_string(),
            })?;

        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PNR".to_string(),
                expected: "String".to_string(),
            })?;

        // Skip if row is out of bounds
        if row >= pnr_array.len() {
            return Ok(false);
        }

        // Verify this is the correct individual
        let pnr = pnr_array.value(row);
        if pnr != self.pnr {
            return Ok(false);
        }
        
        // Extract death date
        let death_date_idx = match batch.schema().index_of("DOD_DATO") {
            Ok(idx) => idx,
            Err(_) => return Ok(false), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(death_date_idx), "DOD_DATO", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(false), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            let days = date_array.value(row);
            if let Some(death_date) = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64))) {
                // Update the death date
                self.death_date = Some(death_date);
                return Ok(true);
            }
        }
        
        Ok(false)
    }
}