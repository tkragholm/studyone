//! Schema-aware constructors for Parent models
//!
//! This module extends the Parent model with schema-specific constructors
//! that understand how to build Parent objects directly from registry schemas.

use crate::error::{Error, Result};
use crate::models::parent::{JobSituation, Parent};
use crate::models::individual::Individual;
use crate::models::income::Income;
use crate::utils::array_utils::downcast_array;
use arrow::array::{Array, BooleanArray, Int32Array, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

impl Parent {
    /// Create a Parent from an IND registry record if the individual exists in the lookup
    ///
    /// # Arguments
    /// * `batch` - Record batch containing IND registry data
    /// * `row` - Row index in the batch
    /// * `individual_lookup` - Map of Individual objects by PNR
    ///
    /// # Returns
    /// * `Result<Option<Parent>>` - Parent object if the individual exists and data is valid
    pub fn from_ind_record(
        batch: &RecordBatch,
        row: usize,
        individual_lookup: &HashMap<String, Arc<Individual>>,
    ) -> Result<Option<Parent>> {
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

        // Get parent's PNR
        let pnr = pnr_array.value(row).to_string();

        // Skip if we don't have this individual in our lookup
        let individual = match individual_lookup.get(&pnr) {
            Some(ind) => ind.clone(),
            None => return Ok(None),
        };

        // Create a basic Parent from the Individual
        let mut parent = Parent::from_individual(individual);

        // Extract employment status if available (SOCIO field in the schema)
        let socio_idx = batch.schema().index_of("SOCIO").ok();
        
        if let Some(idx) = socio_idx {
            if let Some(employment_status) = Self::extract_employment_status(batch, idx, row)? {
                parent = parent.with_employment_status(employment_status);
            }
        }

        // Extract job situation if available
        let socstil_idx = batch.schema().index_of("SOCSTIL").ok();
        
        if let Some(idx) = socstil_idx {
            if let Some(job_situation) = Self::extract_job_situation(batch, idx, row)? {
                parent = parent.with_job_situation(job_situation);
            }
        }

        // Extract pre-exposure income if available
        let income_idx = batch.schema().index_of("PERINDKIALT_13").ok();
        
        if let Some(idx) = income_idx {
            if let Some(income) = Self::extract_pre_exposure_income(batch, idx, row)? {
                parent = parent.with_pre_exposure_income(income);
            }
        }

        Ok(Some(parent))
    }

    /// Extract employment status from IND record
    fn extract_employment_status(
        batch: &RecordBatch,
        socio_idx: usize,
        row: usize,
    ) -> Result<Option<bool>> {
        // SOCIO in IND could be multiple types, we'll handle Int32 and Boolean
        if let Ok(array) = downcast_array::<Int32Array>(batch.column(socio_idx), "SOCIO", "Int32") {
            if row < array.len() && !array.is_null(row) {
                // In IND registry, employment status might be coded as:
                // 1 = employed, 0 = unemployed (or other coding scheme based on registry)
                return Ok(Some(array.value(row) == 1));
            }
        } else if let Ok(array) = downcast_array::<BooleanArray>(batch.column(socio_idx), "SOCIO", "Boolean") {
            if row < array.len() && !array.is_null(row) {
                return Ok(Some(array.value(row)));
            }
        }
        
        Ok(None)
    }

    /// Extract job situation from IND record
    fn extract_job_situation(
        batch: &RecordBatch,
        socstil_idx: usize,
        row: usize,
    ) -> Result<Option<JobSituation>> {
        let array = match downcast_array::<Int32Array>(batch.column(socstil_idx), "SOCSTIL", "Int32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Different type than expected
        };

        if row < array.len() && !array.is_null(row) {
            let job_value = array.value(row);
            Ok(Some(JobSituation::from(job_value)))
        } else {
            Ok(None)
        }
    }

    /// Extract pre-exposure income from IND record
    fn extract_pre_exposure_income(
        batch: &RecordBatch,
        income_idx: usize,
        row: usize,
    ) -> Result<Option<f64>> {
        let array = match downcast_array::<Float64Array>(batch.column(income_idx), "PERINDKIALT_13", "Float64") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Different type than expected
        };

        if row < array.len() && !array.is_null(row) {
            Ok(Some(array.value(row)))
        } else {
            Ok(None)
        }
    }
    
    /// Create Parent models from an entire IND record batch
    pub fn from_ind_batch(
        batch: &RecordBatch,
        individual_lookup: &HashMap<String, Arc<Individual>>,
    ) -> Result<Vec<Parent>> {
        let mut parents = Vec::new();
        
        // Process each row in the batch
        for row in 0..batch.num_rows() {
            if let Ok(Some(parent)) = Self::from_ind_record(batch, row, individual_lookup) {
                parents.push(parent);
            }
        }
        
        Ok(parents)
    }
    
    /// Enhance existing parent with income data from IND record
    pub fn enhance_with_income(
        &mut self,
        income: &Income,
    ) {
        // Add income data point to the parent
        self.add_income(Arc::new(income.clone()));
    }
    
    /// Enhance existing parent with diagnosis data (from LPR record)
    pub fn enhance_with_diagnosis(
        &mut self,
        diagnosis: &crate::models::diagnosis::Diagnosis,
    ) {
        // Add diagnosis to the parent
        self.add_diagnosis(Arc::new(diagnosis.clone()));
    }
}