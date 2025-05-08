//! Schema-aware constructors for Child models
//!
//! This module extends the Child model with schema-specific constructors
//! that understand how to build Child objects directly from registry schemas.

use crate::error::{Error, Result};
use crate::models::child::Child;
use crate::models::individual::Individual;
use arrow::array::{Array, Date32Array, Float64Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{Days, NaiveDate};
use std::collections::HashMap;
use std::sync::Arc;

impl Child {
    /// Create a Child from an MFR registry record if the individual exists in the lookup
    ///
    /// # Arguments
    /// * `batch` - Record batch containing MFR registry data
    /// * `row` - Row index in the batch
    /// * `individual_lookup` - Map of Individual objects by PNR
    ///
    /// # Returns
    /// * `Result<Option<Child>>` - Child object if the individual exists and data is valid
    pub fn from_mfr_record(
        batch: &RecordBatch,
        row: usize,
        individual_lookup: &HashMap<String, Arc<Individual>>,
    ) -> Result<Option<Self>> {
        // Extract CPR_BARN (child's PNR)
        let pnr_idx = batch
            .schema()
            .index_of("CPR_BARN")
            .map_err(|_| Error::ColumnNotFound {
                column: "CPR_BARN".to_string(),
            })?;

        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_BARN".to_string(),
                expected: "String".to_string(),
            })?;

        // Skip if row is out of bounds
        if row >= pnr_array.len() {
            return Ok(None);
        }

        // Get child's PNR
        let pnr = pnr_array.value(row).to_string();

        // Skip if we don't have this individual in our lookup
        let individual = match individual_lookup.get(&pnr) {
            Some(ind) => ind.clone(),
            None => return Ok(None),
        };

        // Create a basic Child from the Individual
        let mut child = Self::from_individual(individual);

        // Extract birth details
        let birth_details = Self::extract_birth_details_from_mfr(batch, row)?;
        if let Some((birth_weight, gestational_age, apgar_score)) = birth_details {
            child = child.with_birth_details(birth_weight, gestational_age, apgar_score);
        }

        // Extract mother's PNR for birth order calculation
        let mother_pnr = Self::extract_mother_pnr_from_mfr(batch, row)?;
        
        // Calculate birth order if we have the mother's PNR
        if let Some(mother_pnr) = mother_pnr {
            let birth_order = Self::calculate_birth_order_from_mfr(batch, &pnr, &mother_pnr)?;
            if let Some(order) = birth_order {
                child = child.with_birth_order(order);
            }
        }

        Ok(Some(child))
    }

    /// Extract birth details (weight, gestational age, APGAR) from MFR record
    fn extract_birth_details_from_mfr(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<(Option<i32>, Option<i32>, Option<i32>)>> {
        // These fields might be optional in MFR schema
        let birth_weight_idx = batch.schema().index_of("VAEGT_BARN").ok();
        let gestational_age_idx = batch.schema().index_of("GESTATIONSALDER_DAGE").ok();
        // APGAR5 doesn't exist in the standard schema, we'll leave it as None
        let apgar_idx = None;

        // Extract birth weight if available
        let birth_weight = if let Some(idx) = birth_weight_idx {
            // Birth weight is stored as a Float64 in our data
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        // Convert float to integer grams
                        Some(array.value(row) as i32)
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        // Extract gestational age if available
        let gestational_age = if let Some(idx) = gestational_age_idx {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(array.value(row))
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        // Extract APGAR score if available
        let apgar_score = if let Some(idx) = apgar_idx {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .and_then(|array| {
                    if row < array.len() && !array.is_null(row) {
                        Some(array.value(row))
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        Ok(Some((birth_weight, gestational_age, apgar_score)))
    }

    /// Extract mother's PNR from MFR record
    fn extract_mother_pnr_from_mfr(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
        let mother_pnr_idx = match batch.schema().index_of("CPR_MODER") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let mother_pnr_array = match batch
            .column(mother_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
        {
            Some(array) => array,
            None => return Ok(None), // Invalid data type
        };

        if row < mother_pnr_array.len() && !mother_pnr_array.is_null(row) {
            Ok(Some(mother_pnr_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }

    /// Calculate birth order from MFR data
    fn calculate_birth_order_from_mfr(
        batch: &RecordBatch,
        child_pnr: &str,
        mother_pnr: &str,
    ) -> Result<Option<i32>> {
        // Get the required columns
        let child_pnr_idx = batch
            .schema()
            .index_of("CPR_BARN")
            .map_err(|_| Error::ColumnNotFound {
                column: "CPR_BARN".to_string(),
            })?;

        let mother_pnr_idx = batch
            .schema()
            .index_of("CPR_MODER")
            .map_err(|_| Error::ColumnNotFound {
                column: "CPR_MODER".to_string(),
            })?;

        let birth_date_idx = batch
            .schema()
            .index_of("FOEDSELSDATO")
            .map_err(|_| Error::ColumnNotFound {
                column: "FOEDSELSDATO".to_string(),
            })?;

        // Get the arrays
        let child_pnr_array = batch
            .column(child_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_BARN".to_string(),
                expected: "String".to_string(),
            })?;

        let mother_pnr_array = batch
            .column(mother_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_MODER".to_string(),
                expected: "String".to_string(),
            })?;

        let birth_date_array = batch
            .column(birth_date_idx)
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FOEDSELSDATO".to_string(),
                expected: "Date32".to_string(),
            })?;

        // Collect birth dates for all children of the same mother
        let mut siblings_data = Vec::new();

        for i in 0..batch.num_rows() {
            if mother_pnr_array.value(i) == mother_pnr {
                let sibling_pnr = child_pnr_array.value(i).to_string();

                if !birth_date_array.is_null(i) {
                    // Convert Date32 to NaiveDate (days since Unix epoch)
                    if let Some(birth_date) = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .and_then(|epoch| epoch.checked_add_days(Days::new(birth_date_array.value(i) as u64)))
                    {
                        siblings_data.push((sibling_pnr, birth_date));
                    }
                }
            }
        }

        // Sort siblings by birth date
        siblings_data.sort_by(|a, b| a.1.cmp(&b.1));

        // Find the position of the target child
        for (position, (sibling_pnr, _)) in siblings_data.iter().enumerate() {
            if sibling_pnr == child_pnr {
                // Birth order is 1-based (1 = first born)
                return Ok(Some((position + 1) as i32));
            }
        }

        // If not found or couldn't be determined
        Ok(None)
    }
    
    /// Create Child models from an entire MFR record batch
    pub fn from_mfr_batch(
        batch: &RecordBatch,
        individual_lookup: &HashMap<String, Arc<Individual>>,
    ) -> Result<Vec<Self>> {
        let mut children = Vec::new();
        
        // Process each row in the batch
        for row in 0..batch.num_rows() {
            if let Ok(Some(child)) = Self::from_mfr_record(batch, row, individual_lookup) {
                children.push(child);
            }
        }
        
        Ok(children)
    }
}