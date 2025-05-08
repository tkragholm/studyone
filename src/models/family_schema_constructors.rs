//! Schema-aware constructors for Family models
//!
//! This module extends the Family model with schema-specific constructors
//! that understand how to build Family objects directly from registry schemas.

use crate::error::{Error, Result};
use crate::models::family::{Family, FamilyType};
use crate::models::child::Child;
use crate::models::parent::Parent;
use crate::utils::array_utils::downcast_array;
use arrow::array::{Array, BooleanArray, Date32Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{Days, NaiveDate};
use std::collections::HashMap;
use std::sync::Arc;

impl Family {
    /// Create a Family from a BEF registry record
    ///
    /// # Arguments
    /// * `batch` - Record batch containing BEF registry data
    /// * `row` - Row index in the batch
    /// * `parent_lookup` - Map of Parent objects by PNR
    /// * `child_lookup` - Map of Child objects by PNR
    ///
    /// # Returns
    /// * `Result<Option<Family>>` - Family object if data is valid
    pub fn from_bef_record(
        batch: &RecordBatch,
        row: usize,
        parent_lookup: &HashMap<String, Arc<Parent>>,
        child_lookup: &HashMap<String, Arc<Child>>,
    ) -> Result<Option<Family>> {
        // Extract family ID
        let family_id_idx = batch
            .schema()
            .index_of("FAMILIE_ID")
            .map_err(|_| Error::ColumnNotFound {
                column: "FAMILIE_ID".to_string(),
            })?;

        let family_id_array = batch
            .column(family_id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FAMILIE_ID".to_string(),
                expected: "String".to_string(),
            })?;

        // Skip if row is out of bounds
        if row >= family_id_array.len() {
            return Ok(None);
        }

        // Get family ID
        let family_id = family_id_array.value(row).to_string();
        
        // Extract family type
        let family_type = Self::extract_family_type(batch, row)?;
        
        // Extract valid_from date
        let valid_from = Self::extract_valid_from(batch, row)?;
        if valid_from.is_none() {
            return Ok(None); // Skip if we can't determine validity date
        }
        
        // Create a new family
        let mut family = Family::new(
            family_id,
            family_type.unwrap_or(FamilyType::Unknown),
            valid_from.unwrap(),
        );
        
        // Extract valid_to date
        if let Some(valid_to) = Self::extract_valid_to(batch, row)? {
            family.valid_to = Some(valid_to);
        }
        
        // Extract municipality code
        if let Some(municipality_code) = Self::extract_municipality_code(batch, row)? {
            family.municipality_code = Some(municipality_code);
        }
        
        // Extract rural status
        if let Some(is_rural) = Self::extract_rural_status(batch, row)? {
            family.is_rural = is_rural;
        }
        
        // Extract mother PNR
        let mother_pnr = Self::extract_mother_pnr(batch, row)?;
        if let Some(pnr) = mother_pnr {
            if let Some(mother) = parent_lookup.get(&pnr) {
                family = family.with_mother(mother.clone());
            }
        }
        
        // Extract father PNR
        let father_pnr = Self::extract_father_pnr(batch, row)?;
        if let Some(pnr) = father_pnr {
            if let Some(father) = parent_lookup.get(&pnr) {
                family = family.with_father(father.clone());
            }
        }
        
        // Extract child PNRs
        let child_pnrs = Self::extract_child_pnrs(batch, row)?;
        for pnr in child_pnrs {
            if let Some(child) = child_lookup.get(&pnr) {
                family.add_child(child.clone());
            }
        }
        
        // Determine if family has parental comorbidity
        Self::update_parental_comorbidity_status(&mut family);
        
        // Determine if family has support network
        if let Some(has_support) = Self::extract_support_network(batch, row)? {
            family.has_support_network = has_support;
        }
        
        Ok(Some(family))
    }
    
    /// Extract family type from BEF record
    fn extract_family_type(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<FamilyType>> {
        let type_idx = match batch.schema().index_of("FAMILIE_TYPE") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let type_array = match downcast_array::<Int32Array>(batch.column(type_idx), "FAMILIE_TYPE", "Int32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < type_array.len() && !type_array.is_null(row) {
            let type_value = type_array.value(row);
            Ok(Some(FamilyType::from(type_value)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract `valid_from` date from BEF record
    fn extract_valid_from(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<NaiveDate>> {
        let date_idx = match batch.schema().index_of("VALID_FROM") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(date_idx), "VALID_FROM", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            // Convert Date32 to NaiveDate (days since Unix epoch)
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract `valid_to` date from BEF record
    fn extract_valid_to(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<NaiveDate>> {
        let date_idx = match batch.schema().index_of("VALID_TO") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let date_array = match downcast_array::<Date32Array>(batch.column(date_idx), "VALID_TO", "Date32") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < date_array.len() && !date_array.is_null(row) {
            // Convert Date32 to NaiveDate (days since Unix epoch)
            let days = date_array.value(row);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(Days::new(days as u64)))
                .map_or(Ok(None), |date| Ok(Some(date)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract municipality code from BEF record
    fn extract_municipality_code(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<String>> {
        let code_idx = match batch.schema().index_of("KOM") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let code_array = if let Ok(arr) = downcast_array::<StringArray>(batch.column(code_idx), "KOM", "String") { arr } else {
            // Try as Int32 if not String
            let int_array = match downcast_array::<Int32Array>(batch.column(code_idx), "KOM", "Int32") {
                Ok(arr) => arr,
                Err(_) => return Ok(None), // Invalid data type
            };
            
            if row < int_array.len() && !int_array.is_null(row) {
                return Ok(Some(int_array.value(row).to_string()));
            }
            return Ok(None);
        };

        if row < code_array.len() && !code_array.is_null(row) {
            Ok(Some(code_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract rural status from BEF record
    fn extract_rural_status(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<bool>> {
        let rural_idx = match batch.schema().index_of("IS_RURAL") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let rural_array = if let Ok(arr) = downcast_array::<BooleanArray>(batch.column(rural_idx), "IS_RURAL", "Boolean") { arr } else {
            // Try as Int32 if not Boolean
            let int_array = match downcast_array::<Int32Array>(batch.column(rural_idx), "IS_RURAL", "Int32") {
                Ok(arr) => arr,
                Err(_) => return Ok(None), // Invalid data type
            };
            
            if row < int_array.len() && !int_array.is_null(row) {
                return Ok(Some(int_array.value(row) == 1));
            }
            return Ok(None);
        };

        if row < rural_array.len() && !rural_array.is_null(row) {
            Ok(Some(rural_array.value(row)))
        } else {
            Ok(None)
        }
    }
    
    /// Extract mother PNR from BEF record
    fn extract_mother_pnr(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<String>> {
        let pnr_idx = match batch.schema().index_of("MOR_PNR") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let pnr_array = match downcast_array::<StringArray>(batch.column(pnr_idx), "MOR_PNR", "String") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < pnr_array.len() && !pnr_array.is_null(row) {
            Ok(Some(pnr_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract father PNR from BEF record
    fn extract_father_pnr(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<String>> {
        let pnr_idx = match batch.schema().index_of("FAR_PNR") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let pnr_array = match downcast_array::<StringArray>(batch.column(pnr_idx), "FAR_PNR", "String") {
            Ok(arr) => arr,
            Err(_) => return Ok(None), // Invalid data type
        };

        if row < pnr_array.len() && !pnr_array.is_null(row) {
            Ok(Some(pnr_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    }
    
    /// Extract child PNRs from BEF record
    fn extract_child_pnrs(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Vec<String>> {
        let mut child_pnrs = Vec::new();
        
        // BEF registry might store child PNRs in various columns like BARN1_PNR, BARN2_PNR, etc.
        // We'll try looking for BARN_PNR[] array column first, or BARN1_PNR, BARN2_PNR, etc. individual columns
        
        // First try array approach if supported
        let array_col = batch.schema().index_of("BARN_PNR").ok();
        if let Some(idx) = array_col {
            // Handle array column if available in your data model
            // Implementation would depend on how arrays are stored in your Arrow schema
            // For simplicity, we'll skip the implementation here
        }
        
        // Try individual columns for each child (up to 5 for this example)
        for i in 1..=5 {
            let col_name = format!("BARN{i}_PNR");
            if let Ok(idx) = batch.schema().index_of(&col_name) {
                if let Ok(pnr_array) = downcast_array::<StringArray>(batch.column(idx), &col_name, "String") {
                    if row < pnr_array.len() && !pnr_array.is_null(row) {
                        child_pnrs.push(pnr_array.value(row).to_string());
                    }
                }
            }
        }
        
        Ok(child_pnrs)
    }
    
    /// Update parental comorbidity status
    fn update_parental_comorbidity_status(family: &mut Family) {
        let mother_has_comorbidity = family
            .mother
            .as_ref()
            .is_some_and(|m| m.has_comorbidity);
            
        let father_has_comorbidity = family
            .father
            .as_ref()
            .is_some_and(|f| f.has_comorbidity);
            
        family.has_parental_comorbidity = mother_has_comorbidity || father_has_comorbidity;
    }
    
    /// Extract support network information
    fn extract_support_network(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<bool>> {
        let support_idx = match batch.schema().index_of("HAS_SUPPORT") {
            Ok(idx) => idx,
            Err(_) => return Ok(None), // Column not found
        };

        let support_array = if let Ok(arr) = downcast_array::<BooleanArray>(batch.column(support_idx), "HAS_SUPPORT", "Boolean") { arr } else {
            // Try as Int32 if not Boolean
            let int_array = match downcast_array::<Int32Array>(batch.column(support_idx), "HAS_SUPPORT", "Int32") {
                Ok(arr) => arr,
                Err(_) => return Ok(None), // Invalid data type
            };
            
            if row < int_array.len() && !int_array.is_null(row) {
                return Ok(Some(int_array.value(row) == 1));
            }
            return Ok(None);
        };

        if row < support_array.len() && !support_array.is_null(row) {
            Ok(Some(support_array.value(row)))
        } else {
            Ok(None)
        }
    }
    
    /// Create Family models from an entire BEF record batch
    pub fn from_bef_batch(
        batch: &RecordBatch,
        parent_lookup: &HashMap<String, Arc<Parent>>,
        child_lookup: &HashMap<String, Arc<Child>>,
    ) -> Result<Vec<Family>> {
        let mut families = Vec::new();
        
        // Process each row in the batch
        for row in 0..batch.num_rows() {
            if let Ok(Some(family)) = Self::from_bef_record(batch, row, parent_lookup, child_lookup) {
                families.push(family);
            }
        }
        
        Ok(families)
    }
}