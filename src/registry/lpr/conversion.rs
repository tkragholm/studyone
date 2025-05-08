//! LPR registry model conversion implementations
//!
//! This module implements bidirectional conversion between LPR registry data
//! and domain models (Diagnosis).

use crate::registry::{Lpr3DiagnoserRegister, LprDiagRegister, ModelConversion};
use crate::error::Result;
use crate::models::diagnosis::{Diagnosis, ScdCriteria, ScdResult};
use crate::common::traits::LprRegistry;
use crate::RecordBatch;

/// `ModelConversion` implementation for LPR2 DIAG registry
impl ModelConversion<Diagnosis> for LprDiagRegister {
    /// Convert LPR2 DIAG registry data to Diagnosis domain models
    ///
    /// Note: This implementation requires a PNR lookup map which must be
    /// set on the registry instance. If not set, it will return an empty list.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR2 DIAG schema
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Diagnosis>>` - The created Diagnosis models or an error
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        let pnr_lookup = if let Some(lookup) = self.get_pnr_lookup() { lookup } else {
            log::warn!("No PNR lookup set for LprDiagRegister, cannot convert to models");
            return Ok(Vec::new());
        };
        
        // Use the LprRegistry trait to convert the batch
        let mut diagnoses = Diagnosis::from_lpr_batch(batch)?;
        
        // Since LprRegistry doesn't handle PNR lookup directly, we need to apply it here
        // This is a transition step until we fully migrate to the trait-based approach
        for diagnosis in &mut diagnoses {
            // Check if we have a lookup entry for this record
            if let Some(record_num) = diagnosis.individual_pnr.strip_prefix("RECORD:") {
                if let Some(pnr) = pnr_lookup.get(record_num) {
                    diagnosis.individual_pnr = pnr.clone();
                }
            }
        }
        
        // Remove any records that don't have valid PNRs
        diagnoses.retain(|d| !d.individual_pnr.starts_with("RECORD:"));
        
        Ok(diagnoses)
    }
    
    /// Convert Diagnosis domain models back to LPR2 DIAG registry data
    ///
    /// # Arguments
    ///
    /// * `models` - The Diagnosis models to convert
    ///
    /// # Returns
    ///
    /// * `Result<RecordBatch>` - The created record batch or an error
    fn from_models(&self, _models: &[Diagnosis]) -> Result<RecordBatch> {
        // This would be implemented with arrow array builders for each field
        // matching the LPR2 DIAG schema
        unimplemented!("Conversion from Diagnosis models to LPR2 DIAG registry data not yet implemented")
    }
    
    /// Process SCD results from diagnoses
    ///
    /// # Arguments
    ///
    /// * `models` - The Diagnosis models to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform_models(&self, _models: &mut [Diagnosis]) -> Result<()> {
        // No transformation needed, SCD classification is done during conversion
        Ok(())
    }
}

/// `ModelConversion` implementation for LPR3 DIAGNOSER registry
impl ModelConversion<Diagnosis> for Lpr3DiagnoserRegister {
    /// Convert LPR3 DIAGNOSER registry data to Diagnosis domain models
    ///
    /// Note: This implementation requires a PNR lookup map which must be
    /// set on the registry instance. If not set, it will return an empty list.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch with LPR3 DIAGNOSER schema
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Diagnosis>>` - The created Diagnosis models or an error
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        let pnr_lookup = if let Some(lookup) = self.get_pnr_lookup() { lookup } else {
            log::warn!("No PNR lookup set for Lpr3DiagnoserRegister, cannot convert to models");
            return Ok(Vec::new());
        };
        
        // Use the LprRegistry trait to convert the batch
        let mut diagnoses = Diagnosis::from_lpr_batch(batch)?;
        
        // Since LprRegistry doesn't handle PNR lookup directly, we need to apply it here
        // This is a transition step until we fully migrate to the trait-based approach
        for diagnosis in &mut diagnoses {
            // Check if we have a lookup entry for this record - LPR3 might use KONTAKT_ID instead of RECNUM
            if let Some(kontakt_id) = diagnosis.individual_pnr.strip_prefix("KONTAKT:") {
                if let Some(pnr) = pnr_lookup.get(kontakt_id) {
                    diagnosis.individual_pnr = pnr.clone();
                }
            }
        }
        
        // Remove any records that don't have valid PNRs
        diagnoses.retain(|d| !d.individual_pnr.starts_with("KONTAKT:"));
        
        Ok(diagnoses)
    }
    
    /// Convert Diagnosis domain models back to LPR3 DIAGNOSER registry data
    ///
    /// # Arguments
    ///
    /// * `models` - The Diagnosis models to convert
    ///
    /// # Returns
    ///
    /// * `Result<RecordBatch>` - The created record batch or an error
    fn from_models(&self, _models: &[Diagnosis]) -> Result<RecordBatch> {
        // This would be implemented with arrow array builders for each field
        // matching the LPR3 DIAGNOSER schema
        unimplemented!("Conversion from Diagnosis models to LPR3 DIAGNOSER registry data not yet implemented")
    }
    
    /// Process SCD results from diagnoses
    ///
    /// # Arguments
    ///
    /// * `models` - The Diagnosis models to transform
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    fn transform_models(&self, _models: &mut [Diagnosis]) -> Result<()> {
        // No transformation needed, SCD classification is done during conversion
        Ok(())
    }
}

/// Utility trait for LPR registries that require a PNR lookup
pub trait PnrLookupRegistry {
    /// Get the PNR lookup map for this registry
    fn get_pnr_lookup(&self) -> Option<std::collections::HashMap<String, String>>;
    
    /// Set the PNR lookup map for this registry
    fn set_pnr_lookup(&mut self, lookup: std::collections::HashMap<String, String>);
    
    /// Process diagnosis records to create SCD results
    ///
    /// # Arguments
    ///
    /// * `diagnoses` - The diagnoses to process
    ///
    /// # Returns
    ///
    /// * `std::collections::HashMap<String, ScdResult>` - SCD results by PNR
    fn process_scd_results(&self, diagnoses: &[Diagnosis]) -> std::collections::HashMap<String, ScdResult> {
        // Group diagnoses by individual
        let mut diagnoses_by_pnr: std::collections::HashMap<String, Vec<std::sync::Arc<Diagnosis>>> = 
            std::collections::HashMap::new();

        for diagnosis in diagnoses {
            diagnoses_by_pnr
                .entry(diagnosis.individual_pnr.clone())
                .or_default()
                .push(std::sync::Arc::new(diagnosis.clone()));
        }

        // Create SCD results for each individual
        let mut results = std::collections::HashMap::new();
        
        let scd_criteria = ScdCriteria::new();

        for (pnr, diags) in diagnoses_by_pnr {
            let mut result = ScdResult::new(pnr);

            for diagnosis in &diags {
                if diagnosis.is_scd {
                    // Add SCD diagnosis to result
                    let category = crate::models::diagnosis::get_scd_category_for_code(&diagnosis.diagnosis_code);
                    let is_congenital = scd_criteria.is_congenital(&diagnosis.diagnosis_code);
                    result.add_scd_diagnosis(diagnosis.clone(), category, is_congenital);
                }
            }

            results.insert(result.pnr.clone(), result);
        }

        results
    }
}