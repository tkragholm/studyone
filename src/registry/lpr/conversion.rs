//! LPR registry model conversion implementations
//!
//! This module implements bidirectional conversion between LPR registry data
//! and domain models (Diagnosis).

use crate::models::diagnosis::{Diagnosis, ScdCriteria, ScdResult};

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
    fn process_scd_results(
        &self,
        diagnoses: &[Diagnosis],
    ) -> std::collections::HashMap<String, ScdResult> {
        // Group diagnoses by individual
        let mut diagnoses_by_pnr: std::collections::HashMap<
            String,
            Vec<std::sync::Arc<Diagnosis>>,
        > = std::collections::HashMap::new();

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
                    let category = crate::models::diagnosis::get_scd_category_for_code(
                        &diagnosis.diagnosis_code,
                    );
                    let is_congenital = scd_criteria.is_congenital(&diagnosis.diagnosis_code);
                    result.add_scd_diagnosis(diagnosis.clone(), category, is_congenital);
                }
            }

            results.insert(result.pnr.clone(), result);
        }

        results
    }
}
