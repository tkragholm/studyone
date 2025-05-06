//! LPR Registry to Diagnosis Adapter
//!
//! This module contains adapters that map LPR registry data to Diagnosis domain models.
//! Supports both LPR2 and LPR3 registry formats.

use super::RegistryAdapter;
use crate::error::{Error, Result};
use crate::models::diagnosis::{Diagnosis, DiagnosisType, ScdResult};
use arrow::array::{Array, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// ICD-10 SCD classification criteria
struct ScdCriteria {
    /// ICD-10 codes that qualify as severe chronic disease
    scd_codes: HashSet<String>,
    /// ICD-10 code patterns (prefixes) that qualify as severe chronic disease
    scd_patterns: Vec<String>,
    /// Severity mappings for specific diagnoses
    severity_mappings: HashMap<String, i32>,
    /// Default severity for SCD diagnoses without specific mapping
    default_severity: i32,
}

impl ScdCriteria {
    /// Create a new SCD criteria set with defaults
    fn new() -> Self {
        let mut scd_codes = HashSet::new();
        // Examples of specific ICD-10 codes considered severe chronic diseases
        scd_codes.insert("E10".to_string()); // Type 1 diabetes
        scd_codes.insert("G40".to_string()); // Epilepsy
        scd_codes.insert("Q90".to_string()); // Down syndrome
        scd_codes.insert("C50".to_string()); // Malignant neoplasm of breast

        let mut scd_patterns = Vec::new();
        // Examples of ICD-10 code patterns for SCD categories
        scd_patterns.push("C".to_string()); // All cancers
        scd_patterns.push("D80".to_string()); // Immunodeficiency
        scd_patterns.push("G71".to_string()); // Primary disorders of muscles
        scd_patterns.push("Q".to_string()); // Congenital malformations

        let mut severity_mappings = HashMap::new();
        // Examples of severity classifications (1=mild, 2=moderate, 3=severe)
        severity_mappings.insert("E10".to_string(), 2); // Type 1 diabetes - moderate
        severity_mappings.insert("G40".to_string(), 2); // Epilepsy - moderate
        severity_mappings.insert("C".to_string(), 3); // Cancer - severe
        severity_mappings.insert("Q90".to_string(), 3); // Down syndrome - severe

        Self {
            scd_codes,
            scd_patterns,
            severity_mappings,
            default_severity: 2, // Default to moderate severity
        }
    }

    /// Check if an ICD-10 code is classified as a severe chronic disease
    fn is_scd(&self, code: &str) -> bool {
        if self.scd_codes.contains(code) {
            return true;
        }

        for pattern in &self.scd_patterns {
            if code.starts_with(pattern) {
                return true;
            }
        }

        false
    }

    /// Get severity for an ICD-10 code
    fn get_severity(&self, code: &str) -> i32 {
        // Check for exact code match
        if let Some(severity) = self.severity_mappings.get(code) {
            return *severity;
        }

        // Check for pattern match
        for (pattern, severity) in &self.severity_mappings {
            if code.starts_with(pattern) {
                return *severity;
            }
        }

        self.default_severity
    }

    /// Check if a diagnosis is congenital (based on ICD-10 chapter)
    fn is_congenital(&self, code: &str) -> bool {
        // Q codes are congenital malformations
        code.starts_with('Q')
    }
}

/// Base adapter for LPR registry data
pub struct LprBaseAdapter {
    /// Mapping of ICD-10 codes to SCD status
    scd_criteria: ScdCriteria,
}

impl LprBaseAdapter {
    /// Create a new LPR adapter
    #[must_use]
    pub fn new() -> Self {
        Self {
            scd_criteria: ScdCriteria::new(),
        }
    }

    /// Create a diagnosis from ICD-10 code and other data
    fn create_diagnosis(
        &self,
        individual_pnr: String,
        diagnosis_code: String,
        diagnosis_type: DiagnosisType,
        diagnosis_date: Option<NaiveDate>,
    ) -> Diagnosis {
        let is_scd = self.scd_criteria.is_scd(&diagnosis_code);
        let severity = if is_scd {
            self.scd_criteria.get_severity(&diagnosis_code)
        } else {
            1 // Non-SCD diagnoses have minimal severity
        };

        let mut diagnosis = Diagnosis::new(
            individual_pnr,
            diagnosis_code,
            diagnosis_type,
            diagnosis_date,
        );

        if is_scd {
            diagnosis = diagnosis.as_scd(severity);
        }

        diagnosis
    }

    /// Process SCD results from a list of diagnoses
    pub fn process_scd_results(&self, diagnoses: &[Diagnosis]) -> HashMap<String, ScdResult> {
        // Group diagnoses by individual
        let mut diagnoses_by_pnr: HashMap<String, Vec<Arc<Diagnosis>>> = HashMap::new();

        for diagnosis in diagnoses {
            diagnoses_by_pnr
                .entry(diagnosis.individual_pnr.clone())
                .or_default()
                .push(Arc::new(diagnosis.clone()));
        }

        // Create SCD results for each individual
        let mut results = HashMap::new();

        for (pnr, diags) in diagnoses_by_pnr {
            let mut result = ScdResult::new(pnr);

            for diagnosis in diags.iter() {
                if diagnosis.is_scd {
                    // Add SCD diagnosis to result
                    let category = self.get_category_for_code(&diagnosis.diagnosis_code);
                    let is_congenital = self.scd_criteria.is_congenital(&diagnosis.diagnosis_code);
                    result.add_scd_diagnosis(diagnosis.clone(), category, is_congenital);
                }

                // We could also count hospitalizations here if we had that data
            }

            results.insert(result.pnr.clone(), result);
        }

        results
    }

    /// Map ICD-10 code to SCD category
    fn get_category_for_code(&self, code: &str) -> u8 {
        // Simple mapping based on ICD-10 chapter
        if code.starts_with('C') || code.starts_with('D') && code.len() >= 3 && &code[1..3] <= "48"
        {
            1 // Blood/neoplasm
        } else if (code.starts_with('D')
            && code.len() >= 3
            && &code[1..3] >= "50"
            && &code[1..3] <= "89")
            || code.starts_with("M35")
            || code.starts_with("M30")
        {
            2 // Immune
        } else if code.starts_with('E') {
            3 // Endocrine
        } else if code.starts_with('G') {
            4 // Neurological
        } else if code.starts_with('I') {
            5 // Cardiovascular
        } else if code.starts_with('J') {
            6 // Respiratory
        } else if code.starts_with('K') {
            7 // Gastrointestinal
        } else if code.starts_with('M') && code != "M35" && !code.starts_with("M30") {
            8 // Musculoskeletal
        } else if code.starts_with('N') {
            9 // Renal
        } else if code.starts_with('Q') {
            10 // Congenital
        } else {
            0 // Other
        }
    }
}

impl Default for LprBaseAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapter for converting LPR2 (DIAG) registry data to Diagnosis models
pub struct Lpr2DiagAdapter {
    base: LprBaseAdapter,
    pnr_lookup: HashMap<String, String>, // Maps record IDs to PNRs
}

impl Lpr2DiagAdapter {
    /// Create a new LPR2 adapter with a lookup of record IDs to PNRs
    #[must_use]
    pub fn new(pnr_lookup: HashMap<String, String>) -> Self {
        Self {
            base: LprBaseAdapter::new(),
            pnr_lookup,
        }
    }
}

impl RegistryAdapter<Diagnosis> for Lpr2DiagAdapter {
    /// Convert an LPR2 DIAG `RecordBatch` to a vector of Diagnosis objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        // This is a static implementation with no PNR lookup
        // In practice, it's better to use the constructor to provide the lookup
        Err(anyhow::anyhow!(
            "Lpr2DiagAdapter requires a pnr_lookup. Use Lpr2DiagAdapter::new() constructor instead."
        ))
    }

    /// Apply additional transformations to the Diagnosis models
    fn transform(models: &mut [Diagnosis]) -> Result<()> {
        // No additional transformations needed
        Ok(())
    }
}

impl Lpr2DiagAdapter {
    /// Process an LPR2 batch and create Diagnosis objects
    pub fn process_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        // Get column indices
        let record_idx = batch
            .schema()
            .index_of("RECNUM")
            .map_err(|_| Error::ColumnNotFound {
                column: "RECNUM".to_string(),
            })?;

        let diag_idx = batch
            .schema()
            .index_of("C_DIAG")
            .map_err(|_| Error::ColumnNotFound {
                column: "C_DIAG".to_string(),
            })?;

        let diag_type_idx =
            batch
                .schema()
                .index_of("C_DIAGTYPE")
                .map_err(|_| Error::ColumnNotFound {
                    column: "C_DIAGTYPE".to_string(),
                })?;

        let date_idx = batch.schema().index_of("LEVERANCEDATO").ok();

        // Cast columns to their appropriate types
        let record_array = batch
            .column(record_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "RECNUM".to_string(),
                expected: "String".to_string(),
            })?;

        let diag_array = batch
            .column(diag_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "C_DIAG".to_string(),
                expected: "String".to_string(),
            })?;

        let diag_type_array = batch
            .column(diag_type_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "C_DIAGTYPE".to_string(),
                expected: "String".to_string(),
            })?;

        let date_array = if let Some(idx) = date_idx {
            Some(
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| Error::InvalidDataType {
                        column: "LEVERANCEDATO".to_string(),
                        expected: "Date32".to_string(),
                    })?,
            )
        } else {
            None
        };

        let mut diagnoses = Vec::new();

        // Process each row in the batch
        for i in 0..batch.num_rows() {
            let record_id = record_array.value(i).to_string();

            // Skip if we don't have this record in our PNR lookup
            if let Some(pnr) = self.pnr_lookup.get(&record_id) {
                let diagnosis_code = if !diag_array.is_null(i) {
                    diag_array.value(i).to_string()
                } else {
                    continue; // Skip rows without a diagnosis code
                };

                let diagnosis_type = if !diag_type_array.is_null(i) {
                    DiagnosisType::from(diag_type_array.value(i))
                } else {
                    DiagnosisType::Other
                };

                let diagnosis_date = if let Some(array) = &date_array {
                    if !array.is_null(i) {
                        Some(
                            NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .checked_add_days(chrono::Days::new(array.value(i) as u64))
                                .unwrap(),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                };

                let diagnosis = self.base.create_diagnosis(
                    pnr.clone(),
                    diagnosis_code,
                    diagnosis_type,
                    diagnosis_date,
                );

                diagnoses.push(diagnosis);
            }
        }

        Ok(diagnoses)
    }
}

/// Adapter for converting LPR3 (Diagnoser) registry data to Diagnosis models
pub struct Lpr3DiagnoserAdapter {
    base: LprBaseAdapter,
    pnr_lookup: HashMap<String, String>, // Maps kontakt IDs to PNRs
}

impl Lpr3DiagnoserAdapter {
    /// Create a new LPR3 adapter with a lookup of kontakt IDs to PNRs
    #[must_use]
    pub fn new(pnr_lookup: HashMap<String, String>) -> Self {
        Self {
            base: LprBaseAdapter::new(),
            pnr_lookup,
        }
    }
}

impl RegistryAdapter<Diagnosis> for Lpr3DiagnoserAdapter {
    /// Convert an LPR3 Diagnoser `RecordBatch` to a vector of Diagnosis objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        // This is a static implementation with no PNR lookup
        // In practice, it's better to use the constructor to provide the lookup
        Err(anyhow::anyhow!(
            "Lpr3DiagnoserAdapter requires a pnr_lookup. Use Lpr3DiagnoserAdapter::new() constructor instead."
        ))
    }

    /// Apply additional transformations to the Diagnosis models
    fn transform(models: &mut [Diagnosis]) -> Result<()> {
        // No additional transformations needed
        Ok(())
    }
}

impl Lpr3DiagnoserAdapter {
    /// Process an LPR3 batch and create Diagnosis objects
    pub fn process_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        // Get column indices
        let kontakt_idx =
            batch
                .schema()
                .index_of("DW_EK_KONTAKT")
                .map_err(|_| Error::ColumnNotFound {
                    column: "DW_EK_KONTAKT".to_string(),
                })?;

        let diag_idx =
            batch
                .schema()
                .index_of("diagnosekode")
                .map_err(|_| Error::ColumnNotFound {
                    column: "diagnosekode".to_string(),
                })?;

        let diag_type_idx =
            batch
                .schema()
                .index_of("diagnosetype")
                .map_err(|_| Error::ColumnNotFound {
                    column: "diagnosetype".to_string(),
                })?;

        let afkraeftet_idx = batch.schema().index_of("senere_afkraeftet").ok();

        // Cast columns to their appropriate types
        let kontakt_array = batch
            .column(kontakt_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "DW_EK_KONTAKT".to_string(),
                expected: "String".to_string(),
            })?;

        let diag_array = batch
            .column(diag_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "diagnosekode".to_string(),
                expected: "String".to_string(),
            })?;

        let diag_type_array = batch
            .column(diag_type_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "diagnosetype".to_string(),
                expected: "String".to_string(),
            })?;

        let afkraeftet_array = if let Some(idx) = afkraeftet_idx {
            Some(
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::InvalidDataType {
                        column: "senere_afkraeftet".to_string(),
                        expected: "String".to_string(),
                    })?,
            )
        } else {
            None
        };

        let mut diagnoses = Vec::new();

        // Process each row in the batch
        for i in 0..batch.num_rows() {
            let kontakt_id = kontakt_array.value(i).to_string();

            // Skip if we don't have this kontakt ID in our PNR lookup
            if let Some(pnr) = self.pnr_lookup.get(&kontakt_id) {
                // Skip if diagnosis was later disproven
                if let Some(array) = &afkraeftet_array {
                    if !array.is_null(i) && array.value(i) == "JA" {
                        continue;
                    }
                }

                let diagnosis_code = if !diag_array.is_null(i) {
                    diag_array.value(i).to_string()
                } else {
                    continue; // Skip rows without a diagnosis code
                };

                let diagnosis_type = if !diag_type_array.is_null(i) {
                    match diag_type_array.value(i) {
                        "A" => DiagnosisType::Primary,
                        "B" => DiagnosisType::Secondary,
                        _ => DiagnosisType::Other,
                    }
                } else {
                    DiagnosisType::Other
                };

                // LPR3 doesn't have direct date fields, we would need to join with kontakter table
                // For now, we leave the date as None
                let diagnosis_date = None;

                let diagnosis = self.base.create_diagnosis(
                    pnr.clone(),
                    diagnosis_code,
                    diagnosis_type,
                    diagnosis_date,
                );

                diagnoses.push(diagnosis);
            }
        }

        Ok(diagnoses)
    }
}

/// Combined adapter that can process both LPR2 and LPR3 data
pub struct LprCombinedAdapter {
    lpr2_adapter: Option<Lpr2DiagAdapter>,
    lpr3_adapter: Option<Lpr3DiagnoserAdapter>,
    base: LprBaseAdapter,
}

impl LprCombinedAdapter {
    /// Create a new combined adapter with lookups for both LPR2 and LPR3
    #[must_use]
    pub fn new(
        lpr2_lookup: Option<HashMap<String, String>>,
        lpr3_lookup: Option<HashMap<String, String>>,
    ) -> Self {
        let lpr2_adapter = lpr2_lookup.map(Lpr2DiagAdapter::new);
        let lpr3_adapter = lpr3_lookup.map(Lpr3DiagnoserAdapter::new);

        Self {
            lpr2_adapter,
            lpr3_adapter,
            base: LprBaseAdapter::new(),
        }
    }

    /// Process an LPR2 batch
    pub fn process_lpr2_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        if let Some(adapter) = &self.lpr2_adapter {
            adapter.process_batch(batch)
        } else {
            Err(anyhow::Error::msg("LPR2 adapter not initialized"))
        }
    }

    /// Process an LPR3 batch
    pub fn process_lpr3_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        if let Some(adapter) = &self.lpr3_adapter {
            adapter.process_batch(batch)
        } else {
            Err(anyhow::Error::msg("LPR3 adapter not initialized"))
        }
    }

    /// Combine diagnoses from multiple sources and create SCD results
    pub fn process_scd_results(&self, diagnoses: &[Diagnosis]) -> HashMap<String, ScdResult> {
        self.base.process_scd_results(diagnoses)
    }
}
