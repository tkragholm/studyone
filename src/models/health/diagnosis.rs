//! Diagnosis entity model
//!
//! This module contains the Diagnosis model, representing health diagnoses in the study.
//! Diagnoses are used to determine severe chronic disease (SCD) status and
//! categorize conditions by type and severity.
//!
//! Also includes SCD classification criteria and utilities for handling diagnosis data.

use crate::common::traits::{LprRegistry, RegistryAware};
use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::core::traits::ArrowSchema;
use crate::models::core::traits::EntityModel;
use crate::models::core::types::DiagnosisType;
use crate::utils::arrow::array_utils::{downcast_array, get_column};
use arrow::array::{Array, Date32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Representation of a medical diagnosis
#[derive(Debug, Clone)]
pub struct Diagnosis {
    /// PNR of the individual with the diagnosis
    pub individual_pnr: String,
    /// ICD-10 diagnosis code
    pub diagnosis_code: String,
    /// Type of diagnosis (primary/secondary)
    pub diagnosis_type: DiagnosisType,
    /// Date when the diagnosis was made
    pub diagnosis_date: Option<NaiveDate>,
    /// Whether this diagnosis is classified as a Severe Chronic Disease
    pub is_scd: bool,
    /// Severity score (1-3, with 3 being most severe)
    pub severity: i32,
}

impl Diagnosis {
    /// Create a new diagnosis
    #[must_use]
    pub const fn new(
        individual_pnr: String,
        diagnosis_code: String,
        diagnosis_type: DiagnosisType,
        diagnosis_date: Option<NaiveDate>,
    ) -> Self {
        Self {
            individual_pnr,
            diagnosis_code,
            diagnosis_type,
            diagnosis_date,
            is_scd: false,
            severity: 1,
        }
    }

    /// Set the diagnosis as a Severe Chronic Disease
    #[must_use]
    pub const fn as_scd(mut self, severity: i32) -> Self {
        self.is_scd = true;
        self.severity = severity;
        self
    }

    /// Check if this diagnosis is part of a specific ICD-10 chapter
    #[must_use]
    pub fn is_in_chapter(&self, chapter: &str) -> bool {
        if self.diagnosis_code.len() < 3 {
            return false;
        }

        let first_char = self.diagnosis_code.chars().next().unwrap();

        match chapter.to_uppercase().as_str() {
            "I" => ('A'..='B').contains(&first_char), // Infectious diseases
            "II" => first_char == 'C' || first_char == 'D', // Neoplasms
            "III" => first_char == 'D',               // Blood disorders
            "IV" => first_char == 'E',                // Endocrine disorders
            "V" => first_char == 'F',                 // Mental disorders
            "VI" => first_char == 'G',                // Nervous system
            "VII" => first_char == 'H',               // Eye and ear
            "VIII" => first_char == 'H',              // Eye and ear (part 2)
            "IX" => first_char == 'I',                // Circulatory system
            "X" => first_char == 'J',                 // Respiratory system
            "XI" => first_char == 'K',                // Digestive system
            "XII" => first_char == 'L',               // Skin
            "XIII" => first_char == 'M',              // Musculoskeletal
            "XIV" => first_char == 'N',               // Genitourinary
            "XV" => first_char == 'O',                // Pregnancy and childbirth
            "XVI" => first_char == 'P',               // Perinatal conditions
            "XVII" => first_char == 'Q',              // Congenital malformations
            "XVIII" => first_char == 'R',             // Symptoms and signs
            "XIX" => ('S'..='T').contains(&first_char), // Injury and poisoning
            "XX" => ('V'..='Y').contains(&first_char), // External causes
            "XXI" => first_char == 'Z',               // Factors influencing health
            "XXII" => first_char == 'U',              // Special codes
            _ => false,
        }
    }

    /// Check if this diagnosis matches a specific code or pattern
    #[must_use]
    pub fn matches_code(&self, pattern: &str) -> bool {
        if pattern.ends_with('*') {
            // Prefix matching
            let prefix = pattern.trim_end_matches('*');
            self.diagnosis_code.starts_with(prefix)
        } else {
            // Exact matching
            self.diagnosis_code == pattern
        }
    }
}

// Implement EntityModel trait
impl EntityModel for Diagnosis {
    // We use a composite key since a person can have multiple diagnoses
    type Id = (String, String);

    fn id(&self) -> &Self::Id {
        // In a proper implementation, we would store the ID as a field
        // For now, use thread_local to avoid the static_mut_refs warning
        thread_local! {
            static DIAGNOSIS_ID: std::cell::RefCell<Option<(String, String)>> = const { std::cell::RefCell::new(None) };
        }

        // Using with_borrow_mut to update the thread-local value
        DIAGNOSIS_ID.with(|cell| {
            *cell.borrow_mut() = Some((self.individual_pnr.clone(), self.diagnosis_code.clone()));
        });

        // Return the ID as a static reference - this is a workaround and would be
        // better implemented with proper field storage in a real application
        static ID: (String, String) = (String::new(), String::new());
        &ID
    }

    fn key(&self) -> String {
        format!("{}:{}", self.individual_pnr, self.diagnosis_code)
    }
}

// Implement ArrowSchema trait
impl ArrowSchema for Diagnosis {
    /// Get the Arrow schema for Diagnosis records
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("individual_pnr", DataType::Utf8, false),
            Field::new("diagnosis_code", DataType::Utf8, false),
            Field::new("diagnosis_type", DataType::Int32, false),
            Field::new("diagnosis_date", DataType::Date32, true),
            Field::new("is_scd", DataType::Boolean, false),
            Field::new("severity", DataType::Int32, false),
        ])
    }

    fn from_record_batch(_batch: &RecordBatch) -> Result<Vec<Self>> {
        // Implementation would convert from Arrow arrays to Diagnosis objects
        unimplemented!("Conversion from RecordBatch to Diagnosis not yet implemented")
    }

    fn to_record_batch(_diagnoses: &[Self]) -> Result<RecordBatch> {
        // Implementation would convert from Diagnosis objects to Arrow arrays
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

// Implement RegistryAware trait for Diagnosis
impl RegistryAware for Diagnosis {
    fn registry_name() -> &'static str {
        "LPR"
    }

    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Delegate to LPR record conversion as it's the primary registry
        Self::from_lpr_record(batch, row)
    }

    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Delegate to LPR batch conversion as it's the primary registry
        Self::from_lpr_batch(batch)
    }
}

// Implement LprRegistry trait for Diagnosis
impl LprRegistry for Diagnosis {
    fn from_lpr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract required fields from the LPR record batch

        // Get PNR - first try to get it directly, but this might require a lookup in some LPR versions
        let pnr_opt = get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_opt {
            let string_array = downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                // If the PNR is null, cannot create a diagnosis
                return Ok(None);
            }
        } else {
            // If PNR column is missing, would need lookup - but for direct conversion
            // we expect the PNR to be available or mapped by the caller
            return Ok(None);
        };

        // Get diagnosis code
        let diag_code_opt = get_column(batch, "DIAG", &DataType::Utf8, false)?;
        let diagnosis_code = if let Some(array) = diag_code_opt {
            let string_array = downcast_array::<StringArray>(&array, "DIAG", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                // If the diagnosis code is null, cannot create a diagnosis
                return Ok(None);
            }
        } else {
            // If diagnosis column is missing, try alternate column names (LPR3 vs LPR2)
            let alt_diag_opt = get_column(batch, "C_DIAG", &DataType::Utf8, false)?;
            if let Some(array) = alt_diag_opt {
                let string_array = downcast_array::<StringArray>(&array, "C_DIAG", "String")?;
                if row < string_array.len() && !string_array.is_null(row) {
                    string_array.value(row).to_string()
                } else {
                    // If the diagnosis code is null, cannot create a diagnosis
                    return Ok(None);
                }
            } else {
                // If both diagnosis columns are missing, cannot create a diagnosis
                return Ok(None);
            }
        };

        // Get diagnosis type - A for primary, B for secondary
        let diag_type_opt = get_column(batch, "DIAGTYPE", &DataType::Utf8, false)?;
        let diagnosis_type = if let Some(array) = diag_type_opt {
            let string_array = downcast_array::<StringArray>(&array, "DIAGTYPE", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                let type_code = string_array.value(row);
                match type_code {
                    "A" => DiagnosisType::Primary,
                    "B" => DiagnosisType::Secondary,
                    _ => DiagnosisType::Other,
                }
            } else {
                // Default to other if not specified
                DiagnosisType::Other
            }
        } else {
            // If diagnosis type column is missing, check for integer type
            let alt_type_opt = get_column(batch, "C_DIAGTYPE", &DataType::Int32, false)?;
            if let Some(array) = alt_type_opt {
                let int_array = downcast_array::<Int32Array>(&array, "C_DIAGTYPE", "Int32")?;
                if row < int_array.len() && !int_array.is_null(row) {
                    let type_code = int_array.value(row);
                    match type_code {
                        1 => DiagnosisType::Primary,
                        2 => DiagnosisType::Secondary,
                        _ => DiagnosisType::Other,
                    }
                } else {
                    // Default to other if not specified
                    DiagnosisType::Other
                }
            } else {
                // If both diagnosis type columns are missing, default to other
                DiagnosisType::Other
            }
        };

        // Get diagnosis date (if available)
        let date_opt = get_column(batch, "INDDTO", &DataType::Date32, false)?;
        let diagnosis_date = if let Some(array) = date_opt {
            let date_array = downcast_array::<Date32Array>(&array, "INDDTO", "Date32")?;
            if row < date_array.len() && !date_array.is_null(row) {
                let days_since_epoch = date_array.value(row);
                Some(
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_days(chrono::Days::new(days_since_epoch as u64))
                        .unwrap(),
                )
            } else {
                None
            }
        } else {
            // Try alternate date column (LPR3)
            let alt_date_opt = get_column(batch, "D_INDDTO", &DataType::Date32, false)?;
            if let Some(array) = alt_date_opt {
                let date_array = downcast_array::<Date32Array>(&array, "D_INDDTO", "Date32")?;
                if row < date_array.len() && !date_array.is_null(row) {
                    let days_since_epoch = date_array.value(row);
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(days_since_epoch as u64))
                            .unwrap(),
                    )
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Create the diagnosis
        let mut diagnosis = Self::new(pnr, diagnosis_code, diagnosis_type, diagnosis_date);

        // Apply SCD classification
        let criteria = ScdCriteria::new();
        if criteria.is_scd(&diagnosis.diagnosis_code) {
            let severity = criteria.get_severity(&diagnosis.diagnosis_code);
            diagnosis = diagnosis.as_scd(severity);
        }

        Ok(Some(diagnosis))
    }

    fn from_lpr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut diagnoses = Vec::new();

        // Process each row in the batch
        for row in 0..batch.num_rows() {
            if let Ok(Some(diagnosis)) = Self::from_lpr_record(batch, row) {
                diagnoses.push(diagnosis);
            }
        }

        Ok(diagnoses)
    }
}

/// ICD-10 SCD classification criteria
#[derive(Debug, Clone)]
pub struct ScdCriteria {
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
    #[must_use]
    pub fn new() -> Self {
        let mut scd_codes = HashSet::new();
        // Examples of specific ICD-10 codes considered severe chronic diseases
        scd_codes.insert("E10".to_string()); // Type 1 diabetes
        scd_codes.insert("G40".to_string()); // Epilepsy
        scd_codes.insert("Q90".to_string()); // Down syndrome
        scd_codes.insert("C50".to_string()); // Malignant neoplasm of breast

        // Examples of ICD-10 code patterns for SCD categories
        let scd_patterns = vec![
            "C".to_string(),   // All cancers
            "D80".to_string(), // Immunodeficiency
            "G71".to_string(), // Primary disorders of muscles
            "Q".to_string(),   // Congenital malformations
        ];

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
    #[must_use]
    pub fn is_scd(&self, code: &str) -> bool {
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
    #[must_use]
    pub fn get_severity(&self, code: &str) -> i32 {
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
    #[must_use]
    pub fn is_congenital(&self, code: &str) -> bool {
        // Q codes are congenital malformations
        code.starts_with('Q')
    }
}

impl Default for ScdCriteria {
    fn default() -> Self {
        Self::new()
    }
}

/// Get SCD category for an ICD-10 code
///
/// Maps ICD-10 codes to SCD categories (1-10) based on disease type
#[must_use]
pub fn get_scd_category_for_code(code: &str) -> u8 {
    // Simple mapping based on ICD-10 chapter
    if code.starts_with('C') || code.starts_with('D') && code.len() >= 3 && &code[1..3] <= "48" {
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

/// SCD algorithm result for a specific individual
#[derive(Debug, Clone)]
pub struct ScdResult {
    /// Individual's PNR
    pub pnr: String,
    /// Whether the individual has any SCD
    pub has_scd: bool,
    /// Date of first SCD diagnosis
    pub first_scd_date: Option<NaiveDate>,
    /// SCD diagnoses with details
    pub scd_diagnoses: Vec<Arc<Diagnosis>>,
    /// Major SCD categories present (up to 10)
    pub scd_categories: Vec<u8>,
    /// Highest severity among diagnoses
    pub max_severity: i32,
    /// Whether any diagnosis is congenital
    pub has_congenital: bool,
    /// Total hospitalization count
    pub hospitalization_count: i32,
}

impl ScdResult {
    /// Create a new SCD result for an individual
    #[must_use]
    pub const fn new(pnr: String) -> Self {
        Self {
            pnr,
            has_scd: false,
            first_scd_date: None,
            scd_diagnoses: Vec::new(),
            scd_categories: Vec::new(),
            max_severity: 0,
            has_congenital: false,
            hospitalization_count: 0,
        }
    }

    /// Add an SCD diagnosis to the result
    pub fn add_scd_diagnosis(
        &mut self,
        diagnosis: Arc<Diagnosis>,
        category: u8,
        is_congenital: bool,
    ) {
        // Update SCD status
        self.has_scd = true;

        // Update first SCD date if needed
        if let Some(diagnosis_date) = diagnosis.diagnosis_date {
            if self.first_scd_date.is_none() || diagnosis_date < self.first_scd_date.unwrap() {
                self.first_scd_date = Some(diagnosis_date);
            }
        }

        // Update categories if not already present
        if !self.scd_categories.contains(&category) {
            self.scd_categories.push(category);
        }

        // Update max severity
        if diagnosis.severity > self.max_severity {
            self.max_severity = diagnosis.severity;
        }

        // Update congenital status
        if is_congenital {
            self.has_congenital = true;
        }

        // Add diagnosis to list
        self.scd_diagnoses.push(diagnosis);
    }

    /// Add hospitalization count
    pub const fn add_hospitalizations(&mut self, count: i32) {
        self.hospitalization_count += count;
    }

    /// Check if individual has a specific SCD category
    #[must_use]
    pub fn has_category(&self, category: u8) -> bool {
        self.scd_categories.contains(&category)
    }

    /// Get category count
    #[must_use]
    pub fn category_count(&self) -> usize {
        self.scd_categories.len()
    }

    /// Calculate hospitalization-based severity
    #[must_use]
    pub const fn hospitalization_severity(&self) -> i32 {
        if self.hospitalization_count >= 5 {
            3 // Severe
        } else if self.hospitalization_count >= 2 {
            2 // Moderate
        } else {
            1 // Mild
        }
    }

    /// Get combined severity score from multiple measures
    #[must_use]
    pub fn combined_severity(&self) -> i32 {
        let diagnosis_severity = self.max_severity;
        let hospitalization_severity = self.hospitalization_severity();
        let category_severity = if self.category_count() > 1 { 3 } else { 1 };

        // Take the maximum of the three severity measures
        diagnosis_severity
            .max(hospitalization_severity)
            .max(category_severity)
    }
}

/// A collection of diagnoses that can be efficiently queried
#[derive(Debug, Default)]
pub struct DiagnosisCollection {
    /// Diagnoses by individual PNR
    diagnoses_by_pnr: HashMap<String, Vec<Arc<Diagnosis>>>,
    /// SCD results by individual PNR
    scd_results: HashMap<String, ScdResult>,
}

impl DiagnosisCollection {
    /// Create a new empty `DiagnosisCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            diagnoses_by_pnr: HashMap::new(),
            scd_results: HashMap::new(),
        }
    }

    /// Get all diagnoses for an individual
    #[must_use]
    pub fn get_diagnoses(&self, pnr: &str) -> Vec<Arc<Diagnosis>> {
        self.diagnoses_by_pnr.get(pnr).cloned().unwrap_or_default()
    }

    /// Add an SCD result
    pub fn add_scd_result(&mut self, result: ScdResult) {
        self.scd_results.insert(result.pnr.clone(), result);
    }

    /// Get SCD result for an individual
    #[must_use]
    pub fn get_scd_result(&self, pnr: &str) -> Option<&ScdResult> {
        self.scd_results.get(pnr)
    }

    /// Get all individuals with SCD
    #[must_use]
    pub fn individuals_with_scd(&self) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| result.has_scd)
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }

    /// Get all individuals without SCD
    #[must_use]
    pub fn individuals_without_scd(&self) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| !result.has_scd)
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }

    /// Get individuals with a specific SCD category
    #[must_use]
    pub fn individuals_with_category(&self, category: u8) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| result.has_category(category))
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }

    /// Count individuals with SCD
    #[must_use]
    pub fn scd_count(&self) -> usize {
        self.individuals_with_scd().len()
    }

    /// Count individuals by severity level
    #[must_use]
    pub fn count_by_severity(&self) -> HashMap<i32, usize> {
        let mut counts = HashMap::new();

        for result in self.scd_results.values() {
            if result.has_scd {
                let severity = result.combined_severity();
                *counts.entry(severity).or_insert(0) += 1;
            }
        }

        counts
    }
}

// Implement ModelCollection trait
impl ModelCollection<Diagnosis> for DiagnosisCollection {
    fn add(&mut self, diagnosis: Diagnosis) {
        let pnr = diagnosis.individual_pnr.clone();
        let diagnosis_arc = Arc::new(diagnosis);

        self.diagnoses_by_pnr
            .entry(pnr)
            .or_default()
            .push(diagnosis_arc);
    }

    fn get(&self, id: &(String, String)) -> Option<Arc<Diagnosis>> {
        let (pnr, code) = id;

        if let Some(diagnoses) = self.diagnoses_by_pnr.get(pnr) {
            diagnoses
                .iter()
                .find(|diag| diag.diagnosis_code == *code)
                .cloned()
        } else {
            None
        }
    }

    fn all(&self) -> Vec<Arc<Diagnosis>> {
        let mut all_diagnoses = Vec::new();

        for diagnoses in self.diagnoses_by_pnr.values() {
            all_diagnoses.extend(diagnoses.iter().cloned());
        }

        all_diagnoses
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Diagnosis>>
    where
        F: Fn(&Diagnosis) -> bool,
    {
        let mut filtered = Vec::new();

        for diagnoses in self.diagnoses_by_pnr.values() {
            for diagnosis in diagnoses {
                if predicate(diagnosis) {
                    filtered.push(diagnosis.clone());
                }
            }
        }

        filtered
    }

    fn count(&self) -> usize {
        self.diagnoses_by_pnr.values().map(std::vec::Vec::len).sum()
    }
}
