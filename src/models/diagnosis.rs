//! Diagnosis entity model
//!
//! This module contains the Diagnosis model, representing health diagnoses in the study.
//! Diagnoses are used to determine severe chronic disease (SCD) status and
//! categorize conditions by type and severity.

use crate::error::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Type of diagnosis (primary or secondary)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosisType {
    /// Primary (main) diagnosis
    Primary,
    /// Secondary diagnosis
    Secondary,
    /// Other or unknown type
    Other,
}

impl From<&str> for DiagnosisType {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "primary" | "main" | "a" => DiagnosisType::Primary,
            "secondary" | "b" => DiagnosisType::Secondary,
            _ => DiagnosisType::Other,
        }
    }
}

impl From<i32> for DiagnosisType {
    fn from(value: i32) -> Self {
        match value {
            1 => DiagnosisType::Primary,
            2 => DiagnosisType::Secondary,
            _ => DiagnosisType::Other,
        }
    }
}

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
    pub fn new(
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
    pub fn as_scd(mut self, severity: i32) -> Self {
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

    /// Get the Arrow schema for Diagnosis records
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("individual_pnr", DataType::Utf8, false),
            Field::new("diagnosis_code", DataType::Utf8, false),
            Field::new("diagnosis_type", DataType::Int32, false),
            Field::new("diagnosis_date", DataType::Date32, true),
            Field::new("is_scd", DataType::Boolean, false),
            Field::new("severity", DataType::Int32, false),
        ])
    }

    /// Convert a vector of Diagnosis objects to a `RecordBatch`
    pub fn to_record_batch(_diagnoses: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
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
    pub fn new(pnr: String) -> Self {
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
    pub fn add_hospitalizations(&mut self, count: i32) {
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
    pub fn hospitalization_severity(&self) -> i32 {
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
#[derive(Debug)]
pub struct DiagnosisCollection {
    /// Diagnoses by individual PNR
    diagnoses_by_pnr: HashMap<String, Vec<Arc<Diagnosis>>>,
    /// SCD results by individual PNR
    scd_results: HashMap<String, ScdResult>,
}

impl Default for DiagnosisCollection {
    fn default() -> Self {
        Self::new()
    }
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

    /// Add a diagnosis to the collection
    pub fn add_diagnosis(&mut self, diagnosis: Diagnosis) {
        let pnr = diagnosis.individual_pnr.clone();
        let diagnosis_arc = Arc::new(diagnosis);

        self.diagnoses_by_pnr
            .entry(pnr)
            .or_default()
            .push(diagnosis_arc);
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
