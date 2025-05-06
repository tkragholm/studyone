//! Child entity model
//!
//! This module contains the Child model, which represents a child in the study.
//! Children have specific attributes related to health conditions, birth details,
//! and can be associated with severe chronic diseases (SCD).

use super::diagnosis::Diagnosis;
use super::individual::Individual;
use crate::error::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Severe Chronic Disease category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScdCategory {
    /// Blood disorders
    BloodDisorder,
    /// Immune system disorders
    ImmuneDisorder,
    /// Endocrine disorders
    EndocrineDisorder,
    /// Neurological disorders
    NeurologicalDisorder,
    /// Cardiovascular disorders
    CardiovascularDisorder,
    /// Respiratory disorders
    RespiratoryDisorder,
    /// Gastrointestinal disorders
    GastrointestinalDisorder,
    /// Musculoskeletal disorders
    MusculoskeletalDisorder,
    /// Renal disorders
    RenalDisorder,
    /// Congenital disorders
    CongenitalDisorder,
    /// No SCD category
    None,
}

impl From<i32> for ScdCategory {
    fn from(value: i32) -> Self {
        match value {
            1 => ScdCategory::BloodDisorder,
            2 => ScdCategory::ImmuneDisorder,
            3 => ScdCategory::EndocrineDisorder,
            4 => ScdCategory::NeurologicalDisorder,
            5 => ScdCategory::CardiovascularDisorder,
            6 => ScdCategory::RespiratoryDisorder,
            7 => ScdCategory::GastrointestinalDisorder,
            8 => ScdCategory::MusculoskeletalDisorder,
            9 => ScdCategory::RenalDisorder,
            10 => ScdCategory::CongenitalDisorder,
            _ => ScdCategory::None,
        }
    }
}

/// Disease severity classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiseaseSeverity {
    /// Mild conditions (e.g., asthma)
    Mild,
    /// Moderate conditions (most SCD algorithm conditions)
    Moderate,
    /// Severe conditions (e.g., cancer, organ transplantation)
    Severe,
    /// No disease or unknown severity
    None,
}

impl From<i32> for DiseaseSeverity {
    fn from(value: i32) -> Self {
        match value {
            1 => DiseaseSeverity::Mild,
            2 => DiseaseSeverity::Moderate,
            3 => DiseaseSeverity::Severe,
            _ => DiseaseSeverity::None,
        }
    }
}

/// Origin of the disease
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiseaseOrigin {
    /// Congenital disease (present at birth)
    Congenital,
    /// Acquired disease (developed after birth)
    Acquired,
    /// No disease or unknown origin
    None,
}

impl From<i32> for DiseaseOrigin {
    fn from(value: i32) -> Self {
        match value {
            1 => DiseaseOrigin::Congenital,
            2 => DiseaseOrigin::Acquired,
            _ => DiseaseOrigin::None,
        }
    }
}

/// Representation of a child with health-related attributes
#[derive(Debug, Clone)]
pub struct Child {
    /// The underlying Individual entity
    individual: Arc<Individual>,
    /// Birth weight in grams
    pub birth_weight: Option<i32>,
    /// Gestational age in weeks
    pub gestational_age: Option<i32>,
    /// Apgar score at 5 minutes
    pub apgar_score: Option<i32>,
    /// Whether the child has a severe chronic disease
    pub has_severe_chronic_disease: bool,
    /// First date of SCD diagnosis
    pub first_scd_date: Option<NaiveDate>,
    /// SCD category if applicable
    pub scd_category: ScdCategory,
    /// Disease severity classification
    pub disease_severity: DiseaseSeverity,
    /// Disease origin classification
    pub disease_origin: DiseaseOrigin,
    /// Number of hospitalizations per year (average)
    pub hospitalizations_per_year: Option<f64>,
    /// Whether this is an index case child (for matching purposes)
    pub is_index_case: bool,
    /// The diagnoses associated with this child
    pub diagnoses: Vec<Arc<Diagnosis>>,
    /// Birth order among siblings (1 = first born)
    pub birth_order: Option<i32>,
}

impl Child {
    /// Create a new Child from an Individual
    #[must_use]
    pub fn from_individual(individual: Arc<Individual>) -> Self {
        Self {
            individual,
            birth_weight: None,
            gestational_age: None,
            apgar_score: None,
            has_severe_chronic_disease: false,
            first_scd_date: None,
            scd_category: ScdCategory::None,
            disease_severity: DiseaseSeverity::None,
            disease_origin: DiseaseOrigin::None,
            hospitalizations_per_year: None,
            is_index_case: false,
            diagnoses: Vec::new(),
            birth_order: None,
        }
    }

    /// Get a reference to the underlying Individual
    #[must_use]
    pub fn individual(&self) -> &Individual {
        &self.individual
    }

    /// Set birth details
    #[must_use]
    pub fn with_birth_details(
        mut self,
        birth_weight: Option<i32>,
        gestational_age: Option<i32>,
        apgar_score: Option<i32>,
    ) -> Self {
        self.birth_weight = birth_weight;
        self.gestational_age = gestational_age;
        self.apgar_score = apgar_score;
        self
    }

    /// Set birth order
    #[must_use]
    pub fn with_birth_order(mut self, birth_order: i32) -> Self {
        self.birth_order = Some(birth_order);
        self
    }

    /// Mark as having SCD with details
    #[must_use]
    pub fn with_scd(
        mut self,
        scd_category: ScdCategory,
        first_scd_date: NaiveDate,
        disease_severity: DiseaseSeverity,
        disease_origin: DiseaseOrigin,
    ) -> Self {
        self.has_severe_chronic_disease = true;
        self.scd_category = scd_category;
        self.first_scd_date = Some(first_scd_date);
        self.disease_severity = disease_severity;
        self.disease_origin = disease_origin;
        self
    }

    /// Set hospitalization frequency
    #[must_use]
    pub fn with_hospitalizations(mut self, hospitalizations_per_year: f64) -> Self {
        self.hospitalizations_per_year = Some(hospitalizations_per_year);
        self
    }

    /// Mark as an index case
    #[must_use]
    pub fn as_index_case(mut self) -> Self {
        self.is_index_case = true;
        self
    }

    /// Add a diagnosis to this child
    pub fn add_diagnosis(&mut self, diagnosis: Arc<Diagnosis>) {
        // Update SCD status if this is an SCD diagnosis
        if diagnosis.is_scd {
            self.has_severe_chronic_disease = true;

            // Update first SCD date if needed
            if let Some(diagnosis_date) = diagnosis.diagnosis_date {
                if self.first_scd_date.is_none() || diagnosis_date < self.first_scd_date.unwrap() {
                    self.first_scd_date = Some(diagnosis_date);
                }
            }
        }

        self.diagnoses.push(diagnosis);
    }

    /// Check if the child has SCD
    #[must_use]
    pub fn has_scd(&self) -> bool {
        self.has_severe_chronic_disease
    }

    /// Check if the child had SCD at a specific date
    #[must_use]
    pub fn had_scd_at(&self, date: &NaiveDate) -> bool {
        if !self.has_severe_chronic_disease {
            return false;
        }

        if let Some(first_scd_date) = self.first_scd_date {
            first_scd_date <= *date
        } else {
            false
        }
    }

    /// Check if this child is eligible to be a case based on SCD status
    #[must_use]
    pub fn is_eligible_case(&self) -> bool {
        self.has_severe_chronic_disease
    }

    /// Check if this child is eligible to be a control based on SCD status
    #[must_use]
    pub fn is_eligible_control(&self) -> bool {
        !self.has_severe_chronic_disease
    }

    /// Calculate age at onset of SCD
    #[must_use]
    pub fn age_at_onset(&self) -> Option<i32> {
        if let (Some(_birth_date), Some(first_scd_date)) =
            (self.individual().birth_date, self.first_scd_date)
        {
            self.individual().age_at(&first_scd_date)
        } else {
            None
        }
    }

    /// Get the Arrow schema for Child records
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("birth_weight", DataType::Int32, true),
            Field::new("gestational_age", DataType::Int32, true),
            Field::new("apgar_score", DataType::Int32, true),
            Field::new("has_severe_chronic_disease", DataType::Boolean, false),
            Field::new("first_scd_date", DataType::Date32, true),
            Field::new("scd_category", DataType::Int32, false),
            Field::new("disease_severity", DataType::Int32, false),
            Field::new("disease_origin", DataType::Int32, false),
            Field::new("hospitalizations_per_year", DataType::Float64, true),
            Field::new("is_index_case", DataType::Boolean, false),
            Field::new("birth_order", DataType::Int32, true),
        ])
    }

    /// Convert a vector of Child objects to a `RecordBatch`
    pub fn to_record_batch(_children: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

/// A collection of children that can be efficiently queried
#[derive(Debug)]
pub struct ChildCollection {
    /// Children indexed by PNR
    children: HashMap<String, Arc<Child>>,
}

impl Default for ChildCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ChildCollection {
    /// Create a new empty `ChildCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
        }
    }

    /// Add a child to the collection
    pub fn add_child(&mut self, child: Child) {
        let pnr = child.individual().pnr.clone();
        let child_arc = Arc::new(child);
        self.children.insert(pnr, child_arc);
    }

    /// Get a child by PNR
    #[must_use]
    pub fn get_child(&self, pnr: &str) -> Option<Arc<Child>> {
        self.children.get(pnr).cloned()
    }

    /// Get all children in the collection
    #[must_use]
    pub fn all_children(&self) -> Vec<Arc<Child>> {
        self.children.values().cloned().collect()
    }

    /// Filter children by a predicate function
    pub fn filter<F>(&self, predicate: F) -> Vec<Arc<Child>>
    where
        F: Fn(&Child) -> bool,
    {
        self.children
            .values()
            .filter(|child| predicate(child))
            .cloned()
            .collect()
    }

    /// Get children with SCD
    #[must_use]
    pub fn children_with_scd(&self) -> Vec<Arc<Child>> {
        self.filter(Child::has_scd)
    }

    /// Get children without SCD (potential controls)
    #[must_use]
    pub fn children_without_scd(&self) -> Vec<Arc<Child>> {
        self.filter(|child| !child.has_scd())
    }

    /// Get children with SCD at a specific date
    #[must_use]
    pub fn children_with_scd_at(&self, date: &NaiveDate) -> Vec<Arc<Child>> {
        self.filter(|child| child.had_scd_at(date))
    }

    /// Get children with a specific SCD category
    #[must_use]
    pub fn children_with_scd_category(&self, category: ScdCategory) -> Vec<Arc<Child>> {
        self.filter(|child| child.scd_category == category)
    }

    /// Get children with a specific disease severity
    #[must_use]
    pub fn children_with_severity(&self, severity: DiseaseSeverity) -> Vec<Arc<Child>> {
        self.filter(|child| child.disease_severity == severity)
    }

    /// Get children marked as index cases
    #[must_use]
    pub fn index_cases(&self) -> Vec<Arc<Child>> {
        self.filter(|child| child.is_index_case)
    }

    /// Count total number of children in the collection
    #[must_use]
    pub fn count(&self) -> usize {
        self.children.len()
    }

    /// Count children with SCD
    #[must_use]
    pub fn scd_count(&self) -> usize {
        self.children_with_scd().len()
    }
}
