//! Child entity model
//!
//! This module contains the Child model, which represents a child in the study.
//! Children have specific attributes related to health conditions, birth details,
//! and can be associated with severe chronic diseases (SCD).

use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::core::Individual;
use crate::models::core::traits::{ArrowSchema, EntityModel, HealthStatus};
use crate::models::core::types::{DiseaseOrigin, DiseaseSeverity, ScdCategory};
use crate::models::health::Diagnosis;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use std::collections::HashMap;
use std::sync::Arc;

/// Representation of a child with health-related attributes
/// Function for creating a default Individual Arc for deserialization
fn default_individual() -> Arc<Individual> {
    Arc::new(Individual::new("placeholder".to_string(), None))
}

/// Representation of a child with health-related attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Child {
    /// The underlying Individual entity
    #[serde(skip, default = "default_individual")]
    // Skip serializing/deserializing with default
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
    #[serde(skip)] // Skip serializing/deserializing diagnoses for now
    pub diagnoses: Vec<Arc<Diagnosis>>,
    /// Birth order among siblings (1 = first born)
    pub birth_order: Option<i32>,
}

impl Child {
    /// Create a new Child from an Individual
    #[must_use]
    pub const fn from_individual(individual: Arc<Individual>) -> Self {
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

    // /// Create a Child directly from a registry record
    // pub fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
    //     // First create an Individual from the registry record
    //     if let Some(individual) = Individual::from_registry_record(batch, row)? {
    //         let mut child = Self::from_individual(Arc::new(individual));

    //         // Enhance with child-specific registry data
    //         child.enhance_from_registry(batch, row)?;

    //         Ok(Some(child))
    //     } else {
    //         Ok(None)
    //     }
    // }

    // /// Enhance this Child with data from a registry record
    // pub fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
    //     use crate::registry::detect::detect_registry_type;
    //     use crate::utils::field_extractors::extract_int32;

    //     let mut enhanced = false;

    //     // First enhance the underlying Individual
    //     let mut individual = Individual::clone(&self.individual);
    //     let individual_enhanced = individual.enhance_from_registry(batch, row)?;

    //     if individual_enhanced {
    //         // Update our Individual reference if it was enhanced
    //         self.individual = Arc::new(individual);
    //         enhanced = true;
    //     }

    //     // Detect registry type to add Child-specific fields
    //     let registry_type = detect_registry_type(batch);

    //     // MFR registry contains birth-related information
    //     if registry_type.as_str() == "MFR" {
    //         // Extract birth-related fields if they're not already set
    //         if self.birth_weight.is_none() {
    //             if let Ok(Some(weight)) = extract_int32(batch, row, "VAEGT", false) {
    //                 self.birth_weight = Some(weight);
    //                 enhanced = true;
    //             }
    //         }

    //         if self.gestational_age.is_none() {
    //             if let Ok(Some(ga)) = extract_int32(batch, row, "SVLENGTH", false) {
    //                 self.gestational_age = Some(ga);
    //                 enhanced = true;
    //             }
    //         }

    //         if self.apgar_score.is_none() {
    //             if let Ok(Some(apgar)) = extract_int32(batch, row, "APGAR5", false) {
    //                 self.apgar_score = Some(apgar);
    //                 enhanced = true;
    //             }
    //         }
    //     }

    //     // LPR registry contains diagnosis information that could affect SCD status
    //     if registry_type.as_str() == "LPR" {
    //         // Extract diagnoses if applicable
    //         // This would be more complex and would require building Diagnosis objects
    //         // and potentially updating SCD status
    //         // For now, we'll just leave this as a placeholder
    //     }

    //     Ok(enhanced)
    // }

    /// Create Child models from a batch of registry records using `serde_arrow`
    pub fn from_registry_batch_with_serde_arrow(batch: &RecordBatch) -> Result<Vec<Self>> {
        // First create Individuals from the registry batch
        let individuals = Individual::from_registry_batch_with_serde_arrow(batch)?;

        // Then convert those Individuals to Children and enhance them
        let mut children = Vec::with_capacity(individuals.len());
        for individual in individuals {
            let child = Self::from_individual(Arc::new(individual));
            children.push(child);
        }

        Ok(children)
    }

    /// Get a reference to the underlying Individual
    #[must_use]
    pub fn individual(&self) -> &Individual {
        self.individual.as_ref()
    }

    /// Set birth details
    #[must_use]
    pub const fn with_birth_details(
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
    pub const fn with_birth_order(mut self, birth_order: i32) -> Self {
        self.birth_order = Some(birth_order);
        self
    }

    /// Mark as having SCD with details
    #[must_use]
    pub const fn with_scd(
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
    pub const fn with_hospitalizations(mut self, hospitalizations_per_year: f64) -> Self {
        self.hospitalizations_per_year = Some(hospitalizations_per_year);
        self
    }

    /// Mark as an index case
    #[must_use]
    pub const fn as_index_case(mut self) -> Self {
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
    pub const fn has_scd(&self) -> bool {
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
    pub const fn is_eligible_case(&self) -> bool {
        self.has_severe_chronic_disease
    }

    /// Check if this child is eligible to be a control based on SCD status
    #[must_use]
    pub const fn is_eligible_control(&self) -> bool {
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
}

// Implement EntityModel trait
impl EntityModel for Child {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.individual().pnr
    }

    fn key(&self) -> String {
        self.individual().pnr.clone()
    }
}

// Delegate HealthStatus to the underlying Individual
impl HealthStatus for Child {
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        self.individual().age_at(reference_date)
    }

    fn was_alive_at(&self, date: &NaiveDate) -> bool {
        self.individual().was_alive_at(date)
    }

    fn was_resident_at(&self, date: &NaiveDate) -> bool {
        self.individual().was_resident_at(date)
    }
}

// Registry-specific implementations have been moved to registry_aware_models.rs

// Implement ArrowSchema trait
impl ArrowSchema for Child {
    /// Get the Arrow schema for Child records using `serde_arrow`
    fn schema() -> Schema {
        let sample =
            Self::from_individual(Arc::new(Individual::new("1234567890".to_string(), None)));

        // Use serde_arrow to generate schema from sample
        let fields = Vec::<arrow::datatypes::FieldRef>::from_samples(
            &[sample],
            TracingOptions::default().allow_null_fields(true),
        )
        .expect("Failed to create schema from sample");

        Schema::new(fields)
    }

    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use registry-aware from_registry_batch_with_serde_arrow which handles Individual creation
        Self::from_registry_batch_with_serde_arrow(batch)
    }

    fn to_record_batch(children: &[Self]) -> Result<RecordBatch> {
        // Generate schema from samples
        let fields = Vec::<arrow::datatypes::FieldRef>::from_samples(
            children,
            TracingOptions::default().allow_null_fields(true),
        )
        .map_err(|e| anyhow::anyhow!("Schema generation error: {}", e))?;

        // Convert to record batch - needs to be a reference to a slice
        serde_arrow::to_record_batch(&fields, &children)
            .map_err(|e| anyhow::anyhow!("Serialization error: {}", e))
    }
}

/// A collection of children that can be efficiently queried
#[derive(Debug, Default)]
pub struct ChildCollection {
    /// Children indexed by PNR
    children: HashMap<String, Arc<Child>>,
}

impl ChildCollection {
    /// Create a new empty `ChildCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
        }
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

    /// Count children with SCD
    #[must_use]
    pub fn scd_count(&self) -> usize {
        self.children_with_scd().len()
    }
}

// Implement ModelCollection trait
impl ModelCollection<Child> for ChildCollection {
    fn add(&mut self, child: Child) {
        let pnr = child.individual().pnr.clone();
        let child_arc = Arc::new(child);
        self.children.insert(pnr, child_arc);
    }

    fn get(&self, id: &String) -> Option<Arc<Child>> {
        self.children.get(id).cloned()
    }

    fn all(&self) -> Vec<Arc<Child>> {
        self.children.values().cloned().collect()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Child>>
    where
        F: Fn(&Child) -> bool,
    {
        self.children
            .values()
            .filter(|child| predicate(child))
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.children.len()
    }
}
