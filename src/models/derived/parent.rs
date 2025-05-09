//! Parent entity model
//!
//! This module contains the Parent model, which represents a parent in the study.
//! Parents have specific attributes related to socioeconomic status, employment,
//! and can be associated with children and families.

use crate::error::Result;
use crate::models::collections::ModelCollection;
use crate::models::core::Individual;
use crate::models::core::traits::{ArrowSchema, EntityModel, HealthStatus};
use crate::models::core::types::JobSituation;
use crate::models::economic::Income;
use crate::models::health::Diagnosis;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// A parent in the study with parental attributes
#[derive(Debug, Clone)]
pub struct Parent {
    /// The underlying Individual entity
    individual: Arc<Individual>,
    /// Employment status at index date (employed/unemployed)
    pub employment_status: bool,
    /// Job situation at index date
    pub job_situation: JobSituation,
    /// Whether the parent has a documented comorbidity
    pub has_comorbidity: bool,
    /// Pre-exposure income amount (inflation-adjusted, in DKK)
    pub pre_exposure_income: Option<f64>,
    /// Documented health conditions
    pub diagnoses: Vec<Arc<Diagnosis>>,
    /// Income trajectory data points
    pub income_data: Vec<Arc<Income>>,
}

impl Parent {
    /// Create a new Parent from an Individual
    #[must_use]
    pub const fn from_individual(individual: Arc<Individual>) -> Self {
        Self {
            individual,
            employment_status: false,
            job_situation: JobSituation::Other,
            has_comorbidity: false,
            pre_exposure_income: None,
            diagnoses: Vec::new(),
            income_data: Vec::new(),
        }
    }

    /// Create a new Parent from an Individual (alias for `from_individual`)
    #[must_use]
    pub const fn new(individual: Arc<Individual>) -> Self {
        Self::from_individual(individual)
    }

    /// Get a reference to the underlying Individual
    #[must_use]
    pub fn individual(&self) -> &Individual {
        self.individual.as_ref()
    }

    /// Set the employment status
    #[must_use]
    pub const fn with_employment_status(mut self, employed: bool) -> Self {
        self.employment_status = employed;
        self
    }

    /// Set the job situation
    #[must_use]
    pub const fn with_job_situation(mut self, job_situation: JobSituation) -> Self {
        self.job_situation = job_situation;
        self
    }

    /// Set the pre-exposure income
    #[must_use]
    pub const fn with_pre_exposure_income(mut self, income: f64) -> Self {
        self.pre_exposure_income = Some(income);
        self
    }

    /// Add a diagnosis to this parent
    pub fn add_diagnosis(&mut self, diagnosis: Arc<Diagnosis>) {
        self.diagnoses.push(diagnosis);
        // Update comorbidity status
        self.update_comorbidity_status();
    }

    /// Add an income data point
    pub fn add_income(&mut self, income: Arc<Income>) {
        self.income_data.push(income);
    }

    /// Update the comorbidity status based on diagnoses
    fn update_comorbidity_status(&mut self) {
        self.has_comorbidity = !self.diagnoses.is_empty();
    }

    /// Get income for a specific year
    #[must_use]
    pub fn income_for_year(&self, year: i32) -> Option<f64> {
        self.income_data
            .iter()
            .find(|income| income.year == year)
            .map(|income| income.amount)
    }

    /// Get income trajectory for a range of years
    #[must_use]
    pub fn income_trajectory(&self, start_year: i32, end_year: i32) -> HashMap<i32, f64> {
        let mut trajectory = HashMap::new();

        for year in start_year..=end_year {
            if let Some(amount) = self.income_for_year(year) {
                trajectory.insert(year, amount);
            }
        }

        trajectory
    }

    /// Check if parent had any diagnoses before a specific date
    #[must_use]
    pub fn had_diagnosis_before(&self, date: &NaiveDate) -> bool {
        self.diagnoses.iter().any(|diagnosis| {
            if let Some(diagnosis_date) = diagnosis.diagnosis_date {
                diagnosis_date < *date
            } else {
                false
            }
        })
    }
}

// Implement EntityModel trait
impl EntityModel for Parent {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.individual().pnr
    }

    fn key(&self) -> String {
        self.individual().pnr.clone()
    }
}

// Delegate HealthStatus to the underlying Individual
impl HealthStatus for Parent {
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

// Implement ArrowSchema trait
impl ArrowSchema for Parent {
    /// Get the Arrow schema for Parent records
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("employment_status", DataType::Boolean, false),
            Field::new("job_situation", DataType::Int32, false),
            Field::new("has_comorbidity", DataType::Boolean, false),
            Field::new("pre_exposure_income", DataType::Float64, true),
        ])
    }

    fn from_record_batch(_batch: &RecordBatch) -> Result<Vec<Self>> {
        // This would require having Individual objects available
        // This functionality is implemented in parent_schema_constructors.rs
        unimplemented!("Conversion from RecordBatch to Parent requires Individual objects")
    }

    fn to_record_batch(_parents: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

/// A collection of parents that can be efficiently queried
#[derive(Debug, Default)]
pub struct ParentCollection {
    /// Parents indexed by PNR
    parents: HashMap<String, Arc<Parent>>,
}

impl ParentCollection {
    /// Create a new empty `ParentCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            parents: HashMap::new(),
        }
    }

    /// Get employed parents
    #[must_use]
    pub fn employed_parents(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.employment_status)
    }

    /// Get unemployed parents
    #[must_use]
    pub fn unemployed_parents(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| !parent.employment_status)
    }

    /// Get parents with comorbidity
    #[must_use]
    pub fn parents_with_comorbidity(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.has_comorbidity)
    }

    /// Get parents with specific job situation
    #[must_use]
    pub fn parents_by_job_situation(&self, situation: JobSituation) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.job_situation == situation)
    }

    /// Get parents with income above threshold
    #[must_use]
    pub fn parents_with_income_above(&self, threshold: f64) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.pre_exposure_income.unwrap_or(0.0) > threshold)
    }
}

// Implement ModelCollection trait
impl ModelCollection<Parent> for ParentCollection {
    fn add(&mut self, parent: Parent) {
        let pnr = parent.individual().pnr.clone();
        let parent_arc = Arc::new(parent);
        self.parents.insert(pnr, parent_arc);
    }

    fn get(&self, id: &String) -> Option<Arc<Parent>> {
        self.parents.get(id).cloned()
    }

    fn all(&self) -> Vec<Arc<Parent>> {
        self.parents.values().cloned().collect()
    }

    fn filter<F>(&self, predicate: F) -> Vec<Arc<Parent>>
    where
        F: Fn(&Parent) -> bool,
    {
        self.parents
            .values()
            .filter(|parent| predicate(parent))
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.parents.len()
    }
}
