//! Parent entity model
//!
//! This module contains the Parent model, which represents a parent in the study.
//! Parents have specific attributes related to socioeconomic status, employment,
//! and can be associated with children and families.

use super::diagnosis::Diagnosis;
use super::income::Income;
use super::individual::Individual;
use crate::error::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Job situation category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSituation {
    /// Employed full-time
    EmployedFullTime,
    /// Employed part-time
    EmployedPartTime,
    /// Self-employed
    SelfEmployed,
    /// Unemployed
    Unemployed,
    /// Student
    Student,
    /// Retired
    Retired,
    /// On leave (e.g., parental leave, sick leave)
    OnLeave,
    /// Other or unknown job situation
    Other,
}

impl From<i32> for JobSituation {
    fn from(value: i32) -> Self {
        match value {
            1 => JobSituation::EmployedFullTime,
            2 => JobSituation::EmployedPartTime,
            3 => JobSituation::SelfEmployed,
            4 => JobSituation::Unemployed,
            5 => JobSituation::Student,
            6 => JobSituation::Retired,
            7 => JobSituation::OnLeave,
            _ => JobSituation::Other,
        }
    }
}

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
    pub fn from_individual(individual: Arc<Individual>) -> Self {
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

    /// Get a reference to the underlying Individual
    #[must_use]
    pub fn individual(&self) -> &Individual {
        &self.individual
    }

    /// Set the employment status
    #[must_use]
    pub fn with_employment_status(mut self, employed: bool) -> Self {
        self.employment_status = employed;
        self
    }

    /// Set the job situation
    #[must_use]
    pub fn with_job_situation(mut self, job_situation: JobSituation) -> Self {
        self.job_situation = job_situation;
        self
    }

    /// Set the pre-exposure income
    #[must_use]
    pub fn with_pre_exposure_income(mut self, income: f64) -> Self {
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

    /// Get the Arrow schema for Parent records
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("employment_status", DataType::Boolean, false),
            Field::new("job_situation", DataType::Int32, false),
            Field::new("has_comorbidity", DataType::Boolean, false),
            Field::new("pre_exposure_income", DataType::Float64, true),
        ])
    }

    /// Convert a vector of Parent objects to a `RecordBatch`
    pub fn to_record_batch(_parents: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

/// A collection of parents that can be efficiently queried
#[derive(Debug)]
pub struct ParentCollection {
    /// Parents indexed by PNR
    parents: HashMap<String, Arc<Parent>>,
}

impl Default for ParentCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ParentCollection {
    /// Create a new empty `ParentCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            parents: HashMap::new(),
        }
    }

    /// Add a parent to the collection
    pub fn add_parent(&mut self, parent: Parent) {
        let pnr = parent.individual().pnr.clone();
        let parent_arc = Arc::new(parent);
        self.parents.insert(pnr, parent_arc);
    }

    /// Get a parent by PNR
    #[must_use]
    pub fn get_parent(&self, pnr: &str) -> Option<Arc<Parent>> {
        self.parents.get(pnr).cloned()
    }

    /// Get all parents in the collection
    #[must_use]
    pub fn all_parents(&self) -> Vec<Arc<Parent>> {
        self.parents.values().cloned().collect()
    }

    /// Filter parents by a predicate function
    pub fn filter<F>(&self, predicate: F) -> Vec<Arc<Parent>>
    where
        F: Fn(&Parent) -> bool,
    {
        self.parents
            .values()
            .filter(|parent| predicate(parent))
            .cloned()
            .collect()
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

    /// Count total number of parents in the collection
    #[must_use]
    pub fn count(&self) -> usize {
        self.parents.len()
    }
}
