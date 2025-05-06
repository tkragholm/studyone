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
    pub fn individual(&self) -> &Individual {
        &self.individual
    }

    /// Set the employment status
    pub fn with_employment_status(mut self, employed: bool) -> Self {
        self.employment_status = employed;
        self
    }

    /// Set the job situation
    pub fn with_job_situation(mut self, job_situation: JobSituation) -> Self {
        self.job_situation = job_situation;
        self
    }

    /// Set the pre-exposure income
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
    pub fn income_for_year(&self, year: i32) -> Option<f64> {
        self.income_data
            .iter()
            .find(|income| income.year == year)
            .map(|income| income.amount)
    }

    /// Get income trajectory for a range of years
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
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("employment_status", DataType::Boolean, false),
            Field::new("job_situation", DataType::Int32, false),
            Field::new("has_comorbidity", DataType::Boolean, false),
            Field::new("pre_exposure_income", DataType::Float64, true),
        ])
    }

    /// Convert a vector of Parent objects to a RecordBatch
    pub fn to_record_batch(parents: &[Self]) -> Result<RecordBatch> {
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

impl ParentCollection {
    /// Create a new empty ParentCollection
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
    pub fn get_parent(&self, pnr: &str) -> Option<Arc<Parent>> {
        self.parents.get(pnr).cloned()
    }

    /// Get all parents in the collection
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
    pub fn employed_parents(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.employment_status)
    }

    /// Get unemployed parents
    pub fn unemployed_parents(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| !parent.employment_status)
    }

    /// Get parents with comorbidity
    pub fn parents_with_comorbidity(&self) -> Vec<Arc<Parent>> {
        self.filter(|parent| parent.has_comorbidity)
    }

    /// Count total number of parents in the collection
    pub fn count(&self) -> usize {
        self.parents.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::diagnosis::{Diagnosis, DiagnosisType};
    use crate::models::income::Income;
    use crate::models::individual::{EducationLevel, Gender, Individual, Origin};

    /// Create a test individual for a parent
    fn create_test_individual() -> Individual {
        Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1975, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM123".to_string()),
            emigration_date: None,
            immigration_date: None,
        }
    }

    #[test]
    fn test_parent_creation() {
        let individual = Arc::new(create_test_individual());
        let parent = Parent::from_individual(individual.clone());

        assert_eq!(parent.individual().pnr, "1234567890");
        assert_eq!(parent.employment_status, false);
        assert_eq!(parent.job_situation, JobSituation::Other);
        assert_eq!(parent.has_comorbidity, false);
        assert!(parent.pre_exposure_income.is_none());
        assert!(parent.diagnoses.is_empty());
        assert!(parent.income_data.is_empty());
    }

    #[test]
    fn test_parent_with_attributes() {
        let individual = Arc::new(create_test_individual());
        let parent = Parent::from_individual(individual.clone())
            .with_employment_status(true)
            .with_job_situation(JobSituation::EmployedFullTime)
            .with_pre_exposure_income(350000.0);

        assert_eq!(parent.employment_status, true);
        assert_eq!(parent.job_situation, JobSituation::EmployedFullTime);
        assert_eq!(parent.pre_exposure_income, Some(350000.0));
    }

    #[test]
    fn test_add_diagnosis() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Initially no comorbidity
        assert_eq!(parent.has_comorbidity, false);

        // Add a diagnosis
        let diagnosis = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J45".to_string(),
            diagnosis_type: DiagnosisType::Secondary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            is_scd: false,
            severity: 1,
        };

        parent.add_diagnosis(Arc::new(diagnosis));

        // Should now have comorbidity
        assert_eq!(parent.has_comorbidity, true);
        assert_eq!(parent.diagnoses.len(), 1);
    }

    #[test]
    fn test_add_income() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Add income data points
        let income1 = Income {
            individual_pnr: "1234567890".to_string(),
            year: 2010,
            amount: 300000.0,
            income_type: "salary".to_string(),
        };

        let income2 = Income {
            individual_pnr: "1234567890".to_string(),
            year: 2011,
            amount: 320000.0,
            income_type: "salary".to_string(),
        };

        parent.add_income(Arc::new(income1));
        parent.add_income(Arc::new(income2));

        // Check retrieval
        assert_eq!(parent.income_data.len(), 2);
        assert_eq!(parent.income_for_year(2010), Some(300000.0));
        assert_eq!(parent.income_for_year(2011), Some(320000.0));
        assert_eq!(parent.income_for_year(2012), None);

        // Check trajectory
        let trajectory = parent.income_trajectory(2009, 2012);
        assert_eq!(trajectory.len(), 2);
        assert_eq!(trajectory.get(&2010), Some(&300000.0));
        assert_eq!(trajectory.get(&2011), Some(&320000.0));
        assert_eq!(trajectory.get(&2009), None);
        assert_eq!(trajectory.get(&2012), None);
    }

    #[test]
    fn test_had_diagnosis_before() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Add a diagnosis from 2015
        let diagnosis = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J45".to_string(),
            diagnosis_type: DiagnosisType::Secondary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            is_scd: false,
            severity: 1,
        };

        parent.add_diagnosis(Arc::new(diagnosis));

        // Test dates
        assert!(!parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2015, 3, 9).unwrap()));
        assert!(parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2015, 3, 11).unwrap()));
        assert!(parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));
    }

    #[test]
    fn test_parent_collection() {
        let mut collection = ParentCollection::new();

        // Create parents
        let individual1 = Arc::new(Individual {
            pnr: "1111111111".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1975, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM1".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let individual2 = Arc::new(Individual {
            pnr: "2222222222".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::High,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM1".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let parent1 = Parent::from_individual(individual1.clone())
            .with_employment_status(true)
            .with_job_situation(JobSituation::EmployedFullTime);

        let parent2 = Parent::from_individual(individual2.clone())
            .with_employment_status(false)
            .with_job_situation(JobSituation::Unemployed);

        // Add to collection
        collection.add_parent(parent1);
        collection.add_parent(parent2);

        // Test collection
        assert_eq!(collection.count(), 2);
        assert!(collection.get_parent("1111111111").is_some());
        assert!(collection.get_parent("2222222222").is_some());
        assert!(collection.get_parent("3333333333").is_none());

        // Test filtering
        let employed = collection.employed_parents();
        assert_eq!(employed.len(), 1);
        assert_eq!(employed[0].individual().pnr, "1111111111");

        let unemployed = collection.unemployed_parents();
        assert_eq!(unemployed.len(), 1);
        assert_eq!(unemployed[0].individual().pnr, "2222222222");
    }
}
