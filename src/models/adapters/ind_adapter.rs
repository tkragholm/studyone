//! IND Registry to Income Adapter
//!
//! This module contains the adapter that maps IND registry data to Income domain models.
//! The IND (Indkomst) registry contains income and tax information.

use super::RegistryAdapter;
use crate::error::{Error, Result};
use crate::models::income::{FamilyIncomeTrajectory, Income, IncomeTrajectory};
use crate::models::parent::Parent;
use arrow::array::{Array, Float64Array, Int8Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Income type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IncomeType {
    /// Total personal income
    TotalPersonal,
    /// Salary income
    Salary,
    /// Self-employment income
    SelfEmployment,
    /// Capital income
    Capital,
    /// Transfer payments
    TransferPayments,
    /// Other income
    Other,
}

impl IncomeType {
    /// Convert income type to string representation
    fn as_str(&self) -> &'static str {
        match self {
            Self::TotalPersonal => "total_personal",
            Self::Salary => "salary",
            Self::SelfEmployment => "self_employment",
            Self::Capital => "capital",
            Self::TransferPayments => "transfer_payments",
            Self::Other => "other",
        }
    }
}

/// Adapter for converting IND registry data to Income models
pub struct IndIncomeAdapter {
    /// Year to be used for income data (some registries contain multi-year data)
    year: i32,
    /// CPI index for inflation adjustment (base year = current year)
    cpi_index: HashMap<i32, f64>,
}

impl IndIncomeAdapter {
    /// Create a new IND adapter for a specific year with inflation adjustment
    #[must_use]
    pub fn new(year: i32, cpi_index: HashMap<i32, f64>) -> Self {
        Self { year, cpi_index }
    }

    /// Create a new IND adapter for a specific year without inflation adjustment
    #[must_use]
    pub fn new_without_cpi(year: i32) -> Self {
        let mut cpi_index = HashMap::new();
        // Set all years to 1.0 (no adjustment)
        for y in 1990..=2030 {
            cpi_index.insert(y, 1.0);
        }

        Self { year, cpi_index }
    }

    /// Apply inflation adjustment to an income amount
    fn adjust_for_inflation(&self, amount: f64, from_year: i32, to_year: i32) -> f64 {
        let from_cpi = self.cpi_index.get(&from_year).copied().unwrap_or(1.0);
        let to_cpi = self.cpi_index.get(&to_year).copied().unwrap_or(1.0);

        // Convert from from_year prices to to_year prices
        amount * (to_cpi / from_cpi)
    }

    /// Extract job situation from employment status code
    fn extract_job_situation(&self, employment_code: i8) -> crate::models::parent::JobSituation {
        use crate::models::parent::JobSituation;

        match employment_code {
            1 | 2 => JobSituation::EmployedFullTime, // Full-time employed
            3 => JobSituation::EmployedPartTime,     // Part-time employed
            4 => JobSituation::SelfEmployed,         // Self-employed
            5 | 6 => JobSituation::Unemployed,       // Unemployed
            7 => JobSituation::Student,              // Student
            8 => JobSituation::Retired,              // Retired
            9 => JobSituation::OnLeave,              // On leave
            _ => JobSituation::Other,                // Other or unknown
        }
    }

    /// Create a trajectory from a collection of income records
    pub fn create_trajectory(&self, incomes: &[Income]) -> IncomeTrajectory {
        if incomes.is_empty() {
            return IncomeTrajectory::new("".to_string(), "".to_string());
        }

        // Group by individual and income type
        let first = &incomes[0];
        let mut trajectory =
            IncomeTrajectory::new(first.individual_pnr.clone(), first.income_type.clone());

        // Add all incomes to the trajectory
        for income in incomes {
            if income.individual_pnr == trajectory.individual_pnr
                && income.income_type == trajectory.income_type
            {
                trajectory.add_income(income.year, income.amount);
            }
        }

        // Interpolate missing years if needed
        trajectory.interpolate_missing();

        trajectory
    }

    /// Create family trajectories from parent PNRs and income collection
    pub fn create_family_trajectories(
        &self,
        family_map: &HashMap<String, (Option<String>, Option<String>)>,
        incomes: &[Income],
    ) -> HashMap<String, FamilyIncomeTrajectory> {
        // Group incomes by individual and income type
        let mut income_map: HashMap<(String, String), Vec<&Income>> = HashMap::new();

        for income in incomes {
            income_map
                .entry((income.individual_pnr.clone(), income.income_type.clone()))
                .or_default()
                .push(income);
        }

        // Create trajectories for each individual/income type
        let mut trajectories: HashMap<(String, String), IncomeTrajectory> = HashMap::new();

        for ((pnr, income_type), incomes) in income_map {
            let mut trajectory = IncomeTrajectory::new(pnr, income_type);

            for income in incomes {
                trajectory.add_income(income.year, income.amount);
            }

            trajectory.interpolate_missing();
            trajectories.insert(
                (
                    trajectory.individual_pnr.clone(),
                    trajectory.income_type.clone(),
                ),
                trajectory,
            );
        }

        // Create family trajectories
        let mut family_trajectories = HashMap::new();

        for (family_id, (mother_pnr, father_pnr)) in family_map {
            let mut family_trajectory = FamilyIncomeTrajectory::new(family_id.clone());
            let income_type = IncomeType::TotalPersonal.as_str();

            // Add mother's trajectory if available
            if let Some(mother_pnr) = mother_pnr {
                if let Some(trajectory) =
                    trajectories.get(&(mother_pnr.clone(), income_type.to_string()))
                {
                    family_trajectory =
                        family_trajectory.with_mother_trajectory(trajectory.clone());
                }
            }

            // Add father's trajectory if available
            if let Some(father_pnr) = father_pnr {
                if let Some(trajectory) =
                    trajectories.get(&(father_pnr.clone(), income_type.to_string()))
                {
                    family_trajectory =
                        family_trajectory.with_father_trajectory(trajectory.clone());
                }
            }

            // Only add family trajectories with at least one parent
            if family_trajectory.mother_trajectory.is_some()
                || family_trajectory.father_trajectory.is_some()
            {
                family_trajectories.insert(family_id.clone(), family_trajectory);
            }
        }

        family_trajectories
    }

    /// Update parent models with income and employment information
    pub fn update_parents(
        &self,
        parents: &mut [Parent],
        batch: &RecordBatch,
        base_year: i32,
    ) -> Result<()> {
        // Get column indices
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| Error::ColumnNotFound {
                column: "PNR".to_string(),
            })?;

        let income_idx =
            batch
                .schema()
                .index_of("PERINDKIALT_13")
                .map_err(|_| Error::ColumnNotFound {
                    column: "PERINDKIALT_13".to_string(),
                })?;

        let employment_idx =
            batch
                .schema()
                .index_of("BESKST13")
                .map_err(|_| Error::ColumnNotFound {
                    column: "BESKST13".to_string(),
                })?;

        // Cast columns to their appropriate types
        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PNR".to_string(),
                expected: "String".to_string(),
            })?;

        let income_array = batch
            .column(income_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PERINDKIALT_13".to_string(),
                expected: "Float64".to_string(),
            })?;

        let employment_array = batch
            .column(employment_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "BESKST13".to_string(),
                expected: "Int8".to_string(),
            })?;

        // Create lookup for faster access
        let mut parent_map: HashMap<String, &mut Parent> = HashMap::new();
        for parent in parents.iter_mut() {
            parent_map.insert(parent.individual().pnr.clone(), parent);
        }

        // Process each row in the batch
        for i in 0..batch.num_rows() {
            let pnr = pnr_array.value(i).to_string();

            if let Some(parent) = parent_map.get_mut(&pnr) {
                // Update employment status
                if !employment_array.is_null(i) {
                    let employment_code = employment_array.value(i);
                    let job_situation = self.extract_job_situation(employment_code);

                    // Consider employed if code is 1-4 (different employment categories)
                    let is_employed = (1..=4).contains(&employment_code);

                    parent.employment_status = is_employed;
                    parent.job_situation = job_situation;
                }

                // Update pre-exposure income (inflation-adjusted)
                if !income_array.is_null(i) {
                    let income_amount = income_array.value(i);
                    let adjusted_income =
                        self.adjust_for_inflation(income_amount, self.year, base_year);
                    parent.pre_exposure_income = Some(adjusted_income);
                }
            }
        }

        Ok(())
    }
}

impl RegistryAdapter<Income> for IndIncomeAdapter {
    /// Convert an IND `RecordBatch` to a vector of Income objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Income>> {
        // This implementation is for static calls without an instance
        // For instance-specific processing, use the instance method from_record_batch_with_year
        let adapter = IndIncomeAdapter::new_without_cpi(2020); // Default year
        adapter.from_record_batch_with_year(batch)
    }
    
    /// Apply additional transformations to the Income models
    fn transform(_models: &mut [Income]) -> Result<()> {
        // No additional transformations needed for incomes from IND
        Ok(())
    }
}

impl IndIncomeAdapter {
    /// Convert an IND `RecordBatch` to a vector of Income objects with the year from this instance
    pub fn from_record_batch_with_year(&self, batch: &RecordBatch) -> Result<Vec<Income>> {
        // Get column indices
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| Error::ColumnNotFound {
                column: "PNR".to_string(),
            })?;

        let total_income_idx =
            batch
                .schema()
                .index_of("PERINDKIALT_13")
                .map_err(|_| Error::ColumnNotFound {
                    column: "PERINDKIALT_13".to_string(),
                })?;

        let salary_idx = batch.schema().index_of("LOENMV_13").ok();

        // Cast columns to their appropriate types
        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PNR".to_string(),
                expected: "String".to_string(),
            })?;

        let total_income_array = batch
            .column(total_income_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PERINDKIALT_13".to_string(),
                expected: "Float64".to_string(),
            })?;

        let salary_array = if let Some(idx) = salary_idx {
            Some(
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| Error::InvalidDataType {
                        column: "LOENMV_13".to_string(),
                        expected: "Float64".to_string(),
                    })?,
            )
        } else {
            None
        };

        let mut incomes = Vec::new();

        // Process each row in the batch
        for i in 0..batch.num_rows() {
            let pnr = pnr_array.value(i).to_string();

            // Add total income
            if !total_income_array.is_null(i) {
                let amount = total_income_array.value(i);

                incomes.push(Income::new(
                    pnr.clone(),
                    self.year,
                    amount,
                    IncomeType::TotalPersonal.as_str().to_string(),
                ));
            }

            // Add salary income if available
            if let Some(array) = &salary_array {
                if !array.is_null(i) {
                    let amount = array.value(i);

                    incomes.push(Income::new(
                        pnr.clone(),
                        self.year,
                        amount,
                        IncomeType::Salary.as_str().to_string(),
                    ));
                }
            }

            // Add derived income type if both total and salary are available
            if !total_income_array.is_null(i)
                && salary_array.is_some()
                && !salary_array.unwrap().is_null(i)
            {
                let total = total_income_array.value(i);
                let salary = salary_array.unwrap().value(i);

                // Other income (transfers, capital, etc.)
                let other = total - salary;
                if other > 0.0 {
                    incomes.push(Income::new(
                        pnr.clone(),
                        self.year,
                        other,
                        IncomeType::Other.as_str().to_string(),
                    ));
                }
            }
        }

        Ok(incomes)
    }

    // This is now implemented in the RegistryAdapter trait implementation
}

/// Multi-year adapter for processing IND data across multiple years
pub struct IndMultiYearAdapter {
    /// Base adapter instances by year
    adapters: HashMap<i32, IndIncomeAdapter>,
    /// CPI index for inflation adjustment (base year = current year)
    cpi_index: HashMap<i32, f64>,
}

impl IndMultiYearAdapter {
    /// Create a new multi-year adapter with a specified CPI index
    #[must_use]
    pub fn new(years: Vec<i32>, cpi_index: HashMap<i32, f64>) -> Self {
        let mut adapters = HashMap::new();

        for year in years {
            adapters.insert(year, IndIncomeAdapter::new(year, cpi_index.clone()));
        }

        Self {
            adapters,
            cpi_index,
        }
    }

    /// Create a default multi-year adapter without inflation adjustment
    #[must_use]
    pub fn new_without_cpi(years: Vec<i32>) -> Self {
        let mut cpi_index = HashMap::new();
        // Set all years to 1.0 (no adjustment)
        for y in 1990..=2030 {
            cpi_index.insert(y, 1.0);
        }

        Self::new(years, cpi_index)
    }

    /// Get adapter for a specific year
    pub fn get_adapter(&self, year: i32) -> Option<&IndIncomeAdapter> {
        self.adapters.get(&year)
    }

    /// Process a batch for a specific year
    pub fn process_batch(&self, batch: &RecordBatch, year: i32) -> Result<Vec<Income>> {
        if let Some(adapter) = self.get_adapter(year) {
            adapter.from_record_batch_with_year(batch)
        } else {
            Err(anyhow::anyhow!("No adapter available for year {}", year))
        }
    }

    /// Create trajectories from income records across all years
    pub fn create_trajectories(
        &self,
        incomes: &[Income],
    ) -> HashMap<String, HashMap<String, IncomeTrajectory>> {
        // Group incomes by individual PNR and income type
        let mut grouped: HashMap<(String, String), Vec<&Income>> = HashMap::new();

        for income in incomes {
            grouped
                .entry((income.individual_pnr.clone(), income.income_type.clone()))
                .or_default()
                .push(income);
        }

        // Create trajectories for each individual and income type
        let mut result: HashMap<String, HashMap<String, IncomeTrajectory>> = HashMap::new();

        for ((pnr, income_type), incomes) in grouped {
            // Create trajectory
            let mut trajectory = IncomeTrajectory::new(pnr.clone(), income_type.clone());

            // Add all income data points
            for income in incomes {
                trajectory.add_income(income.year, income.amount);
            }

            // Interpolate missing years if needed
            trajectory.interpolate_missing();

            // Add to result
            result
                .entry(pnr)
                .or_default()
                .insert(income_type, trajectory);
        }

        result
    }

    /// Create family trajectories for all families
    pub fn create_family_trajectories(
        &self,
        family_map: &HashMap<String, (Option<String>, Option<String>)>,
        individual_trajectories: &HashMap<String, HashMap<String, IncomeTrajectory>>,
    ) -> HashMap<String, FamilyIncomeTrajectory> {
        let mut family_trajectories = HashMap::new();
        let income_type = IncomeType::TotalPersonal.as_str();

        for (family_id, (mother_pnr, father_pnr)) in family_map {
            let mut family_trajectory = FamilyIncomeTrajectory::new(family_id.clone());

            // Add mother's trajectory if available
            if let Some(mother_pnr) = mother_pnr {
                if let Some(trajectories) = individual_trajectories.get(mother_pnr) {
                    if let Some(trajectory) = trajectories.get(income_type) {
                        family_trajectory =
                            family_trajectory.with_mother_trajectory(trajectory.clone());
                    }
                }
            }

            // Add father's trajectory if available
            if let Some(father_pnr) = father_pnr {
                if let Some(trajectories) = individual_trajectories.get(father_pnr) {
                    if let Some(trajectory) = trajectories.get(income_type) {
                        family_trajectory =
                            family_trajectory.with_father_trajectory(trajectory.clone());
                    }
                }
            }

            // Only add family trajectories with at least one parent
            if family_trajectory.mother_trajectory.is_some()
                || family_trajectory.father_trajectory.is_some()
            {
                family_trajectories.insert(family_id.clone(), family_trajectory);
            }
        }

        family_trajectories
    }
}
