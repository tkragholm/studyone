//! Income entity model
//!
//! This module contains the Income model, representing income data for individuals
//! over time. Income data is used for trajectory analysis, including differences
//! between parents of children with and without severe chronic diseases.

use crate::error::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Representation of income data for an individual in a specific year
#[derive(Debug, Clone)]
pub struct Income {
    /// PNR of the individual
    pub individual_pnr: String,
    /// Year of the income
    pub year: i32,
    /// Income amount in DKK (inflation-adjusted)
    pub amount: f64,
    /// Type of income (e.g., salary, self-employment, capital, total)
    pub income_type: String,
}

impl Income {
    /// Create a new Income record
    #[must_use]
    pub fn new(individual_pnr: String, year: i32, amount: f64, income_type: String) -> Self {
        Self {
            individual_pnr,
            year,
            amount,
            income_type,
        }
    }

    /// Get the Arrow schema for Income records
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("individual_pnr", DataType::Utf8, false),
            Field::new("year", DataType::Int32, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("income_type", DataType::Utf8, false),
        ])
    }

    /// Convert a vector of Income objects to a `RecordBatch`
    pub fn to_record_batch(incomes: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}

/// Income trajectory for an individual over multiple years
#[derive(Debug, Clone)]
pub struct IncomeTrajectory {
    /// PNR of the individual
    pub individual_pnr: String,
    /// Income data by year
    pub yearly_income: BTreeMap<i32, f64>,
    /// First year in the trajectory
    pub start_year: i32,
    /// Last year in the trajectory
    pub end_year: i32,
    /// Income type for this trajectory
    pub income_type: String,
}

impl IncomeTrajectory {
    /// Create a new empty income trajectory
    #[must_use]
    pub fn new(individual_pnr: String, income_type: String) -> Self {
        Self {
            individual_pnr,
            yearly_income: BTreeMap::new(),
            start_year: i32::MAX,
            end_year: i32::MIN,
            income_type,
        }
    }

    /// Add income data for a specific year
    pub fn add_income(&mut self, year: i32, amount: f64) {
        self.yearly_income.insert(year, amount);

        // Update start and end years
        if year < self.start_year {
            self.start_year = year;
        }
        if year > self.end_year {
            self.end_year = year;
        }
    }

    /// Add an Income object to the trajectory
    pub fn add_income_record(&mut self, income: &Income) {
        if income.individual_pnr == self.individual_pnr && income.income_type == self.income_type {
            self.add_income(income.year, income.amount);
        }
    }

    /// Get income for a specific year
    #[must_use]
    pub fn income_for_year(&self, year: i32) -> Option<f64> {
        self.yearly_income.get(&year).copied()
    }

    /// Get all years with income data
    #[must_use]
    pub fn years(&self) -> Vec<i32> {
        self.yearly_income.keys().copied().collect()
    }

    /// Get all income values
    #[must_use]
    pub fn values(&self) -> Vec<f64> {
        self.yearly_income.values().copied().collect()
    }

    /// Get income as a vector of (year, amount) pairs
    #[must_use]
    pub fn as_pairs(&self) -> Vec<(i32, f64)> {
        self.yearly_income
            .iter()
            .map(|(&year, &amount)| (year, amount))
            .collect()
    }

    /// Calculate mean income across all years
    #[must_use]
    pub fn mean_income(&self) -> Option<f64> {
        if self.yearly_income.is_empty() {
            return None;
        }

        let sum: f64 = self.yearly_income.values().sum();
        Some(sum / self.yearly_income.len() as f64)
    }

    /// Calculate trend as the slope of a linear regression
    #[must_use]
    pub fn trend(&self) -> Option<f64> {
        if self.yearly_income.len() < 2 {
            return None;
        }

        let n = self.yearly_income.len() as f64;
        let pairs: Vec<(f64, f64)> = self
            .yearly_income
            .iter()
            .map(|(&year, &amount)| (f64::from(year), amount))
            .collect();

        let sum_x: f64 = pairs.iter().map(|(x, _)| x).sum();
        let sum_y: f64 = pairs.iter().map(|(_, y)| y).sum();
        let sum_xy: f64 = pairs.iter().map(|(x, y)| x * y).sum();
        let sum_xx: f64 = pairs.iter().map(|(x, _)| x * x).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        Some(slope)
    }

    /// Calculate pre-post difference relative to an index year
    #[must_use]
    pub fn pre_post_difference(
        &self,
        index_year: i32,
        pre_years: i32,
        post_years: i32,
    ) -> Option<f64> {
        let pre_start = index_year - pre_years;
        let pre_end = index_year - 1;
        let post_start = index_year;
        let post_end = index_year + post_years;

        // Calculate mean pre-index income
        let pre_incomes: Vec<f64> = (pre_start..=pre_end)
            .filter_map(|year| self.income_for_year(year))
            .collect();

        // Calculate mean post-index income
        let post_incomes: Vec<f64> = (post_start..=post_end)
            .filter_map(|year| self.income_for_year(year))
            .collect();

        if pre_incomes.is_empty() || post_incomes.is_empty() {
            return None;
        }

        let pre_mean: f64 = pre_incomes.iter().sum::<f64>() / pre_incomes.len() as f64;
        let post_mean: f64 = post_incomes.iter().sum::<f64>() / post_incomes.len() as f64;

        Some(post_mean - pre_mean)
    }

    /// Fill missing years with linear interpolation
    pub fn interpolate_missing(&mut self) {
        if self.yearly_income.len() < 2 || self.start_year == i32::MAX {
            return;
        }

        let years: Vec<i32> = self.yearly_income.keys().copied().collect();

        for year in self.start_year..=self.end_year {
            if !self.yearly_income.contains_key(&year) {
                // Find nearest lower and higher years with data
                let lower = years.iter().filter(|&&y| y < year).max();
                let higher = years.iter().filter(|&&y| y > year).min();

                if let (Some(&lower_year), Some(&higher_year)) = (lower, higher) {
                    let lower_value = self.yearly_income[&lower_year];
                    let higher_value = self.yearly_income[&higher_year];
                    let year_span = higher_year - lower_year;
                    let position = year - lower_year;

                    // Linear interpolation
                    let interpolated = lower_value
                        + (higher_value - lower_value)
                            * (f64::from(position) / f64::from(year_span));
                    self.yearly_income.insert(year, interpolated);
                }
            }
        }
    }
}

/// A combined income trajectory for a family (both parents)
#[derive(Debug, Clone)]
pub struct FamilyIncomeTrajectory {
    /// Family identifier
    pub family_id: String,
    /// Mother's income trajectory
    pub mother_trajectory: Option<IncomeTrajectory>,
    /// Father's income trajectory
    pub father_trajectory: Option<IncomeTrajectory>,
    /// Combined family income by year
    pub combined_income: BTreeMap<i32, f64>,
    /// First year in the trajectory
    pub start_year: i32,
    /// Last year in the trajectory
    pub end_year: i32,
}

impl FamilyIncomeTrajectory {
    /// Create a new family income trajectory
    #[must_use]
    pub fn new(family_id: String) -> Self {
        Self {
            family_id,
            mother_trajectory: None,
            father_trajectory: None,
            combined_income: BTreeMap::new(),
            start_year: i32::MAX,
            end_year: i32::MIN,
        }
    }

    /// Set the mother's income trajectory
    #[must_use]
    pub fn with_mother_trajectory(mut self, trajectory: IncomeTrajectory) -> Self {
        // Update start and end years
        if trajectory.start_year < self.start_year {
            self.start_year = trajectory.start_year;
        }
        if trajectory.end_year > self.end_year {
            self.end_year = trajectory.end_year;
        }

        self.mother_trajectory = Some(trajectory);
        self.update_combined_income();
        self
    }

    /// Set the father's income trajectory
    #[must_use]
    pub fn with_father_trajectory(mut self, trajectory: IncomeTrajectory) -> Self {
        // Update start and end years
        if trajectory.start_year < self.start_year {
            self.start_year = trajectory.start_year;
        }
        if trajectory.end_year > self.end_year {
            self.end_year = trajectory.end_year;
        }

        self.father_trajectory = Some(trajectory);
        self.update_combined_income();
        self
    }

    /// Update the combined income based on mother and father trajectories
    fn update_combined_income(&mut self) {
        if self.start_year == i32::MAX {
            return;
        }

        for year in self.start_year..=self.end_year {
            let mother_income = self
                .mother_trajectory
                .as_ref()
                .and_then(|t| t.income_for_year(year))
                .unwrap_or(0.0);

            let father_income = self
                .father_trajectory
                .as_ref()
                .and_then(|t| t.income_for_year(year))
                .unwrap_or(0.0);

            self.combined_income
                .insert(year, mother_income + father_income);
        }
    }

    /// Get combined income for a specific year
    #[must_use]
    pub fn income_for_year(&self, year: i32) -> Option<f64> {
        self.combined_income.get(&year).copied()
    }

    /// Get difference between parents' incomes for a year
    #[must_use]
    pub fn income_gap(&self, year: i32) -> Option<f64> {
        let mother_income = self
            .mother_trajectory
            .as_ref()
            .and_then(|t| t.income_for_year(year));

        let father_income = self
            .father_trajectory
            .as_ref()
            .and_then(|t| t.income_for_year(year));

        match (mother_income, father_income) {
            (Some(m), Some(f)) => Some(f - m), // Positive means father earns more
            _ => None,
        }
    }

    /// Calculate the income gap trend over time
    #[must_use]
    pub fn income_gap_trend(&self) -> Option<f64> {
        if self.start_year == i32::MAX {
            return None;
        }

        let mut gap_trajectory =
            IncomeTrajectory::new(format!("gap_{}", self.family_id), "income_gap".to_string());

        for year in self.start_year..=self.end_year {
            if let Some(gap) = self.income_gap(year) {
                gap_trajectory.add_income(year, gap);
            }
        }

        gap_trajectory.trend()
    }

    /// Calculate income share of primary earner
    #[must_use]
    pub fn primary_earner_share(&self, year: i32) -> Option<f64> {
        let mother_income = self
            .mother_trajectory
            .as_ref()
            .and_then(|t| t.income_for_year(year))
            .unwrap_or(0.0);

        let father_income = self
            .father_trajectory
            .as_ref()
            .and_then(|t| t.income_for_year(year))
            .unwrap_or(0.0);

        let combined = mother_income + father_income;

        if combined <= 0.0 {
            return None;
        }

        let primary = mother_income.max(father_income);
        Some(primary / combined)
    }

    /// Calculate pre-post difference in combined income relative to an index year
    #[must_use]
    pub fn pre_post_difference(
        &self,
        index_year: i32,
        pre_years: i32,
        post_years: i32,
    ) -> Option<f64> {
        let pre_start = index_year - pre_years;
        let pre_end = index_year - 1;
        let post_start = index_year;
        let post_end = index_year + post_years;

        // Calculate mean pre-index income
        let pre_incomes: Vec<f64> = (pre_start..=pre_end)
            .filter_map(|year| self.income_for_year(year))
            .collect();

        // Calculate mean post-index income
        let post_incomes: Vec<f64> = (post_start..=post_end)
            .filter_map(|year| self.income_for_year(year))
            .collect();

        if pre_incomes.is_empty() || post_incomes.is_empty() {
            return None;
        }

        let pre_mean: f64 = pre_incomes.iter().sum::<f64>() / pre_incomes.len() as f64;
        let post_mean: f64 = post_incomes.iter().sum::<f64>() / post_incomes.len() as f64;

        Some(post_mean - pre_mean)
    }
}

/// A collection of income data for multiple individuals
#[derive(Debug)]
pub struct IncomeCollection {
    /// Income records by individual PNR
    incomes_by_pnr: HashMap<String, Vec<Arc<Income>>>,
    /// Income trajectories by individual PNR and income type
    trajectories: HashMap<(String, String), IncomeTrajectory>,
    /// Family income trajectories by family ID
    family_trajectories: HashMap<String, FamilyIncomeTrajectory>,
}

impl Default for IncomeCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl IncomeCollection {
    /// Create a new empty `IncomeCollection`
    #[must_use]
    pub fn new() -> Self {
        Self {
            incomes_by_pnr: HashMap::new(),
            trajectories: HashMap::new(),
            family_trajectories: HashMap::new(),
        }
    }

    /// Add an income record to the collection
    pub fn add_income(&mut self, income: Income) {
        let pnr = income.individual_pnr.clone();
        let income_type = income.income_type.clone();
        let year = income.year;
        let amount = income.amount;

        // Add to raw incomes
        let income_arc = Arc::new(income);
        self.incomes_by_pnr
            .entry(pnr.clone())
            .or_default()
            .push(income_arc);

        // Update trajectory
        let trajectory_key = (pnr, income_type);
        self.trajectories
            .entry(trajectory_key.clone())
            .or_insert_with(|| {
                IncomeTrajectory::new(trajectory_key.0.clone(), trajectory_key.1.clone())
            })
            .add_income(year, amount);
    }

    /// Get all income records for an individual
    #[must_use]
    pub fn get_incomes(&self, pnr: &str) -> Vec<Arc<Income>> {
        self.incomes_by_pnr.get(pnr).cloned().unwrap_or_default()
    }

    /// Get income trajectory for an individual and income type
    #[must_use]
    pub fn get_trajectory(&self, pnr: &str, income_type: &str) -> Option<&IncomeTrajectory> {
        self.trajectories
            .get(&(pnr.to_string(), income_type.to_string()))
    }

    /// Add a family income trajectory
    pub fn add_family_trajectory(&mut self, trajectory: FamilyIncomeTrajectory) {
        self.family_trajectories
            .insert(trajectory.family_id.clone(), trajectory);
    }

    /// Get a family income trajectory
    #[must_use]
    pub fn get_family_trajectory(&self, family_id: &str) -> Option<&FamilyIncomeTrajectory> {
        self.family_trajectories.get(family_id)
    }

    /// Calculate a family income trajectory from parent PNRs
    #[must_use]
    pub fn calculate_family_trajectory(
        &self,
        family_id: &str,
        mother_pnr: Option<&str>,
        father_pnr: Option<&str>,
        income_type: &str,
    ) -> Option<FamilyIncomeTrajectory> {
        // Get parent trajectories
        let mother_trajectory = mother_pnr
            .and_then(|pnr| self.get_trajectory(pnr, income_type))
            .cloned();

        let father_trajectory = father_pnr
            .and_then(|pnr| self.get_trajectory(pnr, income_type))
            .cloned();

        if mother_trajectory.is_none() && father_trajectory.is_none() {
            return None;
        }

        let mut family_trajectory = FamilyIncomeTrajectory::new(family_id.to_string());

        if let Some(trajectory) = mother_trajectory {
            family_trajectory = family_trajectory.with_mother_trajectory(trajectory);
        }

        if let Some(trajectory) = father_trajectory {
            family_trajectory = family_trajectory.with_father_trajectory(trajectory);
        }

        Some(family_trajectory)
    }

    /// Create trajectories for all families with complete data
    pub fn build_family_trajectories(
        &mut self,
        family_map: &HashMap<String, (Option<String>, Option<String>)>,
        income_type: &str,
    ) {
        for (family_id, (mother_pnr, father_pnr)) in family_map {
            if let Some(family_trajectory) = self.calculate_family_trajectory(
                family_id,
                mother_pnr.as_deref(),
                father_pnr.as_deref(),
                income_type,
            ) {
                self.add_family_trajectory(family_trajectory);
            }
        }
    }

    /// Count the total number of income records
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.incomes_by_pnr.values().map(std::vec::Vec::len).sum()
    }

    /// Count the number of individuals with income data
    #[must_use]
    pub fn individual_count(&self) -> usize {
        self.incomes_by_pnr.len()
    }

    /// Count the number of family trajectories
    #[must_use]
    pub fn family_count(&self) -> usize {
        self.family_trajectories.len()
    }
}
