//! IND Registry Adapters
//!
//! This module provides adapters for converting IND registry data
//! to Income domain models using the unified adapter interface.

use crate::error::Result;
use crate::common::traits::{RegistryAdapter, StatefulAdapter, ModelLookup};
use crate::models::Income;
use crate::registry::{IndRegister, ModelConversion, model_conversion::ModelConversionExt};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Income types available in IND registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IncomeType {
    /// Personal income
    Personal,
    /// Household income
    Household,
    /// Disposable income
    Disposable,
    /// All income types
    All,
}

/// Adapter for converting IND registry data to Income models
#[derive(Debug)]
pub struct IndIncomeAdapter {
    registry: IndRegister,
    income_type: IncomeType,
}

impl IndIncomeAdapter {
    /// Create a new income adapter for the specified income type
    #[must_use] pub fn new(income_type: IncomeType) -> Self {
        Self {
            registry: IndRegister::new(),
            income_type,
        }
    }
    
    /// Get the income type for this adapter
    #[must_use] pub fn income_type(&self) -> IncomeType {
        self.income_type
    }
    
    /// Filter income records to include only the specified income type
    ///
    /// # Arguments
    ///
    /// * `incomes` - Income records to filter
    ///
    /// # Returns
    ///
    /// * Filtered income records
    fn filter_by_income_type(&self, incomes: &mut [Income]) -> Result<()> {
        // We can't use retain on a slice directly, so we need to work with indices
        // Create a vector to track which indices to keep
        let mut indices_to_keep = Vec::new();
        
        for (i, income) in incomes.iter().enumerate() {
            let keep = match self.income_type {
                IncomeType::All => true,
                IncomeType::Personal => income.income_type == "personal",
                IncomeType::Household => income.income_type == "household",
                IncomeType::Disposable => income.income_type == "disposable",
            };
            
            if keep {
                indices_to_keep.push(i);
            }
        }
        
        // Create a new vector with only the elements we want to keep
        let filtered: Vec<Income> = indices_to_keep.iter()
            .map(|&i| incomes[i].clone())
            .collect();
        
        // Clear the slice and refill it with only the elements we're keeping
        for i in 0..filtered.len().min(incomes.len()) {
            incomes[i] = filtered[i].clone();
        }
        
        // If the filtered list is shorter than the original slice,
        // we need to truncate the slice (caller must handle this)
        
        Ok(())
    }
}

impl StatefulAdapter<Income> for IndIncomeAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Income>> {
        self.registry.to_models(batch)
    }
    
    fn transform_models(&self, models: &mut [Income]) -> Result<()> {
        // First apply any registry transformations
        self.registry.transform_models(models)?;
        
        // Then filter by income type
        self.filter_by_income_type(models)
    }
}

impl RegistryAdapter<Income> for IndIncomeAdapter {
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Income>> {
        let registry = IndRegister::new();
        registry.to_models(batch)
    }
    
    fn transform(models: &mut [Income]) -> Result<()> {
        let registry = IndRegister::new();
        registry.transform_models(models)
    }
}

/// Multi-year adapter for loading income data across multiple years
#[derive(Debug)]
pub struct IndMultiYearAdapter {
    registry: IndRegister,
    years: Vec<u16>,
    income_type: IncomeType,
}

impl IndMultiYearAdapter {
    /// Create a new multi-year adapter
    ///
    /// # Arguments
    ///
    /// * `years` - Years to load data for
    /// * `income_type` - Type of income to load
    ///
    /// # Returns
    ///
    /// * A new multi-year adapter
    #[must_use] pub fn new(years: Vec<u16>, income_type: IncomeType) -> Self {
        Self {
            registry: IndRegister::new(),
            years,
            income_type,
        }
    }
    
    /// Load income data for multiple years
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory containing IND registry files
    /// * `pnr_filter` - Optional PNR filter
    ///
    /// # Returns
    ///
    /// * Income data for the specified years
    pub fn load_multi_year(
        &self,
        base_path: &std::path::Path,
        pnr_filter: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Income>> {
        let mut all_incomes = Vec::new();
        
        for year in &self.years {
            // Construct the year-specific path
            let year_path = base_path.join(format!("{year}"));
            
            // Load data for this year
            let mut year_incomes = self.registry.load_as::<Income>(&year_path, pnr_filter)?;
            
            // Filter by income type
            match self.income_type {
                IncomeType::All => {}
                IncomeType::Personal => {
                    year_incomes.retain(|income| income.income_type == "personal");
                }
                IncomeType::Household => {
                    year_incomes.retain(|income| income.income_type == "household");
                }
                IncomeType::Disposable => {
                    year_incomes.retain(|income| income.income_type == "disposable");
                }
            }
            
            // Add to overall collection
            all_incomes.extend(year_incomes);
        }
        
        Ok(all_incomes)
    }
    
    /// Load income data for multiple years asynchronously
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory containing IND registry files
    /// * `pnr_filter` - Optional PNR filter
    ///
    /// # Returns
    ///
    /// * Income data for the specified years
    pub async fn load_multi_year_async(
        &self,
        base_path: &std::path::Path,
        pnr_filter: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Income>> {
        use futures::stream::{self, StreamExt};
        
        // Create a stream of futures, each loading data for one year
        let income_futures = stream::iter(self.years.iter().map(|year| {
            let year_path = base_path.join(format!("{year}"));
            let registry = &self.registry;
            let income_type = self.income_type;
            
            async move {
                // Load data for this year
                let mut year_incomes = registry.load_as_async::<Income>(&year_path, pnr_filter).await?;
                
                // Filter by income type
                match income_type {
                    IncomeType::All => {}
                    IncomeType::Personal => {
                        year_incomes.retain(|income| income.income_type == "personal");
                    }
                    IncomeType::Household => {
                        year_incomes.retain(|income| income.income_type == "household");
                    }
                    IncomeType::Disposable => {
                        year_incomes.retain(|income| income.income_type == "disposable");
                    }
                }
                
                Ok::<_, crate::error::Error>(year_incomes)
            }
        }))
        .buffer_unordered(4) // Process up to 4 years concurrently
        .collect::<Vec<_>>()
        .await;
        
        // Combine results
        let mut all_incomes = Vec::new();
        for result in income_futures {
            match result {
                Ok(year_incomes) => all_incomes.extend(year_incomes),
                Err(e) => return Err(e.into()),
            }
        }
        
        Ok(all_incomes)
    }
}

/// Implement `ModelLookup` for Income
impl ModelLookup<Income, (String, i32)> for Income {
    /// Create a lookup map from (PNR, year) to Income
    fn create_lookup(incomes: &[Income]) -> HashMap<(String, i32), Arc<Income>> {
        let mut lookup = HashMap::with_capacity(incomes.len());
        for income in incomes {
            lookup.insert(
                (income.individual_pnr.clone(), income.year),
                Arc::new(income.clone())
            );
        }
        lookup
    }
}