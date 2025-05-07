//! IND registry model conversion implementation
//!
//! This module implements direct conversion between IND registry data and
//! Income domain models without requiring a separate adapter.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Result;
use crate::models::income::Income;
use crate::registry::ind::IndRegister;
use crate::registry::model_conversion::ModelConversion;
use crate::registry::RegisterLoader;
use arrow::record_batch::RecordBatch;

impl ModelConversion<Income> for IndRegister {
    /// Convert IND registry data to Income models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Income>> {
        // Uses the standard year of data for IND records
        // In a more advanced implementation, this could be configurable
        let year = 2020;
        
        // Create incomes without inflation adjustment for simplicity
        // In a real implementation, we might want to provide CPI indices
        Income::from_ind_batch(batch, year, None)
    }
    
    /// Convert Income models back to IND registry format
    fn from_models(&self, _models: &[Income]) -> Result<RecordBatch> {
        // This could be implemented if needed, but for now just return
        // an empty implementation error
        Err(anyhow::anyhow!("Converting Income models to IND registry format is not yet implemented"))
    }
    
    /// Apply transformations to Income models
    fn transform_models(&self, _models: &mut [Income]) -> Result<()> {
        // No transformations needed for basic Income models
        Ok(())
    }
}

/// IndRegister with year configuration
pub struct YearConfiguredIndRegister {
    /// Base register
    base_register: IndRegister,
    /// Year to use for income data
    year: i32,
    /// CPI indices for inflation adjustment
    cpi_indices: Option<HashMap<i32, f64>>,
}

impl YearConfiguredIndRegister {
    /// Create a new year-configured IND register
    pub fn new(year: i32) -> Self {
        Self {
            base_register: IndRegister::new(),
            year,
            cpi_indices: None,
        }
    }
    
    /// Set CPI indices for inflation adjustment
    pub fn with_cpi_indices(mut self, indices: HashMap<i32, f64>) -> Self {
        self.cpi_indices = Some(indices);
        self
    }
    
    /// Get the configured year
    pub fn year(&self) -> i32 {
        self.year
    }
    
    /// Get reference to base register
    pub fn base_register(&self) -> &IndRegister {
        &self.base_register
    }
}

impl RegisterLoader for YearConfiguredIndRegister {
    fn get_register_name(&self) -> &'static str {
        self.base_register.get_register_name()
    }
    
    fn get_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.base_register.get_schema()
    }
    
    fn load(&self, base_path: &Path, pnr_filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>> {
        self.base_register.load(base_path, pnr_filter)
    }
    
    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>> {
        self.base_register.load_async(base_path, pnr_filter)
    }
    
    fn supports_pnr_filter(&self) -> bool {
        self.base_register.supports_pnr_filter()
    }
    
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        self.base_register.get_pnr_column_name()
    }
}

impl ModelConversion<Income> for YearConfiguredIndRegister {
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Income>> {
        let year = self.year;
        
        if let Some(cpi_indices) = &self.cpi_indices {
            // Create inflation adjustment function 
            let adjust_fn = move |amount: f64, from_year: i32, to_year: i32| {
                let from_cpi = cpi_indices.get(&from_year).copied().unwrap_or(1.0);
                let to_cpi = cpi_indices.get(&to_year).copied().unwrap_or(1.0);
                
                // Convert from from_year prices to to_year prices
                amount * (to_cpi / from_cpi)
            };
            
            Income::from_ind_batch(batch, year, Some(&adjust_fn))
        } else {
            // No inflation adjustment
            Income::from_ind_batch(batch, year, None)
        }
    }
    
    fn from_models(&self, _models: &[Income]) -> Result<RecordBatch> {
        Err(anyhow::anyhow!("Converting Income models to IND registry format is not yet implemented"))
    }
    
    fn transform_models(&self, _models: &mut [Income]) -> Result<()> {
        Ok(())
    }
}