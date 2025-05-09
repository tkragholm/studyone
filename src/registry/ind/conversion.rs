//! IND registry model conversions
//!
//! This module implements registry-specific conversions for IND registry data.
//! It provides trait implementations to convert from IND registry format to domain models.
//! It also implements the conversion between IND registry data and Income domain models.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use crate::RecordBatch;
use crate::common::traits::RegistryAware;
use crate::error::Result;
use crate::models::Income;
use crate::registry::RegisterLoader;
use crate::registry::ind::IndRegister;

// IndRegistry for Individual is already implemented in models/individual.rs
// We don't need to implement it here again

/// `IndRegister` with year configuration
#[derive(Debug)]
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
    #[must_use]
    pub fn new(year: i32) -> Self {
        Self {
            base_register: IndRegister::new(),
            year,
            cpi_indices: None,
        }
    }

    /// Set CPI indices for inflation adjustment
    #[must_use]
    pub fn with_cpi_indices(mut self, indices: HashMap<i32, f64>) -> Self {
        self.cpi_indices = Some(indices);
        self
    }

    /// Get the configured year
    #[must_use]
    pub const fn year(&self) -> i32 {
        self.year
    }

    /// Get reference to base register
    #[must_use]
    pub const fn base_register(&self) -> &IndRegister {
        &self.base_register
    }

    // Method has been replaced with trait-based implementation in ModelConversion
}

// Implement StatefulAdapter for YearConfiguredIndRegister
impl crate::common::traits::adapter::StatefulAdapter<Income> for YearConfiguredIndRegister {
    fn convert_batch(&self, _batch: &RecordBatch) -> Result<Vec<Income>> {
        // This would normally convert income data for the configured year
        // For now, return an empty vector as a stub implementation
        Ok(Vec::new())
    }
}

// Implement RegistryAware to support registry-specific traits
impl RegistryAware for YearConfiguredIndRegister {
    fn registry_name() -> &'static str {
        "IND"
    }

    fn from_registry_record(_batch: &RecordBatch, _row: usize) -> Result<Option<Self>> {
        // This doesn't make sense for a register, but we need to implement it
        // Returns None as this isn't a typical entity
        Ok(None)
    }

    fn from_registry_batch(_batch: &RecordBatch) -> Result<Vec<Self>> {
        // This doesn't make sense for a register, but we need to implement it
        // Returns empty vector as this isn't a typical entity
        Ok(Vec::new())
    }
}

impl RegisterLoader for YearConfiguredIndRegister {
    fn get_register_name(&self) -> &'static str {
        self.base_register.get_register_name()
    }

    fn get_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.base_register.get_schema()
    }

    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
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
