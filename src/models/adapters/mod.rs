//! Registry-to-Model Adapters
//!
//! This module contains adapters that map registry data to domain models.
//! These adapters are used to convert data from various registry formats
//! into the domain models used throughout the application.

use crate::error::Result;
use arrow::record_batch::RecordBatch;

/// Defines the interface for registry-to-model adapters
pub trait RegistryAdapter<T> {
    /// Convert a `RecordBatch` from a registry into domain model objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<T>>;

    /// Apply additional transformations if needed
    fn transform(models: &mut [T]) -> Result<()>;
}

// Registry adapters for specific Danish registries
pub mod bef_adapter; // Map BEF registry to Individual/Family models
pub mod ind_adapter;
pub mod lpr_adapter; // Map LPR registry to Diagnosis models
pub mod mfr_adapter; // Map MFR registry to Child models // Map IND registry to Income models
pub mod adapter_utils; // Utility functions for adapters

// Re-export commonly used types
pub use bef_adapter::{BefCombinedAdapter, BefFamilyAdapter, BefIndividualAdapter};
pub use ind_adapter::{IncomeType, IndIncomeAdapter, IndMultiYearAdapter};
pub use lpr_adapter::{Lpr2DiagAdapter, Lpr3DiagnoserAdapter, LprCombinedAdapter};
pub use mfr_adapter::MfrChildAdapter;
pub use adapter_utils::{get_column, downcast_array, default_date_config};
