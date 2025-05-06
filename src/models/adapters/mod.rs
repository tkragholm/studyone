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

// Future submodules for specific registry adapters.
// These will be implemented in Phase 2.2 of the project.
//
// pub mod bef_adapter;    // Map BEF registry to Individual/Family models
// pub mod mfr_adapter;    // Map MFR registry to Child models
// pub mod lpr_adapter;    // Map LPR registry to Diagnosis models
// pub mod ind_adapter;    // Map IND registry to Income models
