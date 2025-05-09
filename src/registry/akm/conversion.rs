//! AKM Registry conversion implementation
//!
//! This module contains the implementation for converting AKM data to domain models.
//!
//! The implementation uses the `serde_arrow` approach for deserialization.

use crate::common::traits::registry::AkmRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;

/// Convert a single AKM record to an Individual model
///
/// This function is a convenience wrapper around the deserializer.
pub fn from_akm_record(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::akm::deserializer::deserialize_row(batch, row)
}

/// Convert an entire batch of AKM records to Individual models
///
/// This function is a convenience wrapper around the deserializer.
pub fn from_akm_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::akm::deserializer::deserialize_batch(batch)
}

/// Enhance an Individual with employment data from an AKM record
///
/// This function is kept for backward compatibility, but uses the individual's
/// implementation for enhancement.
pub fn enhance_with_employment_data(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    // Forward to the AkmRegistry trait implementation on Individual
    individual.enhance_with_employment_data(batch, row)
}
