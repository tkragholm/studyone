//! UDDF Registry conversion implementation
//!
//! This module contains the implementation for converting UDDF data to domain models.
//!
//! The implementation uses the `serde_arrow` approach for deserialization.

use crate::common::traits::registry::UddfRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;

/// Convert a single UDDF record to an Individual model
///
/// This function is a convenience wrapper around the deserializer.
pub fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::uddf::deserializer::deserialize_row(batch, row)
}

/// Convert an entire batch of UDDF records to Individual models
///
/// This function is a convenience wrapper around the deserializer.
pub fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::uddf::deserializer::deserialize_batch(batch)
}

/// Enhance an Individual with education data from a UDDF record
///
/// This function is kept for backward compatibility, but uses the individual's
/// implementation for enhancement.
pub fn enhance_with_education_data(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    // Forward to the UddfRegistry trait implementation on Individual
    individual.enhance_with_education_data(batch, row)
}
