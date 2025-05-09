//! BEF registry model conversions
//!
//! This module forwards conversion requests to the serde_arrow-based deserializer.
//! It maintains compatibility with the old `ModelConversion` interface.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::Individual;

/// Forward to serde_arrow-based deserializer for Individual conversion
pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Delegate to deserializer which uses SerdeIndividual
    super::deserializer::deserialize_row(batch, row)
}

/// Forward to serde_arrow-based deserializer for batch conversion
pub fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // Delegate to deserializer which uses SerdeIndividual
    super::deserializer::deserialize_batch(batch)
}
