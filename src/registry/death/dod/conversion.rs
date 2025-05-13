//! DOD Registry conversion implementation
//!
//! This module contains the implementation for converting DOD data to domain models.
//!
//! The implementation can use either the `serde_arrow` approach or the trait-based approach for deserialization.

use crate::error::Result;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;

/// Enhance an individual with death date information
///
/// This function is a convenience wrapper around the deserializer.
pub fn enhance_with_death_date(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::death::dod::deserializer::enhance_with_death_data(individual, batch, row)
}

/// Find an individual by PNR in a DOD batch and enhance it with death information
///
/// This function is a convenience wrapper around the deserializer.
/// Uses the serde_arrow-based deserializer by default.
pub fn enhance_individuals_with_death_info(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    // Forward to the serde_arrow-based deserializer
    crate::registry::death::dod::deserializer::enhance_individuals_with_death_info(individuals, batch)
}

/// Find an individual by PNR in a DOD batch and enhance it with death information
/// using the trait-based deserializer
///
/// This function is a convenience wrapper around the trait-based deserializer.
pub fn enhance_individuals_with_death_info_trait(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    // Forward to the trait-based deserializer
    crate::registry::death::dod::trait_deserializer::enhance_individuals_with_death_info(individuals, batch)
}
