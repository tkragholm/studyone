//! Common deserializer functions for registry trait deserializers
//!
//! This module provides implementations for the `deserialize_row` and 
//! `deserialize_batch` functions that are used in the trait deserializer macros.

use crate::error::Result;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;

/// Deserialize a single row from a batch using the provided deserializer
///
/// # Arguments
/// * `deserializer` - The trait deserializer instance
/// * `batch` - The Arrow `RecordBatch` to extract from
/// * `row` - The row index
///
/// # Returns
/// * `Result<Option<Individual>>` - The deserialized individual or None
pub fn deserialize_row<T>(
    deserializer: &T, 
    batch: &RecordBatch, 
    row: usize
) -> Result<Option<Individual>>
where 
    T: crate::registry::trait_deserializer::RegistryDeserializer
{
    if row >= batch.num_rows() {
        return Ok(None);
    }

    // Create a slice of the batch containing just the specified row
    let slice = batch.slice(row, 1);
    
    // Deserialize the slice (will return a Vec with 0 or 1 elements)
    let individuals = deserialize_batch(deserializer, &slice)?;
    
    // Return the individual if one was deserialized, otherwise None
    Ok(individuals.into_iter().next())
}

/// Deserialize a batch of records using the provided deserializer
///
/// # Arguments
/// * `deserializer` - The trait deserializer instance
/// * `batch` - The Arrow `RecordBatch` to deserialize
///
/// # Returns
/// * `Result<Vec<Individual>>` - The deserialized individuals
pub fn deserialize_batch<T>(
    deserializer: &T, 
    batch: &RecordBatch
) -> Result<Vec<Individual>>
where 
    T: crate::registry::trait_deserializer::RegistryDeserializer
{
    // Use the deserializer's registry_type for logging
    log::debug!(
        "Deserializing {} batch with trait-based deserializer",
        deserializer.registry_type()
    );

    // Access the extractor implementations from the deserializer
    let extractors = deserializer.field_extractors();

    // Results vector
    let mut results = Vec::new();

    // Deserialize each row
    for row in 0..batch.num_rows() {
        // Create a new Individual
        let mut individual = Individual::default();

        // Apply each field extractor
        for extractor in extractors {
            extractor.extract_and_set(batch, row, &mut individual as &mut dyn std::any::Any)?;
        }

        // Add the individual to results
        results.push(individual);
    }

    log::debug!(
        "Successfully deserialized {} individuals from {} batch",
        results.len(),
        deserializer.registry_type()
    );

    Ok(results)
}