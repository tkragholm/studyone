//! DOD registry trait-based deserializer
//!
//! This module provides functionality for deserializing DOD registry data
//! using the trait-based field access system.

use arrow::record_batch::RecordBatch;
use log::debug;

use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::registry_traits::DodFields;
use crate::registry::death::dod::schema::create_dod_schema;

// Use the macro to generate the trait deserializer
crate::generate_trait_deserializer!(DodTraitDeserializer, "DOD", create_dod_schema);

/// Deserialize a DOD record batch using the trait-based deserializer
///
/// # Arguments
///
/// * `batch` - The DOD record batch to deserialize
///
/// # Returns
///
/// A Result containing a Vec of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing DOD batch with trait-based deserializer");

    let deserializer = DodTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from a DOD record batch
///
/// # Arguments
///
/// * `batch` - The DOD record batch
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// A Result containing an Option with the deserialized Individual
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = DodTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// Enhance individuals with death information from a DOD batch
///
/// This function takes a slice of Individual models and a DOD `RecordBatch`,
/// and enhances the individuals with death dates where available using the trait-based
/// field access system.
pub fn enhance_individuals_with_death_info(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    let mut count = 0;

    // Create a deserializer
    let deserializer = DodTraitDeserializer::new();

    // Create a map of PNRs to row indices for fast lookup
    let mut pnr_row_map = std::collections::HashMap::new();

    // Deserialize each row to get PNRs
    for row in 0..batch.num_rows() {
        if let Some(individual) = deserializer.deserialize_row(batch, row)? {
            pnr_row_map.insert(individual.pnr.clone(), row);
        }
    }

    // Update individuals that exist in the death registry
    for individual in individuals.iter_mut() {
        if let Some(&row) = pnr_row_map.get(&individual.pnr) {
            // Create a temporary individual to extract death information
            if let Some(death_individual) = deserializer.deserialize_row(batch, row)? {
                // Extract death date
                let dod_fields: &dyn DodFields = &death_individual;
                if let Some(death_date) = dod_fields.death_date() {
                    // Set death date on the target individual
                    let target_dod_fields: &mut dyn DodFields = individual;
                    target_dod_fields.set_death_date(Some(death_date));

                    // Set death cause if available
                    if let Some(cause) = dod_fields.death_cause() {
                        target_dod_fields.set_death_cause(Some(cause.to_string()));
                    }

                    // Set underlying cause if available
                    if let Some(underlying) = dod_fields.underlying_death_cause() {
                        target_dod_fields.set_underlying_death_cause(Some(underlying.to_string()));
                    }

                    count += 1;
                }
            }
        }
    }

    Ok(count)
}
