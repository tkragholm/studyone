//! BEF registry trait implementations for Individual
//!
//! This module contains the implementation of BefRegistry for the Individual model
//! using the serde_arrow-based deserialization approach.

use crate::RecordBatch;
use crate::common::traits::BefRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;

impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Directly use the serde_arrow-based deserializer
        crate::registry::bef::deserializer::deserialize_row(batch, row)
    }

    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Create a mapped batch with proper field names for deserialization
        let field_mapping = crate::registry::bef::schema::field_mapping();
        let batch_with_mapping = crate::registry::bef::deserializer::create_mapped_batch(
            batch,
            field_mapping
        )?;

        // Use SerdeIndividual directly for deserialization
        let serde_individuals = SerdeIndividual::from_batch(&batch_with_mapping)?;

        // Convert SerdeIndividual instances to regular Individual instances
        let individuals = serde_individuals
            .into_iter()
            .map(|si| si.into_inner())
            .collect();

        Ok(individuals)
    }
}