//! BEF registry conversion using serde_arrow with direct attributes
//!
//! This module provides serde_arrow-based conversion between BEF registry records
//! and domain models, utilizing serde attributes for direct conversion.

use crate::error::Result;
use crate::models::core::individual::SerdeIndividual;
use crate::models::core::individual::Individual;
use arrow::record_batch::RecordBatch;
use log::debug;

/// BEF registry serde_arrow converter
#[derive(Default)]
pub struct BefSerdeConverter;

impl BefSerdeConverter {
    /// Create a new BEF converter
    pub fn new() -> Self {
        Self {}
    }
    
    /// Convert a batch using serde_arrow, returning standard Individual models
    pub fn convert_batch_with_serde_arrow(batch: &RecordBatch) -> Result<Vec<Individual>> {
        debug!("Converting BEF registry batch with serde_arrow");

        // Using direct serde_arrow conversion with serde attributes
        match SerdeIndividual::from_batch(batch) {
            Ok(serde_individuals) => {
                debug!("Successfully deserialized {} individuals using serde_arrow", serde_individuals.len());
                // Convert from SerdeIndividual to standard Individual
                let individuals = serde_individuals.into_iter()
                    .map(|serde_ind| serde_ind.into_inner())
                    .collect();
                Ok(individuals)
            },
            Err(e) => Err(anyhow::anyhow!("Serde Arrow deserialization error: {}", e)),
        }
    }

    /// Convert a batch using serde_arrow, returning SerdeIndividual models
    pub fn convert_batch_to_serde_individuals(batch: &RecordBatch) -> Result<Vec<SerdeIndividual>> {
        debug!("Converting BEF registry batch with serde_arrow to SerdeIndividual");
        SerdeIndividual::from_batch(batch)
    }

    /// Convert domain models to BEF registry format
    pub fn convert_models_to_batch(individuals: &[Individual]) -> Result<RecordBatch> {
        // Convert standard Individuals to SerdeIndividuals for serialization
        let serde_individuals: Vec<SerdeIndividual> = individuals.iter()
            .map(SerdeIndividual::from_standard)
            .collect();

        // Direct serialization using serde_arrow
        serde_arrow::to_record_batch(&serde_arrow::schema::SchemaLike::from_samples(
            &serde_individuals,
            serde_arrow::schema::TracingOptions::default(),
        )?, &serde_individuals)
        .map_err(|e| anyhow::anyhow!("Error serializing to RecordBatch: {}", e))
    }
}