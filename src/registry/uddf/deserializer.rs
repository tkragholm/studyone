//! UDDF registry trait-based deserializer (macro version)
//!
//! This module provides a macro-generated trait-based deserializer
//! for UDDF registry data, using the unified schema definition.

use crate::generate_trait_deserializer;
use crate::registry::uddf::schema::create_uddf_schema;
use crate::models::core::Individual;
use crate::error::Result;
use arrow::record_batch::RecordBatch;

// Generate the trait deserializer from the unified schema
generate_trait_deserializer!(UddfTraitDeserializer, "UDDF", create_uddf_schema);

/// Deserialize a row from a batch
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = UddfTraitDeserializer::new();
    crate::registry::deserializer_functions::deserialize_row(&deserializer, batch, row)
}

/// Deserialize a batch of records
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    let deserializer = UddfTraitDeserializer::new();
    crate::registry::deserializer_functions::deserialize_batch(&deserializer, batch)
}
