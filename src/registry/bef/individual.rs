//! BEF registry trait implementations for Individual
//!
//! This module contains the implementation of `BefRegistry` for the Individual model
//! using the trait-based deserialization from unified schema approach.

use crate::RecordBatch;
use crate::common::traits::BefRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::bef::trait_deserializer_macro::deserialize_row(batch, row)
    }

    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::bef::trait_deserializer_macro::deserialize_batch(batch)
    }
}