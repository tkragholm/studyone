//! IND registry trait implementations for Individual
//!
//! This module contains the implementation of `IndRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::IndRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl IndRegistry for Individual {
    fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::ind::deserializer::deserialize_row(batch, row)
    }

    fn from_ind_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the trait-based deserializer from the unified schema
        crate::registry::ind::deserializer::deserialize_batch(batch)
    }
}
