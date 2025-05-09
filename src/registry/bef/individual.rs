//! BEF registry trait implementations for Individual
//! 
//! This module contains the implementation of BefRegistry for the Individual model.

use crate::RecordBatch;
use crate::common::traits::BefRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Delegate to the BEF deserializer
        crate::registry::bef::deserializer::deserialize_row(batch, row)
    }

    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Delegate to the BEF deserializer
        crate::registry::bef::deserializer::deserialize_batch(batch)
    }
}