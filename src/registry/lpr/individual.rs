//! LPR registry trait implementations for Individual
//!
//! This module contains the implementation of `LprRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::LprRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl LprRegistry for Individual {
    fn from_lpr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Using trait-based deserializer instead
        crate::registry::lpr::trait_deserializer::deserialize_adm_row(batch, row)
    }

    fn from_lpr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Using trait-based deserializer instead
        crate::registry::lpr::trait_deserializer::deserialize_adm_batch(batch)
    }
}
