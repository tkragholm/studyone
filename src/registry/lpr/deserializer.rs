//! LPR registry deserializer module
//!
//! This module provides deserializers for LPR registry data.

use crate::error::Result;
use crate::models::core::Individual;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

// Note: Diagnosis struct has been removed. Use Individual with LprFields trait instead.

/// This is a placeholder implementation that will be replaced by the trait-based deserializer.
/// Instead of using this function, use the deserializer_functions.rs module or
/// the LprFields trait on Individual.
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // This function is only here to maintain API compatibility.
    // Use trait_deserializer::deserialize_adm_row instead.
    crate::registry::lpr::trait_deserializer::deserialize_adm_row(batch, row)
}

/// This is a placeholder implementation that will be replaced by the trait-based deserializer.
/// Instead of using this function, use the deserializer_functions.rs module or
/// the LprFields trait on Individual.
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // This function is only here to maintain API compatibility.
    // Use trait_deserializer::deserialize_adm_batch instead.
    crate::registry::lpr::trait_deserializer::deserialize_adm_batch(batch)
}

/// Provide a default field mapping for backward compatibility
pub fn field_mapping() -> HashMap<String, String> {
    HashMap::new() // Empty mapping
}
