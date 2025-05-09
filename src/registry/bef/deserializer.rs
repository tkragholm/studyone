//! BEF registry deserialization
//!
//! This module provides functionality for deserializing BEF registry data
//! into domain models using the SerdeIndividual wrapper.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::registry::bef::{conversion, schema};
use arrow::datatypes::{Field, Schema};
use log::{debug, warn};
use std::collections::HashMap;
use std::sync::Arc;

/// Get field mapping from BEF registry to SerdeIndividual
///
/// Uses the mapping defined in the schema module.
pub fn field_mapping() -> HashMap<String, String> {
    schema::field_mapping()
}

/// Deserialize RecordBatch to Vec<Individual> directly using SerdeIndividual
///
/// This function attempts to use the SerdeIndividual deserialization mechanism for
/// efficient conversion. If that fails, it falls back to the legacy conversion method.
///
/// # Arguments
///
/// * `batch` - The BEF record batch to deserialize
///
/// # Returns
///
/// A vector of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing BEF batch with SerdeIndividual");

    // Try using SerdeIndividual for direct deserialization
    match deserialize_with_serde_individual(batch) {
        Ok(individuals) => {
            debug!("Successfully deserialized BEF batch with SerdeIndividual");
            Ok(individuals)
        }
        Err(err) => {
            warn!(
                "SerdeIndividual deserialization failed, falling back to legacy conversion: {}",
                err
            );
            // Fall back to legacy conversion method
            conversion::from_bef_batch(batch)
        }
    }
}

/// Deserialize a single row from a RecordBatch to an Individual
///
/// # Arguments
///
/// * `batch` - The BEF record batch
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// An Option containing the deserialized Individual, or None if deserialization failed
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // For single row, use the legacy conversion method for now
    // In the future, this could be optimized to use SerdeIndividual as well
    conversion::from_bef_record(batch, row)
}

/// Inner implementation of SerdeIndividual-based deserialization
///
/// This function handles the process of deserializing a BEF registry batch
/// into a vector of Individual models using the SerdeIndividual wrapper.
fn deserialize_with_serde_individual(batch: &RecordBatch) -> Result<Vec<Individual>> {
    // Create a mapped schema with field name conversions if needed
    let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;

    // Use the SerdeIndividual to deserialize
    let serde_individuals = SerdeIndividual::from_batch(&batch_with_mapping)?;

    // Convert SerdeIndividual to regular Individual
    let individuals = serde_individuals
        .into_iter()
        .map(|si| si.into_inner())
        .collect();

    Ok(individuals)
}

/// Create a record batch with mapped field names
///
/// This function creates a new RecordBatch with field names mapped
/// according to the provided mapping table, to facilitate deserialization.
///
/// # Arguments
///
/// * `batch` - The original record batch
/// * `field_mapping` - Mapping from source field names to target field names
///
/// # Returns
///
/// A new RecordBatch with mapped field names
fn create_mapped_batch(
    batch: &RecordBatch,
    field_mapping: HashMap<String, String>,
) -> Result<RecordBatch> {
    // Create a new schema with mapped field names
    let mut fields = Vec::new();
    for field in batch.schema().fields() {
        let field_name = field.name();
        if let Some(mapped_name) = field_mapping.get(field_name) {
            fields.push(Field::new(
                mapped_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        } else {
            // Create a new Field to match the expected type
            fields.push(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
    }

    let mapped_schema = Schema::new(fields);

    // Create a new RecordBatch with the mapped schema
    RecordBatch::try_new(Arc::new(mapped_schema), batch.columns().to_vec())
        .map_err(|e| anyhow::anyhow!("Failed to create mapped batch: {}", e))
}
