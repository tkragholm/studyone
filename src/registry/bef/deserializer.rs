//! BEF registry deserialization
//!
//! This module provides functionality for deserializing BEF registry data
//! into domain models using the `SerdeIndividual` wrapper.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::registry::bef::schema;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Get field mapping from BEF registry to `SerdeIndividual`
///
/// Uses the mapping defined in the schema module.
#[must_use] pub fn field_mapping() -> HashMap<String, String> {
    schema::field_mapping()
}

/// Deserialize `RecordBatch` to Vec<Individual> directly using `SerdeIndividual`
///
/// This function uses the `SerdeIndividual` deserialization mechanism for
/// efficient conversion based on `serde_arrow`.
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

    // Create a mapped batch with proper field names for deserialization
    let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;

    // Print the schema for debugging
    log::debug!("Mapped batch schema: {:?}", batch_with_mapping.schema());

    // Use SerdeIndividual for deserialization
    let serde_result = SerdeIndividual::from_batch(&batch_with_mapping);

    let serde_individuals = match serde_result {
        Ok(individuals) => individuals,
        Err(e) => {
            log::error!("Deserialization error: {}", e);
            return Err(anyhow::anyhow!("Failed to deserialize: {}", e));
        }
    };

    // Convert SerdeIndividual instances to regular Individual instances
    let individuals = serde_individuals
        .into_iter()
        .map(crate::models::core::individual::serde::SerdeIndividual::into_inner)
        .collect();

    debug!("Successfully deserialized BEF batch with SerdeIndividual");
    Ok(individuals)
}

/// Deserialize a single row from a `RecordBatch` to an Individual
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
    // Create a new RecordBatch with just the specified row
    let columns = batch.columns().iter()
        .map(|col| col.slice(row, 1))
        .collect::<Vec<_>>();

    let row_batch = RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create row batch: {}", e))?;

    // Use the batch deserialization method
    let individuals = deserialize_batch(&row_batch)?;

    // Get the first (and only) Individual from the result
    Ok(individuals.into_iter().next())
}


/// Create a record batch with mapped field names
///
/// This function creates a new `RecordBatch` with field names mapped
/// according to the provided mapping table, to facilitate deserialization.
///
/// # Arguments
///
/// * `batch` - The original record batch
/// * `field_mapping` - Mapping from source field names to target field names
///
/// # Returns
///
/// A new `RecordBatch` with mapped field names
pub fn create_mapped_batch(
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
