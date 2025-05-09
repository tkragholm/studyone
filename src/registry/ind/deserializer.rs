//! IND registry deserialization
//!
//! This module provides functionality for deserializing IND (Income) registry data
//! into domain models using the SerdeIndividual wrapper.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::core::types::SocioeconomicStatus;
use crate::registry::ind::schema;
use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Get field mapping from IND registry to SerdeIndividual
///
/// Uses the mapping defined in the schema module.
pub fn field_mapping() -> HashMap<String, String> {
    schema::field_mapping()
}

/// Deserialize RecordBatch to Vec<Individual> directly using SerdeIndividual
///
/// This function uses the SerdeIndividual deserialization mechanism for
/// efficient conversion with serde_arrow.
///
/// # Arguments
///
/// * `batch` - The IND record batch to deserialize
///
/// # Returns
///
/// A vector of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing IND batch with SerdeIndividual");

    // Create a mapped batch with proper field names for deserialization
    let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;

    // Use SerdeIndividual for deserialization
    let serde_individuals = SerdeIndividual::from_batch(&batch_with_mapping)?;

    // Convert SerdeIndividual instances to regular Individual instances
    let mut individuals: Vec<Individual> = serde_individuals
        .into_iter()
        .map(|si| si.into_inner())
        .collect();

    // Post-process each individual if needed
    for individual in &mut individuals {
        post_process_individual(individual);
    }

    debug!("Successfully deserialized IND batch with SerdeIndividual");
    Ok(individuals)
}

/// Deserialize a single row from a RecordBatch to an Individual
///
/// # Arguments
///
/// * `batch` - The IND record batch
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// An Option containing the deserialized Individual, or None if deserialization failed
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    // Create a new RecordBatch with just the specified row
    let columns = batch
        .columns()
        .iter()
        .map(|col| col.slice(row, 1))
        .collect::<Vec<_>>();

    let row_batch = RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create row batch: {}", e))?;

    // Use the batch deserialization method
    let individuals = deserialize_batch(&row_batch)?;

    // Get the first (and only) Individual from the result
    Ok(individuals.into_iter().next())
}

/// Post-process an Individual after deserialization from IND data
///
/// This function applies any additional transformations needed after
/// the basic deserialization process.
///
/// # Arguments
///
/// * `individual` - The Individual to post-process
fn post_process_individual(individual: &mut Individual) {
    // Map socioeconomic status codes to enum values if needed
    if individual.socioeconomic_status == SocioeconomicStatus::Unknown {
        // This would need a way to access the original code
        // In a real implementation, we might store the code in a temporary field
        // or use a different approach for complex conversions
    }
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
