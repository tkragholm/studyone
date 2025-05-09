//! IND registry deserialization
//!
//! This module provides functionality for deserializing IND (Income) registry data
//! into domain models using the SerdeIndividual wrapper.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::core::types::SocioeconomicStatus;
use crate::registry::ind::{conversion, schema};
use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use log::{debug, warn};
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
/// This function attempts to use the SerdeIndividual deserialization mechanism for
/// efficient conversion. If that fails, it falls back to the legacy conversion method.
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

    // Try using SerdeIndividual for direct deserialization
    match deserialize_with_serde_individual(batch) {
        Ok(mut individuals) => {
            debug!("Successfully deserialized IND batch with SerdeIndividual");

            // Post-process socioeconomic status (since it's an intermediate field)
            for individual in &mut individuals {
                // Here we would post-process any fields that aren't handled directly by SerdeIndividual
                // For example, converting socioeconomic_status_code to socioeconomic_status enum
                post_process_individual(individual);
            }

            Ok(individuals)
        }
        Err(err) => {
            warn!(
                "SerdeIndividual deserialization failed, falling back to legacy conversion: {}",
                err
            );
            // Fallback to row-by-row processing
            let mut individuals = Vec::new();
            for row in 0..batch.num_rows() {
                if let Some(individual) = deserialize_row(batch, row)? {
                    individuals.push(individual);
                }
            }
            Ok(individuals)
        }
    }
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
    // For single row, extract the PNR and create a minimal Individual
    use crate::models::core::types::Gender;
    use crate::utils::array_utils::{downcast_array, get_column};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    // Extract PNR from IND registry data
    let pnr_col = get_column(batch, "PNR", &DataType::Utf8, false)?;
    let pnr = if let Some(array) = pnr_col {
        let string_array = downcast_array::<StringArray>(&array, "PNR", "String")?;
        if row < string_array.len() && !string_array.is_null(row) {
            string_array.value(row).to_string()
        } else {
            return Ok(None); // No valid PNR
        }
    } else {
        return Ok(None); // No PNR column
    };

    // Create a basic individual with just the PNR
    Ok(Some(Individual::new(pnr, Gender::Unknown, None)))
}

/// Inner implementation of SerdeIndividual-based deserialization
///
/// This function handles the process of deserializing an IND registry batch
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
