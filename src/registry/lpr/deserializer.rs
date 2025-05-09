//! LPR registry deserialization
//!
//! This module provides functionality for deserializing LPR registry data
//! into domain models using `serde_arrow`.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::core::types::Gender;
use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Get field mapping from LPR registry to `SerdeIndividual`
#[must_use] pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    // LPR-specific field mappings would go here
    // For example:
    mapping.insert("CPR".to_string(), "pnr".to_string());
    mapping
}

/// Deserialize `RecordBatch` to Vec<Individual> using `serde_arrow`
///
/// This function uses the `SerdeIndividual` deserialization mechanism for
/// efficient conversion.
///
/// # Arguments
///
/// * `batch` - The LPR record batch to deserialize
///
/// # Returns
///
/// A vector of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR batch with SerdeIndividual");

    // Check if batch has the necessary columns for serde_arrow approach
    if batch.schema().field_with_name("PNR").is_ok() || batch.schema().field_with_name("CPR").is_ok() {
        // Try serde_arrow approach
        let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;
        
        match SerdeIndividual::from_batch(&batch_with_mapping) {
            Ok(serde_individuals) => {
                let individuals = serde_individuals
                    .into_iter()
                    .map(crate::models::core::individual::serde::SerdeIndividual::into_inner)
                    .collect();
                
                debug!("Successfully deserialized LPR batch with SerdeIndividual");
                return Ok(individuals);
            }
            Err(e) => {
                debug!("Falling back to basic extraction: {e}");
                // Continue to basic extraction below
            }
        }
    }

    // Basic extraction (fallback approach)
    let mut individuals = Vec::new();
    for row in 0..batch.num_rows() {
        if let Some(individual) = deserialize_row(batch, row)? {
            individuals.push(individual);
        }
    }
    Ok(individuals)
}

/// Deserialize a single row from a `RecordBatch` to an Individual
///
/// # Arguments
///
/// * `batch` - The LPR record batch
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// An Option containing the deserialized Individual, or None if deserialization failed
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    use crate::utils::array_utils::{downcast_array, get_column};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    // Try to get PNR column (might be called CPR in some LPR data)
    let pnr_col = get_column(batch, "PNR", &DataType::Utf8, false)
        .or_else(|_| get_column(batch, "CPR", &DataType::Utf8, false))?;
    
    let pnr = if let Some(array) = pnr_col {
        let string_array = downcast_array::<StringArray>(&array, "PNR/CPR", "String")?;
        if row < string_array.len() && !string_array.is_null(row) {
            string_array.value(row).to_string()
        } else {
            return Ok(None); // No valid PNR
        }
    } else {
        return Ok(None); // No PNR column
    };

    // Create a basic individual with just the PNR
    let individual = Individual::new(pnr, Gender::Unknown, None);
    Ok(Some(individual))
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