//! Registry deserialization utility
//!
//! This module provides a unified interface for deserializing registry data
//! to domain models using the appropriate registry-specific deserializer.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::bef;
use crate::registry::detect::{RegistryType, detect_registry_type};
use crate::registry::ind;
use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Deserialize a `RecordBatch` into Individual models
///
/// This function detects the registry type and delegates to the appropriate
/// registry-specific deserializer.
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` to deserialize
///
/// # Returns
///
/// A vector of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    let registry_type = detect_registry_type(batch);
    debug!("Deserializing batch of type: {}", registry_type.as_str());

    match registry_type {
        RegistryType::BEF => bef::deserializer::deserialize_batch(batch),
        RegistryType::IND => ind::deserializer::deserialize_batch(batch),
        RegistryType::AKM => akm::deserializer::deserialize_batch(batch),
        // For other registry types, we'll implement them gradually
        // For now, we can use a simple implementation that just extracts PNR
        _ => deserialize_minimal(batch),
    }
}

/// Deserialize a specific row from a `RecordBatch` into an Individual model
///
/// # Arguments
///
/// * `batch` - The `RecordBatch` containing the row
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// An Optional Individual model (None if deserialization failed)
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let registry_type = detect_registry_type(batch);
    debug!(
        "Deserializing row {} from batch of type: {}",
        row,
        registry_type.as_str()
    );

    match registry_type {
        RegistryType::BEF => bef::deserializer::deserialize_row(batch, row),
        RegistryType::IND => ind::deserializer::deserialize_row(batch, row),
        // For other registry types, we'll implement them gradually
        // For now, we can use a simple implementation that just extracts PNR
        _ => deserialize_minimal_row(batch, row),
    }
}

/// Minimally deserialize a `RecordBatch` to extract just PNR
///
/// This is a fallback for registry types that don't have a specific deserializer yet.
pub fn deserialize_minimal(batch: &RecordBatch) -> Result<Vec<Individual>> {
    let mut individuals = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        if let Some(individual) = deserialize_minimal_row(batch, row)? {
            individuals.push(individual);
        }
    }

    Ok(individuals)
}

/// Minimally deserialize a row to extract just PNR
///
/// This is a fallback for registry types that don't have a specific deserializer yet.
pub fn deserialize_minimal_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    use crate::models::core::types::Gender;
    use crate::utils::array_utils::{downcast_array, get_column};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    // Try to get PNR column
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

    // Create minimal Individual with just the PNR
    Ok(Some(Individual::new(pnr, Gender::Unknown, None)))
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
            // Keep the original field name
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
