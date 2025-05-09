//! MFR registry deserialization
//!
//! This module provides functionality for deserializing MFR (Medical Birth Registry) data
//! into domain models using serde_arrow.
//!
//! This includes conversion to both Individual and Child models.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::derived::child::Child;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Get field mapping from MFR registry to SerdeIndividual
pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    // MFR-specific field mappings
    mapping.insert("BARNETS_CPR".to_string(), "pnr".to_string());
    mapping.insert("MOR_CPR".to_string(), "mother_pnr".to_string());
    mapping.insert("FAR_CPR".to_string(), "father_pnr".to_string());
    mapping
}

/// Deserialize RecordBatch to Vec<Individual> using serde_arrow
///
/// # Arguments
///
/// * `batch` - The MFR record batch to deserialize
///
/// # Returns
///
/// A vector of Individual models
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing MFR batch with SerdeIndividual");

    // Check if batch has necessary columns for serde_arrow approach
    if batch.schema().field_with_name("BARNETS_CPR").is_ok() {
        // Try serde_arrow approach
        let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;

        match SerdeIndividual::from_batch(&batch_with_mapping) {
            Ok(serde_individuals) => {
                let individuals = serde_individuals
                    .into_iter()
                    .map(|si| si.into_inner())
                    .collect();

                debug!("Successfully deserialized MFR batch with SerdeIndividual");
                return Ok(individuals);
            }
            Err(e) => {
                debug!("Falling back to minimal extraction: {}", e);
                // Continue to minimal extraction below
            }
        }
    }

    // Minimal extraction (fallback approach)
    use crate::registry::deserializer::deserialize_minimal;
    deserialize_minimal(batch)
}

/// Deserialize a single row from a RecordBatch to an Individual
///
/// # Arguments
///
/// * `batch` - The MFR record batch
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

/// Deserialize a RecordBatch to Vec<Child> using serde_arrow
///
/// This function first converts the data to Individuals, then creates Child
/// models from those Individuals, enhancing them with MFR-specific birth data.
///
/// # Arguments
///
/// * `batch` - The MFR record batch to deserialize
/// * `individual_lookup` - Optional lookup table to find parent Individuals by PNR
///
/// # Returns
///
/// A vector of Child models
pub fn deserialize_child_batch(
    batch: &RecordBatch,
    individual_lookup: &HashMap<String, Arc<Individual>>,
) -> Result<Vec<Child>> {
    debug!("Deserializing MFR batch to Child models");

    // First get the individuals from the batch
    let individuals = deserialize_batch(batch)?;

    // Convert individuals to children
    let mut children = Vec::with_capacity(individuals.len());

    for (i, individual) in individuals.into_iter().enumerate() {
        // Create child from individual
        let individual_arc = Arc::new(individual);
        let mut child = Child::from_individual(individual_arc);

        // Enhance with birth-related data
        if batch.schema().field_with_name("VAEGT").is_ok() {
            // Extract birth weight
            if let Ok(Some(weight)) = crate::utils::field_extractors::extract_int32(batch, i, "VAEGT", false) {
                child.birth_weight = Some(weight);
            }
        }

        if batch.schema().field_with_name("SVLENGTH").is_ok() {
            // Extract gestational age
            if let Ok(Some(ga)) = crate::utils::field_extractors::extract_int32(batch, i, "SVLENGTH", false) {
                child.gestational_age = Some(ga);
            }
        }

        if batch.schema().field_with_name("APGAR5").is_ok() {
            // Extract Apgar score
            if let Ok(Some(apgar)) = crate::utils::field_extractors::extract_int32(batch, i, "APGAR5", false) {
                child.apgar_score = Some(apgar);
            }
        }

        // Look up parent details if available (just adding to logic, not necessarily implemented)
        if let Ok(Some(mother_pnr)) = crate::utils::field_extractors::extract_string(batch, i, "MOR_CPR", false) {
            if let Some(_mother) = individual_lookup.get(&mother_pnr) {
                // Could enhance child with mother's details here if needed
                debug!("Found mother in lookup table for child: {}", child.individual().pnr);
            }
        }

        children.push(child);
    }

    debug!("Successfully deserialized MFR batch to {} Child models", children.len());
    Ok(children)
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
