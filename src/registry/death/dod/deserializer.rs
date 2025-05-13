//! DOD Registry deserializer
//!
//! This module provides serde_arrow-based deserialization for the DOD registry.
//! For trait-based deserialization, see the `trait_deserializer` module.

use crate::error::Result;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Field mapping for DOD registry
///
/// Maps DOD registry field names to `SerdeIndividual` field names
#[must_use]
pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    
    // Basic fields
    mapping.insert("PNR".to_string(), "PNR".to_string());
    
    // Death fields
    mapping.insert("DODDATO".to_string(), "DODDATO".to_string());
    
    mapping
}

/// Create a mapped batch with standardized field names for deserialization
///
/// This function takes a `RecordBatch` with registry-specific field names
/// and creates a new `RecordBatch` with mapped field names that match
/// the `SerdeIndividual` structure's field names.
pub fn create_mapped_batch(
    batch: &RecordBatch,
    field_mapping: HashMap<String, String>,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_columns = Vec::new();
    let mut new_fields = Vec::new();

    // Create new fields and columns with mapped names
    for field_idx in 0..schema.fields().len() {
        let field = schema.field(field_idx);
        let column = batch.column(field_idx);
        let field_name = field.name();

        if let Some(mapped_name) = field_mapping.get(field_name) {
            // Use the mapped name
            new_fields.push(Field::new(
                mapped_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        } else {
            // If no mapping exists, keep the original name
            new_fields.push(Field::new(
                field_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }
        new_columns.push(column.clone());
    }

    // Create new schema and record batch with mapped field names
    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batch = RecordBatch::try_new(new_schema, new_columns)?;

    Ok(new_batch)
}

/// Deserialize a batch of DOD records into Individual models
///
/// This function takes a `RecordBatch` containing DOD registry data
/// and deserializes it into a Vec of Individual models using
/// the `SerdeIndividual` approach.
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing DOD batch with SerdeIndividual");

    // Create a mapped batch with proper field names for deserialization
    let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;
    
    // Use SerdeIndividual for deserialization
    let serde_individuals = SerdeIndividual::from_batch(&batch_with_mapping)?;
    
    // Convert SerdeIndividual instances to regular Individual instances
    let individuals = serde_individuals
        .into_iter()
        .map(crate::models::core::individual::serde::SerdeIndividual::into_inner)
        .collect();
    
    debug!("Successfully deserialized DOD batch with SerdeIndividual");
    Ok(individuals)
}

/// Deserialize a single row from a DOD batch
///
/// This function takes a `RecordBatch` and row index, and deserializes
/// just that single row into an Individual model.
pub fn deserialize_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    if row >= batch.num_rows() {
        return Ok(None);
    }

    // Create a slice of the batch containing just the specified row
    let slice = batch.slice(row, 1);
    
    // Deserialize the slice (will return a Vec with 0 or 1 elements)
    let individuals = deserialize_batch(&slice)?;
    
    // Return the individual if one was deserialized, otherwise None
    Ok(individuals.into_iter().next())
}

/// Enhance individuals with death information from a DOD batch
///
/// This function takes a slice of Individual models and a DOD `RecordBatch`,
/// and enhances the individuals with death dates where available.
pub fn enhance_individuals_with_death_info(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    use crate::utils::field_extractors::extract_string;
    use crate::utils::field_extractors::extract_date_from_string;

    let mut count = 0;

    // Create a map of PNRs to row indices for fast lookup
    let mut pnr_row_map = std::collections::HashMap::new();
    for row in 0..batch.num_rows() {
        if let Some(pnr) = extract_string(batch, row, "PNR", false)? {
            pnr_row_map.insert(pnr, row);
        }
    }

    // Update individuals that exist in the death registry
    for individual in individuals.iter_mut() {
        if let Some(&row) = pnr_row_map.get(&individual.pnr) {
            // Extract death date
            if let Some(date) = extract_date_from_string(batch, row, "DODDATO", false)? {
                individual.death_date = Some(date);
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Enhance a single individual with death information
///
/// This function takes an Individual and enhances it with death date
/// information if available in the DOD `RecordBatch`.
pub fn enhance_with_death_data(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    use crate::utils::field_extractors::extract_date_from_string;

    // Extract death date using the field extractor
    if let Some(date) = extract_date_from_string(batch, row, "DODDATO", false)? {
        individual.death_date = Some(date);
        return Ok(true);
    }

    Ok(false)
}