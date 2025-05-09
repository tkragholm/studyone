//! AKM Registry deserializer
//!
//! This module provides serde_arrow-based deserialization for the AKM registry.

use crate::error::Result;
use crate::models::core::individual::serde::SerdeIndividual;
use crate::models::core::Individual;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, Schema};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

/// Field mapping for AKM registry
///
/// Maps AKM registry field names to SerdeIndividual field names
#[must_use]
pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    
    // Basic fields - AKM registry doesn't have most demographic fields
    mapping.insert("PNR".to_string(), "PNR".to_string());
    
    // Employment fields that AKM registry has
    mapping.insert("SOCIO".to_string(), "SOCIO".to_string());
    mapping.insert("DISCO".to_string(), "DISCO".to_string());
    mapping.insert("BRANCHE".to_string(), "BRANCHE".to_string());
    mapping.insert("ARB_STED_ID".to_string(), "ARB_STED_ID".to_string());
    mapping.insert("HELTID".to_string(), "HELTID".to_string());
    
    mapping
}

/// Create a mapped batch with standardized field names for deserialization
///
/// This function takes a RecordBatch with registry-specific field names
/// and creates a new RecordBatch with mapped field names that match
/// the SerdeIndividual structure's field names.
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

/// Deserialize a batch of AKM records into Individual models
///
/// This function takes a RecordBatch containing AKM registry data
/// and deserializes it into a Vec of Individual models using
/// the SerdeIndividual approach.
pub fn deserialize_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing AKM batch with SerdeIndividual");

    // Create a mapped batch with proper field names for deserialization
    let batch_with_mapping = create_mapped_batch(batch, field_mapping())?;
    
    // Use SerdeIndividual for deserialization
    let serde_individuals = SerdeIndividual::from_batch(&batch_with_mapping)?;
    
    // Convert SerdeIndividual instances to regular Individual instances
    let individuals = serde_individuals
        .into_iter()
        .map(|si| si.into_inner())
        .collect();
    
    debug!("Successfully deserialized AKM batch with SerdeIndividual");
    Ok(individuals)
}

/// Deserialize a single row from an AKM batch
///
/// This function takes a RecordBatch and row index, and deserializes
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